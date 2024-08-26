import numpy as np
import pandas as pd
from Py6S import *
import os
from osgeo import gdal
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import time
from tqdm import tqdm


def lonlat_to_pixel(lon, lat, geotransform):
    pixel_x = int((lon - geotransform[0]) / geotransform[1])
    pixel_y = int((lat - geotransform[3]) / geotransform[5])
    return pixel_x, pixel_y


def process_poi(poi, band_data, saz, saa, soz, soa, lucc_data, water_vapor_data, ozone_data, band_wavelengths,
                lucc_to_aerosol, geotransform, mask_data):
    poi_id, lon, lat = poi
    x, y = lonlat_to_pixel(lon, lat, geotransform)

    if not (0 <= x < band_data[0].shape[0] and 0 <= y < band_data[0].shape[1]):
        return [poi_id, lon, lat] + [0] * 6 + [0]

    if mask_data[x, y] == 1:
        return [poi_id, lon, lat] + [0] * 6 + [0]

    if any(np.isnan(band[x, y]) for band in band_data):
        return [poi_id, lon, lat] + [0] * 6 + [0]

    band_values = [band[x, y] for band in band_data]
    saz_val, saa_val, soz_val, soa_val = saz[x, y], saa[x, y], soz[x, y], soa[x, y]
    lucc_val = lucc_data[x, y]
    water_vapor_val = water_vapor_data[x, y]
    ozone_val = ozone_data[x, y]

    s = SixS()
    s.geometry.solar_z = soz_val
    s.geometry.solar_a = saa_val
    s.geometry.view_z = soa_val
    s.geometry.view_a = saz_val
    s.aero_profile = AeroProfile.PredefinedType(lucc_to_aerosol.get(lucc_val, AeroProfile.Continental))
    s.atmos_profile = AtmosProfile.UserWaterAndOzone(water=water_vapor_val, ozone=ozone_val)

    results = {}
    for i, wavelength in enumerate(band_wavelengths):
        s.wavelength = Wavelength(wavelength)
        s.run()
        results[f'Band{i + 1}'] = s.outputs.pixel_reflectance

    band4 = results.get('Band4', 0)
    band3 = results.get('Band3', 0)
    ndvi = (band4 - band3) / (band4 + band3) if (band4 + band3) != 0 else 0
    results['NDVI'] = ndvi

    return [poi_id, lon, lat] + [results.get(f'Band{i + 1}', 0) for i in range(6)] + [ndvi]


def atmospheric_correction(date, hour):
    base_path = f'preprocessing_data/{date.strftime("%Y%m%d")}'
    lucc_path = f'preprocessing_data/{date.year}'
    output_path = 'processed_data'

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pois = pd.read_csv('D:/Data20240801/Air_stations_lon_lat.csv', header=None,
                       names=['ID', 'Lon', 'Lat']).values.tolist()

    lucc_file = os.path.join(lucc_path, 'H8_LUCC.tif')
    lucc_dataset = gdal.Open(lucc_file)
    lucc_data = lucc_dataset.ReadAsArray()
    geotransform = lucc_dataset.GetGeoTransform()

    mask_file = os.path.join(base_path, 'availability_pixel_mask.npz')
    mask_data = np.load(mask_file)['availability_pixel_mask']

    toa_bands = [np.load(os.path.join(base_path, f'toa_reflectance_albedo_0{i}.npz'))['toa_reflectance'] for i in
                 range(1, 7)]
    mod08_data = np.load(os.path.join(base_path, 'resampled_MOD08_data.npz'))
    water_vapor_data = mod08_data['Atmospheric_Water_Vapor_Mean']
    ozone_data = mod08_data['Total_Ozone_Mean']

    brdf_params = np.load(os.path.join(base_path, 'BRDF_parameters.npz'))
    saz, saa, soz, soa = brdf_params['SAZ'], brdf_params['SAA'], brdf_params['SOZ'], brdf_params['SOA']

    lucc_to_aerosol = {
        1: AeroProfile.Continental,
        2: AeroProfile.Continental,
        3: AeroProfile.Continental,
        13: AeroProfile.Urban,
        17: AeroProfile.Maritime,
        255: AeroProfile.Maritime
    }

    band_wavelengths = [0.47, 0.51, 0.64, 0.86, 1.6, 2.3]
    results = []

    valid_pois = [poi for poi in pois if
                  0 <= lonlat_to_pixel(poi[1], poi[2], geotransform)[0] < mask_data.shape[0] and mask_data[
                      lonlat_to_pixel(poi[1], poi[2], geotransform)] == 0]

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_poi, poi, toa_bands, saz, saa, soz, soa, lucc_data, water_vapor_data, ozone_data,
                            band_wavelengths, lucc_to_aerosol, geotransform, mask_data) for poi in valid_pois]
        for future in as_completed(futures):
            results.append(future.result())

    results_df = pd.DataFrame(results, columns=['ID', 'Lon', 'Lat'] + [f'Band{i + 1}' for i in range(6)] + ['NDVI'])
    results_df.to_csv(os.path.join(output_path, 'L2SR_L2NDVI_results.csv'), index=False)
