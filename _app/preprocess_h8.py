import netCDF4 as nc
import numpy as np
import os
import matplotlib.pyplot as plt


def preprocess_h8_data(file_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    dataset = nc.Dataset(file_path, 'r')
    lon = dataset.variables['longitude'][:]
    lat = dataset.variables['latitude'][:]

    albedo_bands = {}
    soz_degrees = dataset.variables['SOZ'][:]
    soz_radians = np.deg2rad(soz_degrees)
    cos_soz = np.cos(soz_radians)
    cos_soz[(soz_degrees > 70) | (soz_degrees < 0)] = np.nan

    for i in range(1, 6):
        band_name = f'albedo_0{i}'
        albedo_data = dataset.variables[band_name][:]
        toa_reflectance = albedo_data / cos_soz
        toa_reflectance[toa_reflectance > 0.98] = np.nan
        albedo_bands[band_name] = toa_reflectance

        if np.ma.is_masked(toa_reflectance):
            toa_reflectance = np.ma.filled(toa_reflectance, fill_value=np.nan)

        np.savez_compressed(os.path.join(output_dir, f'toa_reflectance_{band_name}.npz'),
                            toa_reflectance=toa_reflectance)

        plt.figure(figsize=(10, 10))
        toa_img = plt.imshow(toa_reflectance, cmap='viridis', extent=(lon.min(), lon.max(), lat.min(), lat.max()))
        plt.colorbar(toa_img, fraction=0.046, pad=0.04)
        plt.title(f'TOA Reflectance Thumbnail - {band_name}')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        thumb_path = os.path.join(output_dir, f'toa_thumbnail_{band_name}.png')
        plt.savefig(thumb_path, dpi=100, bbox_inches='tight', pad_inches=0.1)
        plt.close()

        # 计算NDVI并生成灰度直方图和缩略图
    b4_toa_reflectance = albedo_bands['albedo_04']
    b3_toa_reflectance = albedo_bands['albedo_03']

    ndvi = (b4_toa_reflectance - b3_toa_reflectance) / (b4_toa_reflectance + b3_toa_reflectance)
    ndvi = np.ma.filled(ndvi, fill_value=np.nan)

    np.savez_compressed(os.path.join(output_dir, 'ndvi.npz'), ndvi=ndvi)

    plt.figure(figsize=(10, 6))
    plt.hist(ndvi[~np.isnan(ndvi)].flatten(), bins=100, color='gray', alpha=0.7)
    plt.title('NDVI Histogram')
    plt.xlabel('NDVI')
    plt.ylabel('Frequency')
    hist_path = os.path.join(output_dir, 'ndvi_histogram.png')
    plt.savefig(hist_path)
    plt.close()

    plt.figure(figsize=(10, 10))
    ndvi_img = plt.imshow(ndvi, cmap='RdYlGn', vmin=-1, vmax=1, extent=(lon.min(), lon.max(), lat.min(), lat.max()))
    plt.colorbar(ndvi_img, fraction=0.046, pad=0.04)
    plt.title('NDVI Thumbnail')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    thumb_path = os.path.join(output_dir, 'ndvi_thumbnail.png')
    plt.savefig(thumb_path, dpi=100, bbox_inches='tight', pad_inches=0.1)
    plt.close()


def preprocess_h8(date, hour):

    file_path = f'downloaded_data/h8l1/himawari_{date.strftime("%Y%m%d")}_{hour:02d}.nc'
    output_dir = f'preprocessing_data/{date.strftime("%Y%m%d")}'
    preprocess_h8_data(file_path, output_dir)
