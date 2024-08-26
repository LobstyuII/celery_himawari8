import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial import cKDTree
import os


def nearest_valid_value(mod08_variable, mod08_points, h8_points, h8_lat_grid):
    mod08_variable_flat = mod08_variable.flatten()
    valid_mask = (mod08_variable_flat > 0)
    valid_mod08_points = mod08_points[valid_mask]
    valid_mod08_values = mod08_variable_flat[valid_mask]

    tree = cKDTree(valid_mod08_points)
    distances, indices = tree.query(h8_points, k=1)

    resampled = valid_mod08_values[indices]
    return resampled.reshape(h8_lat_grid.shape)


def process_mod08_data(mod08_file, h8_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    mod08_data = nc.Dataset(mod08_file, 'r')
    mod08_xdim = mod08_data.variables['XDim'][:]
    mod08_ydim = mod08_data.variables['YDim'][:]

    h8_data = nc.Dataset(h8_file, 'r')
    h8_lat = h8_data.variables['latitude'][:]
    h8_lon = h8_data.variables['longitude'][:]
    h8_lat_grid, h8_lon_grid = np.meshgrid(h8_lat, h8_lon)

    mod08_lon_grid, mod08_lat_grid = np.meshgrid(mod08_xdim, mod08_ydim)
    mod08_points = np.array([mod08_lat_grid.flatten(), mod08_lon_grid.flatten()]).T
    h8_points = np.array([h8_lat_grid.flatten(), h8_lon_grid.flatten()]).T

    total_ozone_resampled = nearest_valid_value(mod08_data.variables['Total_Ozone_Mean'][:], mod08_points, h8_points,
                                                h8_lat_grid)
    atmospheric_water_vapor_resampled = nearest_valid_value(mod08_data.variables['Atmospheric_Water_Vapor_Mean'][:],
                                                            mod08_points, h8_points, h8_lat_grid)
    aod_resampled = nearest_valid_value(mod08_data.variables['AOD_550_Dark_Target_Deep_Blue_Combined_Mean'][:],
                                        mod08_points, h8_points, h8_lat_grid)

    np.savez(os.path.join(output_dir, 'resampled_MOD08_data.npz'),
             Total_Ozone_Mean=total_ozone_resampled,
             Atmospheric_Water_Vapor_Mean=atmospheric_water_vapor_resampled,
             AOD_550_Dark_Target_Deep_Blue_Combined_Mean=aod_resampled)


def process_mod08(date, hour):
    mod08_file = f'downloaded_data/mod08/MOD08_{date.strftime("%Y%m%d")}.hdf'
    h8_file = f'downloaded_data/h8l1/himawari_{date.strftime("%Y%m%d")}_{hour:02d}.nc'
    output_dir = f'preprocessing_data/{date.strftime("%Y%m%d")}'
    process_mod08_data(mod08_file, h8_file, output_dir)
