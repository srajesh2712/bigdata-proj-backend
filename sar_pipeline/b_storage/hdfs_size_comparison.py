import os
import time
import numpy as np
import pandas as pd
import xarray as xr
import fsspec
import rasterio
from rasterio.io import MemoryFile

# --------------------------
# CONFIG
# --------------------------
HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020
os.environ["HADOOP_USER_NAME"] = HADOOP_USER
 
BAND_INDEX = 1

ZARR_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/8/40_tile.b_storage", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/7/43_tile.b_storage", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/6/42_tile.b_storage", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/5/41_tile.b_storage", "2024-02-06"),
]
TIFF_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/8/40_tile.tif", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/7/43_tile.tif", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/6/42_tile.tif", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/5/41_tile.tif", "2024-02-06"),
]

# --------------------------
# HDFS UTILS
# --------------------------

def get_hdfs_size(path, fs):
    """Calculates size in MB for a file or a Zarr directory on HDFS"""
    if fs.isfile(path):
        return fs.size(path) / (1024 * 1024)
    else:
        total_size = 0
        for root, dirs, files in fs.walk(path):
            for f in files:
                full_path = f"{root}/{f}"
                total_size += fs.size(full_path)
        return total_size / (1024 * 1024)
 
# --------------------------
# EXECUTION
# --------------------------
def print_file_dimensions(tiff_paths, fs):
    print(f"\n{'File Name':<25} | {'Dimensions (Width x Height)':<25}")
    print("-" * 55)
    
    for t_path, _ in tiff_paths:
        # We only read the header (bytes-level) via fsspec
        with fs.open(t_path, "rb") as f:
            with rasterio.open(f) as src:
                file_name = os.path.basename(t_path)
                print(f"{file_name:<25} | {src.width:<10} x {src.height:<10}")
     
if __name__ == "__main__":
    # Initialize HDFS filesystem
    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)

    print("---  HDFS Vs Zarr STORAGE ANALYSIS (Per File) ---")
    print(f"{'File Name':<25} | {'TIFF Size':<12} | {'Zarr Size':<12} | {'Savings'}")
    print("-" * 70)

    total_tif_mb = 0
    total_zar_mb = 0

    # Iterate through both paths to compare counterparts
    for (t_path, _), (z_path, _) in zip(TIFF_PATHS, ZARR_PATHS):
        t_size = get_hdfs_size(t_path, fs)
        z_size = get_hdfs_size(z_path, fs)
        
        total_tif_mb += t_size
        total_zar_mb += z_size
        
        file_name = os.path.basename(t_path)
        savings = t_size - z_size
        perc = (savings / t_size) * 100
        
        print(f"{file_name:<25} | {t_size:>8.2f} MB | {z_size:>8.2f} MB | {perc:>5.1f}%")
    print_file_dimensions(TIFF_PATHS, fs)

    print("-" * 70)
    print(f"{'TOTAL':<25} | {total_tif_mb:>8.2f} MB | {total_zar_mb:>8.2f} MB | {((total_tif_mb-total_zar_mb)/total_tif_mb)*100:>5.1f}%")
    print(f"\nOverall Compression Benefit: {total_tif_mb - total_zar_mb:.2f} MB saved.")
    print("-" * 40)
 
