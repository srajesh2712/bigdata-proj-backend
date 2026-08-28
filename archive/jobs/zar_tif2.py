import os
import time
import numpy as np
import pandas as pd
import xarray as xr
import fsspec
import rasterio
from rasterio.io import MemoryFile
from dask.distributed import Client

# --------------------------
# CONFIG
# --------------------------
HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

AOI_X0, AOI_X1 = 0, 5000
AOI_Y0, AOI_Y1 = 0, 5000
BAND_INDEX = 1

ZARR_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/20_tile.storage", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/17_tile.storage", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/18_tile.storage", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/19_tile.storage", "2024-02-06"),
]
TIFF_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/20_tile.tif", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/17_tile.tif", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/18_tile.tif", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/19_tile.tif", "2024-02-06"),
]

# Simulate a larger load
SCALED_ZARR = ZARR_PATHS * 1
SCALED_TIFF = TIFF_PATHS * 1

# --------------------------
# READERS
# --------------------------

def read_zarr_tile(zarr_path, date_str):
    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    mapper = fs.get_mapper(zarr_path)
    # Open without loading into RAM
    ds = xr.open_zarr(mapper, consolidated=True)
    curr_var = list(ds.data_vars)[0]
    ds = ds.rename({curr_var: "band_data"})
    
    # Stay lazy!
    ds = ds.isel(x=slice(AOI_X0, AOI_X1), y=slice(AOI_Y0, AOI_Y1))
    return ds.expand_dims(time=[pd.to_datetime(date_str)])

def read_tiff_tile(tif_path, date_str):
    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    with fs.open(tif_path, "rb") as f:
        # BOTTLENECK: 500MB transferred over HDFS for EVERY file
        data = f.read() 
        with MemoryFile(data) as memfile:
            with memfile.open() as src:
                window = rasterio.windows.Window(AOI_X0, AOI_Y0, AOI_X1 - AOI_X0, AOI_Y1 - AOI_Y0)
                arr = src.read(BAND_INDEX, window=window)
                
    da = xr.DataArray(arr[np.newaxis, :, :], dims=("time", "y", "x"),
                      coords={"time":[pd.to_datetime(date_str)]})
    return da.to_dataset(name="band_data")

def build_cube_from_futures(futures, client):
    datasets = client.gather(futures)
    return xr.concat(datasets, dim="time")

# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    client = Client('dask-scheduler:8786')
    
    print(f"Running Benchmark with {len(SCALED_ZARR)} files...")

    # --- ZARR TEST ---
    t0 = time.time()
    zarr_futures = [client.submit(read_zarr_tile, p, d) for p, d in SCALED_ZARR]
    zarr_cube = build_cube_from_futures(zarr_futures, client)
    zarr_final = zarr_cube["band_data"].mean().compute()
    t1 = time.time()
    print(f"✅ ZARR Total Time: {t1 - t0:.3f} sec")

    # --- TIFF TEST ---
    t0 = time.time()
    tiff_futures = [client.submit(read_tiff_tile, p, d) for p, d in SCALED_TIFF]
    tif_cube = build_cube_from_futures(tiff_futures, client)
    tif_final = tif_cube["band_data"].mean().compute()
    t2 = time.time()
    print(f"✅ TIFF Total Time: {t2 - t0:.3f} sec")

    print(f"\nSpeed difference: {t2-t0:.3f}s (TIFF) vs {t1-t0:.3f}s (ZARR)")
