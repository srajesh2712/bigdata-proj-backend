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

# AOI window
AOI_X0, AOI_X1 = 0, 200
AOI_Y0, AOI_Y1 = 0, 200
BAND_INDEX = 1  # rasterio bands start at 1

# Paths
ZARR_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/13_tile.b_storage", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/16_tile.b_storage", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/15_tile.b_storage", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/14_tile.b_storage", "2024-02-06"),
]
TIFF_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/13_tile.tif", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/16_tile.tif", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/15_tile.tif", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/14_tile.tif", "2024-02-06"),
]

# --------------------------
# UTILS
# --------------------------
def linear_to_db(arr):
    arr = np.where(arr == 0, np.nan, arr)
    return 10 * np.log10(arr)

# --------------------------
# SINGLE TILE READERS
# --------------------------
def read_zarr_tile(zarr_path, date_str):
    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    mapper = fs.get_mapper(zarr_path)
    ds = xr.open_zarr(mapper, consolidated=True)
    ds = ds.isel(x=slice(AOI_X0, AOI_X1), y=slice(AOI_Y0, AOI_Y1))

    # Rename variable if needed
    if "__xarray_dataarray_variable__" in ds.data_vars:
        ds = ds.rename({"__xarray_dataarray_variable__": "band_data"})

    # Convert to dB with explicit dims
    arr = linear_to_db(ds["band_data"].load().values)
    dims = ("y", "x") if arr.ndim == 2 else ("band", "y", "x")
    ds["band_data"] = xr.DataArray(arr, dims=dims)

    # Expand time dimension
    ds = ds.expand_dims(time=[pd.to_datetime(date_str)])
    return ds

def read_tiff_tile(tif_path, date_str):
    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    clean_path = tif_path.replace("hdfs://namenode:8020", "")
    with fs.open(clean_path, "rb") as f:
        with MemoryFile(f.read()) as memfile:
            with memfile.open() as src:
                window = rasterio.windows.Window(AOI_X0, AOI_Y0, AOI_X1 - AOI_X0, AOI_Y1 - AOI_Y0)
                arr = src.read(BAND_INDEX, window=window)
    arr = linear_to_db(arr)
    da = xr.DataArray(arr[np.newaxis, :, :], dims=("time", "y", "x"),
                      coords={"time":[pd.to_datetime(date_str)]})
    return da.to_dataset(name="band_data")

# --------------------------
# BUILD CUBE FROM FUTURES (parallel)
# --------------------------
def build_cube_from_futures(futures):
    datasets = client.gather(futures)
    ref_ds = datasets[0]
    aligned = [ds.reindex(x=ref_ds.x, y=ref_ds.y, method="nearest") for ds in datasets]
    cube = xr.concat(aligned, dim="time")
    return cube

# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    client = Client('dask-scheduler:8786')
    print(f"Connected to Dask. Workers: {len(client.scheduler_info()['workers'])}")

    # Submit ZARR reads to Dask
    zarr_futures = [client.submit(read_zarr_tile, path, date) for path, date in ZARR_PATHS]
    t0 = time.time()
    zarr_cube = build_cube_from_futures(zarr_futures)
    t1 = time.time()
    print(f"✅ ZARR cube built in {t1 - t0:.3f} sec")
    zarr_mean_ts = zarr_cube["band_data"].mean(dim=("x","y")).values
    print("ZARR AOI mean per time [dB]:", zarr_mean_ts)

    # Submit TIFF reads to Dask
    tiff_futures = [client.submit(read_tiff_tile, path, date) for path, date in TIFF_PATHS]
    t0 = time.time()
    tif_cube = build_cube_from_futures(tiff_futures)
    t1 = time.time()
    print(f"✅ TIFF cube built in {t1 - t0:.3f} sec")
    tif_mean_ts = tif_cube["band_data"].mean(dim=("x","y")).values
    print("TIFF AOI mean per time [dB]:", tif_mean_ts)
    # Take mean over extra dimensions if present
    zarr_mean_ts_scalar = zarr_mean_ts
    if zarr_mean_ts.ndim > 1:
        # collapse all axes except time
        zarr_mean_ts_scalar = zarr_mean_ts.mean(axis=1)

    # --- SUMMARY ---
    print("\n==================== SUMMARY ====================")
    for i, (_, date) in enumerate(ZARR_PATHS):
        print(f"{date}: ZARR={zarr_mean_ts_scalar[i]:.3f}, TIFF={tif_mean_ts[i]:.3f}")
