import rasterio
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from scipy.stats import ks_2samp

snap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Snap_preprocessed.tif"
pysnap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Pysnap_preprocessed.tif"
spark_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_spark_preprocessed.tif"
dask_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_dask_preprocessed.tif"
zarr_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile.zarr"



def read_band(path, band=1):
    with rasterio.open(path) as src:
        return src.read(band).astype(np.float32)


def read_zarr_band(path, band=1):
    ds = xr.open_zarr(path)
    var = list(ds.data_vars)[0]
    arr = ds[var].values
    return arr[band - 1].astype(np.float32)



def align(a, b):
    min_y = min(a.shape[0], b.shape[0])
    min_x = min(a.shape[1], b.shape[1])
    return a[:min_y, :min_x], b[:min_y, :min_x]


def inspect_tiff(path):
    with rasterio.open(path) as src:
        print("\nTIFF:", path)
        print("Bands:", src.count)

        for i in range(1, src.count + 1):
            band = src.read(i)
            print(f"Band {i}: min={band.min()}, max={band.max()}")


def inspect_zarr(path):
    ds = xr.open_zarr(path)
    arr = list(ds.data_vars.values())[0].values

    print("\nZARR:", path)
    for i in range(arr.shape[0]):
        print(f"Band {i+1}: min={arr[i].min()}, max={arr[i].max()}")

inspect_tiff(snap_file)
inspect_tiff(pysnap_file)
inspect_tiff(spark_file)
inspect_tiff(dask_file)
inspect_zarr(zarr_file)


def run_validation():

    for band in [1, 2]:

        print("\n==============================")
        print("BAND:", band)
        print("==============================")

        snap = read_band(snap_file, band)
        pysnap = read_band(pysnap_file, band)
        spark = read_band(spark_file, band)
        dask = read_band(dask_file, band)
        zarr = read_zarr_band(zarr_file, band)

        datasets = {
            "SNAP": snap,
            "PySNAP": pysnap,
            "Spark": spark,
            "Dask": dask,
            "Zarr": zarr
        }

        base = snap


run_validation()