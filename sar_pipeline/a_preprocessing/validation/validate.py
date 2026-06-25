import rasterio
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from scipy.stats import ks_2samp

# -----------------------------
# FILE PATHS
# -----------------------------
snap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Snap_preprocessed.tif"
pysnap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Pysnap_preprocessed.tif"
spark_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_spark_preprocessed.tif"
dask_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_dask_preprocessed.tif"
zarr_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile.zarr"


# -----------------------------
# READ TIFF
# -----------------------------
def read_band(path, band=1):
    with rasterio.open(path) as src:
        return src.read(band).astype(np.float32)


# -----------------------------
# READ ZARR (FIXED)
# -----------------------------
def read_zarr_band(path, band=1):
    ds = xr.open_zarr(path)
    var = list(ds.data_vars)[0]
    arr = ds[var].values
    return arr[band - 1].astype(np.float32)


# -----------------------------
# SAFE ALIGNMENT (CRITICAL FIX)
# -----------------------------
def align(a, b):
    min_y = min(a.shape[0], b.shape[0])
    min_x = min(a.shape[1], b.shape[1])
    return a[:min_y, :min_x], b[:min_y, :min_x]


# -----------------------------
# METRICS (RMSE + CORR in dB)
# -----------------------------
def metrics(a, b):

    a, b = align(a, b)

    mask = np.isfinite(a) & np.isfinite(b) & (a > 0) & (b > 0)
    a = a[mask]
    b = b[mask]

    a_db = 10 * np.log10(a)
    b_db = 10 * np.log10(b)

    rmse = np.sqrt(np.mean((a_db - b_db) ** 2))
    corr = np.corrcoef(a_db, b_db)[0, 1]

    return rmse, corr


# -----------------------------
# KS DISTANCE (distribution test)
# -----------------------------
def ks_distance(a, b):

    a, b = align(a, b)

    mask = np.isfinite(a) & np.isfinite(b) & (a > 0) & (b > 0)

    a = a[mask].ravel()
    b = b[mask].ravel()

    stat, p = ks_2samp(a, b)
    return stat, p


# -----------------------------
# INSPECTION
# -----------------------------
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


# -----------------------------
# RUN INSPECTION
# -----------------------------
inspect_tiff(snap_file)
inspect_tiff(pysnap_file)
inspect_tiff(spark_file)
inspect_tiff(dask_file)
inspect_zarr(zarr_file)


# -----------------------------
# VALIDATION LOOP
# -----------------------------
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

        print("\n--- RMSE + CORR (dB domain) ---")
        for name, arr in datasets.items():
            rmse, corr = metrics(base, arr)
            print(f"{name:8s} -> RMSE: {rmse:.5f}, Corr: {corr:.5f}")

        print("\n--- KS TEST (linear domain) ---")
        for name, arr in datasets.items():
            stat, p = ks_distance(base, arr)
            print(f"{name:8s} -> KS stat: {stat:.5f}, p-value: {p:.5e}")


# -----------------------------
# RUN
# -----------------------------
run_validation()