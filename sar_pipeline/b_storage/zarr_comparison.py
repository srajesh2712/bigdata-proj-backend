import rioxarray
import xarray as xr
import rasterio
import os
import time
import shutil
import numpy as np
import zarr

# --- CONFIGURATION ---
input_tif = "data/S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE_9.tif"
output_zarr = "test_sar.b_storage"

N_RANDOM_READS = 200
N_WINDOW_READS = 50
WINDOW_SIZE = 256

# Remove existing test folder if it exists
if os.path.exists(output_zarr):
    shutil.rmtree(output_zarr)

print(f"\n--- Starting Standalone Test for {input_tif} ---")

# ----------------------------------------------------
# 1. LOAD TIFF
# ----------------------------------------------------
start_load = time.time()
rds = rioxarray.open_rasterio(input_tif)
print(f"Load time (rioxarray open): {time.time() - start_load:.4f}s")

h, w = rds.shape[-2:]
print(f"Raster shape: height={h}, width={w}")

# ----------------------------------------------------
# 2. CONVERT TIFF -> ZARR WITH COMPRESSION
# ----------------------------------------------------
compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

# encoding must match the variable name in the DataArray
varname_rds = rds.name if rds.name else "__xarray_dataarray_variable__"

encoding = {
    varname_rds: {
        "compressor": compressor,
        "chunks": (1, 512, 512)   # band, y, x (good chunk size for SAR)
    }
}

start_conv = time.time()
rds.to_zarr(output_zarr, mode="w", consolidated=True, encoding=encoding)
end_conv = time.time()

# ----------------------------------------------------
# 3. SIZE METRICS
# ----------------------------------------------------
tif_size = os.path.getsize(input_tif) / (1024 * 1024)

zarr_size = sum(
    os.path.getsize(os.path.join(dp, f))
    for dp, _, filenames in os.walk(output_zarr)
    for f in filenames
) / (1024 * 1024)

print(f"\n[STORAGE RESULTS]")
print(f"Original GeoTIFF: {tif_size:.2f} MB")
print(f"Zarr Storage:     {zarr_size:.2f} MB")
print(f"Compression:      {((tif_size - zarr_size) / tif_size) * 100:.1f}% reduction")
print(f"Conversion Time:  {end_conv - start_conv:.4f}s")

# ----------------------------------------------------
# 4. OPEN ZARR
# ----------------------------------------------------
ds = xr.open_zarr(output_zarr, consolidated=True)
varname = list(ds.data_vars)[0]
print(f"\nZarr Variable Used: {varname}")

# ----------------------------------------------------
# 5. RANDOM PIXEL ACCESS TEST
# ----------------------------------------------------
print("\n--- RANDOM PIXEL ACCESS TEST ---")

rand_x = np.random.randint(0, w, size=N_RANDOM_READS)
rand_y = np.random.randint(0, h, size=N_RANDOM_READS)

# ---- RasterIO pixel reads ----
with rasterio.open(input_tif) as src:
    start = time.time()
    for i in range(N_RANDOM_READS):
        x = rand_x[i]
        y = rand_y[i]
        val = src.read(1, window=((y, y+1), (x, x+1)))
    tiff_time = time.time() - start

print(f"RasterIO random pixel reads: {tiff_time:.4f}s total")
print(f"RasterIO avg per pixel:      {tiff_time / N_RANDOM_READS:.6f}s")

# ---- Zarr pixel reads ----
start = time.time()
for i in range(N_RANDOM_READS):
    x = rand_x[i]
    y = rand_y[i]
    val = ds[varname].isel(x=x, y=y).values
zarr_time = time.time() - start

print(f"Zarr random pixel reads:     {zarr_time:.4f}s total")
print(f"Zarr avg per pixel:          {zarr_time / N_RANDOM_READS:.6f}s")

# ----------------------------------------------------
# 6. RANDOM WINDOW ACCESS TEST
# ----------------------------------------------------
print("\n--- RANDOM WINDOW (AOI) READ TEST ---")

rand_x2 = np.random.randint(0, w - WINDOW_SIZE, size=N_WINDOW_READS)
rand_y2 = np.random.randint(0, h - WINDOW_SIZE, size=N_WINDOW_READS)

# ---- RasterIO window reads ----
with rasterio.open(input_tif) as src:
    start = time.time()
    for i in range(N_WINDOW_READS):
        x0 = rand_x2[i]
        y0 = rand_y2[i]
        arr = src.read(1, window=((y0, y0+WINDOW_SIZE), (x0, x0+WINDOW_SIZE)))
    tiff_window_time = time.time() - start

print(f"RasterIO window reads: {tiff_window_time:.4f}s total")
print(f"RasterIO avg per AOI:  {tiff_window_time / N_WINDOW_READS:.6f}s")

# ---- Zarr window reads ----
start = time.time()
for i in range(N_WINDOW_READS):
    x0 = rand_x2[i]
    y0 = rand_y2[i]
    arr = ds[varname].isel(
        x=slice(x0, x0+WINDOW_SIZE),
        y=slice(y0, y0+WINDOW_SIZE)
    ).values
zarr_window_time = time.time() - start

print(f"Zarr window reads:    {zarr_window_time:.4f}s total")
print(f"Zarr avg per AOI:     {zarr_window_time / N_WINDOW_READS:.6f}s")

# ----------------------------------------------------
# 7. SUMMARY
# ----------------------------------------------------
print("\n================ SUMMARY ================")
print(f"Random Pixel Read Speedup (RasterIO/Zarr): {tiff_time / zarr_time:.2f}x")
print(f"Window Read Speedup (RasterIO/Zarr):       {tiff_window_time / zarr_window_time:.2f}x")
print("=========================================")
