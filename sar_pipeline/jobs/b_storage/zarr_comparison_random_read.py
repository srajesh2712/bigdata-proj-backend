import rioxarray
import xarray as xr
import rasterio
import os
import time
import shutil
import numpy as np
from collections import defaultdict

# -----------------------------
# CONFIG
# -----------------------------
input_tif = "data/S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE_9.tif"
output_zarr = "test_sar.b_storage"

CHUNK_SIZE = 512

N_RANDOM_PIXELS = 2000
N_WINDOW_READS = 50
WINDOW_SIZE = 512

# Cleanup old b_storage
if os.path.exists(output_zarr):
    shutil.rmtree(output_zarr)

print(f"\n--- FAIR TIFF vs ZARR BENCHMARK ---")
print(f"Input TIFF: {input_tif}")

# -----------------------------
# 1) Load TIFF
# -----------------------------
start_load = time.time()
rds = rioxarray.open_rasterio(input_tif, chunks={"x": CHUNK_SIZE, "y": CHUNK_SIZE})
h, w = rds.shape[-2:]
print(f"Raster shape: height={h}, width={w}")
print(f"Load time (rioxarray open): {time.time() - start_load:.4f}s")

# -----------------------------
# 2) Convert TIFF -> ZARR
# -----------------------------
start_conv = time.time()
rds.to_zarr(output_zarr, mode="w", consolidated=True)
conv_time = time.time() - start_conv

# -----------------------------
# 3) Size comparison
# -----------------------------
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
print(f"Conversion Time:  {conv_time:.4f}s")

# -----------------------------
# 4) Open Zarr
# -----------------------------
ds = xr.open_zarr(output_zarr, consolidated=True)
varname = list(ds.data_vars)[0]
print(f"\nZarr Variable Used: {varname}")

# -----------------------------
# 5) Generate random pixel coordinates
# -----------------------------
rand_x = np.random.randint(0, w, size=N_RANDOM_PIXELS)
rand_y = np.random.randint(0, h, size=N_RANDOM_PIXELS)

# -----------------------------
# 6) TIFF Random Pixel Reads (RasterIO)
# -----------------------------
print("\n--- RANDOM PIXEL TEST (TIFF rasterio) ---")

with rasterio.open(input_tif) as src:
    start = time.time()
    for i in range(N_RANDOM_PIXELS):
        x = rand_x[i]
        y = rand_y[i]
        _ = src.read(1, window=((y, y + 1), (x, x + 1)))
    tiff_time = time.time() - start

print(f"RasterIO total time: {tiff_time:.4f}s")
print(f"RasterIO avg/pixel:  {tiff_time / N_RANDOM_PIXELS:.6f}s")

# -----------------------------
# 7) ZARR Random Pixel Reads (UNFAIR - one by one)
# -----------------------------
print("\n--- RANDOM PIXEL TEST (ZARR naive one-by-one) ---")

start = time.time()
for i in range(N_RANDOM_PIXELS):
    x = rand_x[i]
    y = rand_y[i]
    _ = ds[varname].isel(x=x, y=y).values
zarr_naive_time = time.time() - start

print(f"Zarr naive total time: {zarr_naive_time:.4f}s")
print(f"Zarr naive avg/pixel:  {zarr_naive_time / N_RANDOM_PIXELS:.6f}s")

# -----------------------------
# 8) ZARR Random Pixel Reads (FAIR - chunk caching)
# -----------------------------
print("\n--- RANDOM PIXEL TEST (ZARR fair chunk-based) ---")

# Group random pixels by chunk index
chunk_groups = defaultdict(list)

for i in range(N_RANDOM_PIXELS):
    cx = rand_x[i] // CHUNK_SIZE
    cy = rand_y[i] // CHUNK_SIZE
    chunk_groups[(cy, cx)].append((rand_y[i], rand_x[i]))

start = time.time()

for (cy, cx), coords in chunk_groups.items():
    y0 = cy * CHUNK_SIZE
    x0 = cx * CHUNK_SIZE

    chunk = ds[varname].isel(
        band = 0,
        y=slice(y0, y0 + CHUNK_SIZE),
        x=slice(x0, x0 + CHUNK_SIZE)
    ).values

    for (yy, xx) in coords:
        _ = chunk[yy - y0, xx - x0]

zarr_fair_time = time.time() - start

print(f"Zarr fair total time: {zarr_fair_time:.4f}s")
print(f"Zarr fair avg/pixel:  {zarr_fair_time / N_RANDOM_PIXELS:.6f}s")
print(f"Chunks loaded:         {len(chunk_groups)}")

# -----------------------------
# 9) RANDOM WINDOW READ TEST
# -----------------------------
print("\n--- RANDOM WINDOW READ TEST ---")

rand_x2 = np.random.randint(0, w - WINDOW_SIZE, size=N_WINDOW_READS)
rand_y2 = np.random.randint(0, h - WINDOW_SIZE, size=N_WINDOW_READS)

# TIFF windows
with rasterio.open(input_tif) as src:
    start = time.time()
    for i in range(N_WINDOW_READS):
        x0 = rand_x2[i]
        y0 = rand_y2[i]
        _ = src.read(1, window=((y0, y0 + WINDOW_SIZE), (x0, x0 + WINDOW_SIZE)))
    tiff_window_time = time.time() - start

# Zarr windows
start = time.time()
for i in range(N_WINDOW_READS):
    x0 = rand_x2[i]
    y0 = rand_y2[i]
    _ = ds[varname].isel(
        y=slice(y0, y0 + WINDOW_SIZE),
        x=slice(x0, x0 + WINDOW_SIZE)
    ).values
zarr_window_time = time.time() - start

print(f"TIFF window total: {tiff_window_time:.4f}s")
print(f"TIFF avg/window:   {tiff_window_time / N_WINDOW_READS:.6f}s")

print(f"Zarr window total: {zarr_window_time:.4f}s")
print(f"Zarr avg/window:   {zarr_window_time / N_WINDOW_READS:.6f}s")

# -----------------------------
# 10) SUMMARY
# -----------------------------
print("\n================ SUMMARY ================")
print(f"TIFF random pixels:        {tiff_time:.4f}s")
print(f"Zarr naive random pixels:  {zarr_naive_time:.4f}s")
print(f"Zarr fair random pixels:   {zarr_fair_time:.4f}s")

print(f"\nPixel speedup naive:  {tiff_time / zarr_naive_time:.2f}x (TIFF/Zarr)")
print(f"Pixel speedup fair:   {tiff_time / zarr_fair_time:.2f}x (TIFF/Zarr)")

print(f"\nTIFF window reads:    {tiff_window_time:.4f}s")
print(f"Zarr window reads:    {zarr_window_time:.4f}s")
print(f"Window speedup:       {tiff_window_time / zarr_window_time:.2f}x (TIFF/Zarr)")
print("=========================================")
