import rioxarray
import xarray as xr
import os
import time

# ------------------------
# 1️⃣ Load the GeoTIFF with Dask chunks
# ------------------------
file_path = "data/tile_Q2_1775417427.87809.tif"
rds = rioxarray.open_rasterio(file_path, chunks={'x': 512, 'y': 512})

# ------------------------
# 2️⃣ Convert to Zarr (with compression)
# ------------------------
start_time = time.time()
zarr_path = "sar_data.storage"

# 'Blosc' with 'zstd' is good for SAR data
rds.to_zarr(zarr_path, mode='w', consolidated=True)
end_time = time.time()

# ------------------------
# 3️⃣ Compare sizes
# ------------------------
tif_size = os.path.getsize(file_path) / (1024*1024)
zarr_size = sum(os.path.getsize(os.path.join(dirpath, f)) 
                for dirpath, _, filenames in os.walk(zarr_path) 
                for f in filenames) / (1024*1024)

print(f"Original GeoTIFF: {tif_size:.2f} MB")
print(f"Zarr Compressed: {zarr_size:.2f} MB")
print(f"Conversion Time: {end_time - start_time:.2f} seconds")

# ------------------------
# 4️⃣ Open the Zarr lazily
# ------------------------
ds = xr.open_zarr(zarr_path, consolidated=True)

print("\n📌 Dataset Variables:", list(ds.data_vars))
print("📌 Dataset Coords:", list(ds.coords))
print("x coords:", ds['x'].values[:10])
print("y coords:", ds['y'].values[:10])
# pick first variable automatically
varname = list(ds.data_vars)[0]
print(f"\n✅ Using variable: {varname}")

# ------------------------
# 5️⃣ Define AOI (lat/lon) and fetch
# ------------------------
x_min, x_max = ds['x'].min().item(), ds['x'].max().item()
y_min, y_max = ds['y'].min().item(), ds['y'].max().item()
print(f"x range: {x_min} to {x_max}")
print(f"y range: {y_min} to {y_max}")

# rioxarray preserves spatial info, so we can use sel
subset = ds[varname].sel(
    x=slice(x_min, x_min + 0.1005),     # x increasing
    y=slice(y_max, y_max - 0.0005)      # y decreasing
)


 
# Trigger read
start_read = time.time()
subset_data = subset.compute()
end_read = time.time()

print(f"\n✅ AOI shape: {subset_data.shape}")
print(f"✅ AOI data sample:\n{subset_data.values}")
print(f"✅ AOI read time: {end_read - start_read:.4f} seconds")

# ------------------------
# 6️⃣ Optionally write AOI back to GeoTIFF
# ------------------------
subset.rio.to_raster("subset_aoi.tif")
print("✅ AOI written to 'subset_aoi.tif'")
