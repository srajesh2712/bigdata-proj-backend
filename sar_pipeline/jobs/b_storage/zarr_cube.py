import matplotlib
matplotlib.use("Agg")  # Non-GUI backend to avoid Tkinter warnings

import numpy as np
import xarray as xr
import fsspec
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# --------------------------
# 0️⃣ Config
# --------------------------
HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

ZARR_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/13_tile.b_storage", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/16_tile.b_storage", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/15_tile.b_storage", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/14_tile.b_storage", "2024-02-06"),
]

# --------------------------
# 1️⃣ Connect to HDFS and load Zarrs
# --------------------------
fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
datasets = []

for path, date_str in ZARR_PATHS:
    print(f"📂 Loading Zarr: {path}")
    mapper = fs.get_mapper(path)
    ds = xr.open_zarr(mapper, consolidated=True)

    # Rename default variable if needed
    if "__xarray_dataarray_variable__" in ds.data_vars:
        ds = ds.rename({"__xarray_dataarray_variable__": "band_data"})

    # Add time coordinate
    ds = ds.expand_dims(time=[pd.to_datetime(date_str)])
    datasets.append(ds)

# Align to common grid
ref_ds = datasets[0]
aligned_datasets = [ds.reindex(x=ref_ds.x, y=ref_ds.y, method="nearest") for ds in datasets]

# Concatenate into time-series cube
cube = xr.concat(aligned_datasets, dim="time")
print("✅ Time-series cube created")
print("📌 Cube dims:", cube.dims)
print("📌 Variables:", list(cube.data_vars))
print("📌 Coords:", list(cube.coords))

# --------------------------
# 2️⃣ Extract AOI window (example 200x200)
# --------------------------
band_index = 0
window = cube.band_data.isel(
    band=band_index,
    x=slice(0, 200),
    y=slice(0, 200)
)

# --------------------------
# 3️⃣ Mask zeros and convert to dB
# --------------------------
window_nonzero = window.where(window != 0)
window_nonzero_db = 10 * np.log10(window_nonzero)  # linear -> dB

# --------------------------
# 4️⃣ Compute AOI mean over time in dB
# --------------------------
window_mean_db = window_nonzero_db.mean(dim="time", skipna=True)
print("✅ AOI mean over time [dB] (ignoring zeros/NaNs):")
print(window_mean_db.values)

# --------------------------
# 5️⃣ Extract per-pixel time series for valid pixels
# --------------------------
mask_valid = ~np.isnan(window_mean_db.values)
y_idx, x_idx = np.where(mask_valid)
print(f"✅ Total valid pixels in window: {len(y_idx)}")

if len(y_idx) > 0:
    y0, x0 = y_idx[0], x_idx[0]
    ts_pixel_db = window_nonzero_db.isel(y=y0, x=x0).values
    print(f"Time series for first valid pixel (y={y0}, x={x0}) [dB]: {ts_pixel_db}")

# Example: specific pixel
pixel_ts_db = window_nonzero_db.isel(y=100, x=50).values
time = cube.time.values

# --------------------------
# 6️⃣ Plot pixel time series in dB
# --------------------------
plt.figure(figsize=(30,20))
plt.plot(time, pixel_ts_db, marker='o')
plt.title("Backscatter Time Series (Pixel y=100, x=50) [dB]")
plt.xlabel("Date")
plt.ylabel("Backscatter [dB]")
plt.xticks(rotation=45)
plt.grid(True)
plt.gcf().autofmt_xdate() 
plt.savefig("pixel_timeseries_dB.png")
plt.close()

# --------------------------
# 7️⃣ Compute AOI mean per time step in dB
# --------------------------
window_data_db = window_nonzero_db.values
mean_per_time_db = np.nanmean(window_data_db, axis=(1,2))
print("✅ Mean AOI backscatter per time step [dB]:", mean_per_time_db)

plt.figure(figsize=(30,20))
plt.plot(time, mean_per_time_db, marker='o', color='blue')
plt.title("AOI Mean Backscatter Over Time [dB]")
plt.xlabel("Date")
plt.ylabel("Mean Backscatter [dB]")
plt.grid(True)
plt.savefig("AOI_mean_timeseries_dB.png")
plt.close()

# --------------------------
# 8️⃣ Create GIF of AOI progression in dB
# --------------------------
fig, ax = plt.subplots(figsize=(5,5))

vmin_db = np.nanmin(window_data_db)
vmax_db = np.nanmax(window_data_db)

def update(frame):
    ax.clear()
    im = ax.imshow(window_data_db[frame], cmap='viridis', vmin=vmin_db, vmax=vmax_db)
    ax.set_title(f"Backscatter [dB]: {pd.to_datetime(time[frame]).date()}")
    return [im]

ani = animation.FuncAnimation(fig, update, frames=len(time), blit=False)
ani.save("flood_aoi_dB.gif", writer='pillow', fps=1)
plt.close()
print("✅ GIF saved as flood_aoi_dB.gif")
