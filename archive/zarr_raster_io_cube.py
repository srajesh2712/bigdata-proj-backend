import matplotlib
matplotlib.use("Agg")

import numpy as np
import xarray as xr
import fsspec
import pandas as pd
import os
import time
import rasterio
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# --------------------------
# CONFIG
# --------------------------
HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

AOI_X0, AOI_X1 = 0, 200
AOI_Y0, AOI_Y1 = 0, 200
BAND_INDEX = 1   # rasterio band starts from 1

# ---- ZARR PATHS ----
ZARR_PATHS = [
    ("/user/btcchl0040/dask_preprocessed/1/13_tile.storage", "2024-01-01"),
    ("/user/btcchl0040/dask_preprocessed/2/16_tile.storage", "2024-01-13"),
    ("/user/btcchl0040/dask_preprocessed/3/15_tile.storage", "2024-01-25"),
    ("/user/btcchl0040/dask_preprocessed/4/14_tile.storage", "2024-02-06"),
]

# ---- TIFF PATHS (must exist locally or mounted path) ----
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
# 1) ZARR CUBE WORKFLOW
# --------------------------
def run_zarr_workflow():
    print("\n==================== ZARR WORKFLOW ====================")

    t0 = time.time()

    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    datasets = []

    for path, date_str in ZARR_PATHS:
        mapper = fs.get_mapper(path)
        ds = xr.open_zarr(mapper, consolidated=True)
        ds = ds.isel(x=slice(AOI_X0, AOI_X1), y=slice(AOI_Y0, AOI_Y1))
        if "__xarray_dataarray_variable__" in ds.data_vars:
            ds = ds.rename({"__xarray_dataarray_variable__": "band_data"})

        ds = ds.expand_dims(time=[pd.to_datetime(date_str)])
        datasets.append(ds)

    ref_ds = datasets[0]
    aligned_datasets = [ds.reindex(x=ref_ds.x, y=ref_ds.y, method="nearest") for ds in datasets]

    cube = xr.concat(aligned_datasets, dim="time")
    cube_build_time = time.time() - t0

    print(f"✅ Zarr cube build time: {cube_build_time:.4f} sec")
    print("Cube dims:", cube.dims)

    # Extract AOI window
    t1 = time.time()
    window = cube.band_data.isel(
        band=0,
        x=slice(AOI_X0, AOI_X1),
        y=slice(AOI_Y0, AOI_Y1)
    ).values
    aoi_read_time = time.time() - t1
    print(f"✅ Zarr AOI read time: {aoi_read_time:.4f} sec")

    # Convert to dB
    window_db = linear_to_db(window)

    # Mean per time step
    mean_per_time_db = np.nanmean(window_db, axis=(1, 2))
    print("✅ Zarr AOI mean per time [dB]:", mean_per_time_db)

    # Save plot
    time_vals = [pd.to_datetime(x).date() for x in cube.time.values]

    plt.figure(figsize=(10, 5))
    plt.plot(time_vals, mean_per_time_db, marker="o")
    plt.title("Zarr AOI Mean Backscatter [dB]")
    plt.xlabel("Date")
    plt.ylabel("Backscatter [dB]")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.grid(True)
    plt.savefig("zarr_aoi_mean_db.png")
    plt.close()

    # GIF
    t2 = time.time()
    fig, ax = plt.subplots(figsize=(6, 6))

    vmin = np.nanmin(window_db)
    vmax = np.nanmax(window_db)

    def update(frame):
        ax.clear()
        im = ax.imshow(window_db[frame], vmin=vmin, vmax=vmax, cmap="viridis")
        ax.set_title(f"Zarr AOI [dB] {time_vals[frame]}")
        return [im]

    ani = animation.FuncAnimation(fig, update, frames=len(time_vals), blit=False)
    ani.save("zarr_aoi.gif", writer="pillow", fps=1)
    plt.close()

    gif_time = time.time() - t2
    print(f"✅ Zarr GIF creation time: {gif_time:.4f} sec")

    total_time = time.time() - t0
    print(f"✅ Zarr total workflow time: {total_time:.4f} sec")

    return cube_build_time, aoi_read_time, gif_time, total_time
    
def store_cube(cube):
    # HDFS path for stored cube
    cube_store_path = "/user/btcchl0040/dask_preprocessed/aoi_cube.storage"

    # Use fsspec to handle HDFS storage
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user="btcchl0040")

    # Write cube to HDFS
    with fs.open(cube_store_path, mode="wb") as f:
        cube.to_zarr(f, consolidated=True)
# --------------------------
# 2) RASTERIO STACK WORKFLOW (HDFS)
# --------------------------
def run_rasterio_workflow():
    print("\n==================== RASTERIO WORKFLOW (HDFS) ====================")

    t0 = time.time()

    fs = fsspec.filesystem("hdfs", host=HDFS_NAMENODE, port=HDFS_PORT, user=HADOOP_USER)
    stack = []
    dates = []

    t1 = time.time()
    for tif_path, date_str in TIFF_PATHS:
        # Remove HDFS protocol if present
        clean_path = tif_path.replace("hdfs://namenode:8020", "")
        with fs.open(clean_path, "rb") as f:
            # Use MemoryFile to let rasterio read from file-like object
            from rasterio.io import MemoryFile
            with MemoryFile(f.read()) as memfile:
                with memfile.open() as src:
                    window = rasterio.windows.Window(AOI_X0, AOI_Y0, AOI_X1 - AOI_X0, AOI_Y1 - AOI_Y0)
                    arr = src.read(BAND_INDEX, window=window)
                    stack.append(arr)
                    dates.append(pd.to_datetime(date_str).date())

    aoi_stack_read_time = time.time() - t1
    print(f"✅ Rasterio AOI stack read time: {aoi_stack_read_time:.4f} sec")

    stack = np.array(stack)  # shape = (time, y, x)

    # Convert to dB
    stack_db = linear_to_db(stack)

    # Mean per time step
    mean_per_time_db = np.nanmean(stack_db, axis=(1, 2))
    print("✅ Rasterio AOI mean per time [dB]:", mean_per_time_db)

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(dates, mean_per_time_db, marker="o")
    plt.title("Rasterio AOI Mean Backscatter [dB]")
    plt.xlabel("Date")
    plt.ylabel("Backscatter [dB]")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.grid(True)
    plt.savefig("rasterio_aoi_mean_db.png")
    plt.close()

    # GIF
    t2 = time.time()
    fig, ax = plt.subplots(figsize=(6, 6))

    vmin = np.nanmin(stack_db)
    vmax = np.nanmax(stack_db)

    def update(frame):
        ax.clear()
        im = ax.imshow(stack_db[frame], vmin=vmin, vmax=vmax, cmap="viridis")
        ax.set_title(f"Rasterio AOI [dB] {dates[frame]}")
        return [im]

    ani = animation.FuncAnimation(fig, update, frames=len(dates), blit=False)
    ani.save("rasterio_aoi.gif", writer="pillow", fps=1)
    plt.close()

    gif_time = time.time() - t2
    print(f"✅ Rasterio GIF creation time: {gif_time:.4f} sec")

    total_time = time.time() - t0
    print(f"✅ Rasterio total workflow time: {total_time:.4f} sec")

    return aoi_stack_read_time, gif_time, total_time
# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    zarr_cube_build, zarr_aoi_read, zarr_gif, zarr_total = run_zarr_workflow()
    ras_aoi_read, ras_gif, ras_total = run_rasterio_workflow()

    print("\n==================== FINAL COMPARISON ====================")
    print(f"Zarr cube build time:     {zarr_cube_build:.4f}s")
    print(f"Zarr AOI read time:       {zarr_aoi_read:.4f}s")
    print(f"Zarr GIF time:            {zarr_gif:.4f}s")
    print(f"Zarr total:               {zarr_total:.4f}s")

    print(f"Rasterio AOI read time:   {ras_aoi_read:.4f}s")
    print(f"Rasterio GIF time:        {ras_gif:.4f}s")
    print(f"Rasterio total:           {ras_total:.4f}s")

    print("\n--- SPEEDUP (Rasterio/Zarr) ---")
    print(f"AOI read speedup:   {ras_aoi_read / zarr_aoi_read:.2f}x")
    print(f"Total speedup:      {ras_total / zarr_total:.2f}x")
    print("==========================================================")
