from dask.distributed import Client
import pandas as pd
import fsspec
import time
import logging
import os
import numpy as np
import rasterio

from rasterio import MemoryFile
from rasterio.windows import Window
from rasterio.warp import transform_bounds
from rasterio.shutil import copy as rasterio_copy


# ----------------------------
# TILE PROCESSING FUNCTION
# ----------------------------
def process_sar_tile(task_data):

    print("entering process_sar_tile")

    window_id, pre_path, post_path, win_coords = task_data
    start_time = time.time()

    os.environ["HADOOP_USER_NAME"] = "root"
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

    win = Window(*win_coords)

    # ----------------------------
    # SAFE HDFS → LOCAL READ
    # ----------------------------
    def read_local(path, local_path):

        fs.get(path, local_path)

        with rasterio.open(local_path) as src:
            data = src.read(1, window=win)
            transform = src.window_transform(win)
            crs = src.crs
            bounds = src.window_bounds(win)

        return data, transform, crs, bounds

    try:
        local_pre = f"/tmp/pre_{window_id}.tif"
        local_post = f"/tmp/post_{window_id}.tif"
        download_start = time.time()

        pre, _, _, _ = read_local(pre_path, local_pre)

        post, src_transform, src_crs, _ = read_local(
            post_path,
            local_post
        )

        download_time = time.time() - download_start
        
        print(f"Tile {window_id}: Pre max={np.max(pre)}, Post max={np.max(post)}")

        # ----------------------------
        # EMPTY TILE CHECK
        # ----------------------------
        if np.max(pre) <= 0:
            return {
                "tile_id": window_id,
                "pixel_count": 0,
                "status": "EMPTY_LAND",
                "mask_path": None,
                "min_lon": 0.0,
                "min_lat": 0.0,
                "max_lon": 0.0,
                "max_lat": 0.0,
                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration": round(time.time() - start_time, 2),
            }

        compute_start = time.time()

        pre_db = 10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32))
        post_db = 10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32))

        flood_mask = (post_db - pre_db < -5.5).astype(np.uint8)

        pixel_count = int(flood_mask.sum())

        compute_time = time.time() - compute_start
        # ----------------------------
        # WRITE MASK (LOCAL)
        # ----------------------------
        tile_meta = {
            "driver": "GTiff",
            "height": win.height,
            "width": win.width,
            "count": 1,
            "dtype": "uint8",
            "crs": src_crs,
            "transform": src_transform,
            "nodata": 0,
        }

        local_out = f"/tmp/tile_{window_id}.tif"
        hdfs_out = f"/user/btcchl0040/sar/results/masks/tile_{window_id}.tif"

        with MemoryFile() as memfile:
            with memfile.open(**tile_meta) as dst:
                dst.write(flood_mask, 1)

            with memfile.open() as src:
                rasterio_copy(
                    src,
                    local_out,
                    driver="GTiff",
                    compress="LZW",
                    tiled=True,
                    blockxsize=256,
                    blockysize=256,
                )

        # ----------------------------
        # PUSH TO HDFS
        # ----------------------------
        fs.put(local_out, hdfs_out)

        # ----------------------------
        # COORDINATES
        # ----------------------------
        left, bottom, right, top = src.window_bounds(win)

        left_lon, bottom_lat, right_lon, top_lat = transform_bounds(
            src_crs,
            "EPSG:4326",
            left,
            bottom,
            right,
            top,
        )
        
        for f in [local_pre, local_post, local_out]:
            try:
                os.remove(f)
            except OSError:
                pass
        return {
            "tile_id": window_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "mask_path": hdfs_out,
            "min_lon": float(left_lon),
            "min_lat": float(bottom_lat),
            "max_lon": float(right_lon),
            "max_lat": float(top_lat),
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": round(time.time() - start_time, 2),
        }

    except Exception as e:

        return {
            "tile_id": window_id,
            "pixel_count": 0,
            "status": f"ERROR: {str(e)}",
            "mask_path": None,
            "min_lon": 0.0,
            "min_lat": 0.0,
            "max_lon": 0.0,
            "max_lat": 0.0,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": round(time.time() - start_time, 2),
        }


# ----------------------------
# MAIN DRIVER
# ----------------------------
def main():

    print("inside dask processing")

    client = Client("tcp://dask-scheduler:8786")

    print(client)

    pre_p = "/user/btcchl0040/spark_preprocessed/7/43_tile.tif"
    post_p = "/user/btcchl0040/spark_preprocessed/8/40_tile.tif"

    tasks = []

    for r in range(6):
        for c in range(7):

            tasks.append(
                (
                    r * 7 + c,
                    pre_p,
                    post_p,
                    (c * 1024, r * 1024, 1024, 1024),
                )
            )

    start = time.time()

    futures = [
        client.submit(process_sar_tile, task, pure=False)
        for task in tasks
    ]

    results = client.gather(futures)

    total_time = time.time() - start

    print("\n--- FLOOD RESULTS ---")

    total_pixels = 0

    for res in sorted(results, key=lambda x: x["tile_id"]):

        print(
            f"Tile {res['tile_id']:02d} | "
            f"{res['status']} | "
            f"Pixels={res['pixel_count']} | "
            f"Duration={res['duration']} sec"
        )

        total_pixels += res["pixel_count"]

    print("\nTOTAL FLOOD PIXELS:", total_pixels)

    print(f"TOTAL PIPELINE TIME: {total_time:.2f} sec")

    df = pd.DataFrame(results)

    print("\nTask Statistics")
    print(df[["tile_id", "duration"]])

    print("\nAverage Tile Time:", df["duration"].mean())
    print("Max Tile Time:", df["duration"].max())
    print("Min Tile Time:", df["duration"].min())

    client.close()


if __name__ == "__main__":
    main()
