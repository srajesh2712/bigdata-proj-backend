import os
import time
from datetime import datetime

import fsspec
import numpy as np
import rasterio
from rasterio.windows import Window

HDFS_HOST = "namenode"
HDFS_PORT = 8020
TILE_SIZE = 1024
JOB_IDS = [37, 38]

PRE_TIF = (f"/user/btcchl0040/"
           f"dask_preprocessed/"
           f"{JOB_IDS[0]}/"
           f"{JOB_IDS[0]}_tile.tif")

POST_TIF = (f"/user/btcchl0040/"
            f"dask_preprocessed/"
            f"{JOB_IDS[1]}/"
            f"{JOB_IDS[1]}_tile.tif")


def download_geotiff(hdfs_path, local_path):
    fs = fsspec.filesystem("hdfs", host=HDFS_HOST, port=HDFS_PORT)

    print(f"Downloading:")
    print(f"  HDFS : {hdfs_path}")
    print(f"  Local: {local_path}")

    fs.get(hdfs_path, local_path)


def get_geotiff_info(path):
    with rasterio.open(path) as src:
        width = src.width
        height = src.height
        crs = src.crs
        transform = src.transform

    return height, width, crs, transform


def process_tile(tile_id, pre_src, post_src, window):
    start_time = time.time()

    try:

        pre = pre_src.read(1, window=window)
        post = post_src.read(1, window=window)

        pre_shape = pre.shape
        post_shape = post.shape

        if pre.size == 0:
            return {"tile_id": tile_id, "pixel_count": 0, "status": "EMPTY_TILE",
                "duration": round(time.time() - start_time, 2), "processed_at": datetime.utcnow().isoformat(),
                "pre_shape": pre_shape, "post_shape": post_shape}

        # ----------------------------------------------------
        # EMPTY LAND
        # ----------------------------------------------------

        if np.max(pre) <= 0:
            return {"tile_id": tile_id, "pixel_count": 0, "status": "EMPTY_LAND",
                "duration": round(time.time() - start_time, 2), "processed_at": datetime.utcnow().isoformat(),
                "pre_shape": pre_shape, "post_shape": post_shape}

        pre_db = (10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32)))
        post_db = (10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32)))

        diff = post_db - pre_db
        flood_mask = (diff < -5.5).astype(np.uint8)
        pixel_count = int(flood_mask.sum())
        duration = round(time.time() - start_time, 2)

        return {
            "tile_id": tile_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "duration": duration,
            "processed_at": datetime.utcnow().isoformat(),
            "pre_shape": pre_shape,
            "post_shape": post_shape}

    except Exception as e:
        return {
            "tile_id": tile_id,
            "pixel_count": 0,
            "status": f"ERROR : {str(e)}",
            "duration": round(time.time() - start_time, 2),
            "processed_at": datetime.utcnow().isoformat(),
            "pre_shape": None,
            "post_shape": None}


def main():
    overall_start = time.time()
    print("Starting Standalone GeoTIFF processing...")
    local_pre = "/tmp/standalone_pre.tif"
    local_post = "/tmp/standalone_post.tif"
    download_start = time.time()
    download_geotiff(PRE_TIF, local_pre)
    download_geotiff(POST_TIF, local_post)
    download_time = (time.time() - download_start)
    print(f"\nInput download time: "
          f"{download_time:.2f} sec")
    print("\nReading GeoTIFF dimensions...")
    height, width, crs, transform = (get_geotiff_info(local_pre))
    print(f"Raster dimensions: "
          f"{width} x {height}")
    print(f"CRS: {crs}")
    processing_start = time.time()
    results = []
    with rasterio.open(local_pre) as pre_src, rasterio.open(local_post) as post_src:
        tasks = []
        tile_id = 0
        for y in range(0, height, TILE_SIZE):
            for x in range(0, width, TILE_SIZE):
                w = min(TILE_SIZE, width - x)
                h = min(TILE_SIZE, height - y)
                window = Window(x, y, w, h)
                tasks.append((tile_id, window))
                tile_id += 1
        print(f"\nTotal tiles: "
              f"{len(tasks)}")

        for tile_id, window in tasks:
            result = process_tile(tile_id, pre_src, post_src, window)
            results.append(result)
    processing_time = (time.time() - processing_start)
    print("\n------ Printing Results ------\n")
    total_pixels = 0
    for result in sorted(results, key=lambda x: x["tile_id"]):
        print(f"Tile {result['tile_id']:02d} | "
              f"{result['status']} | "
              f"Pixels={result['pixel_count']} | "
              f"Duration={result['duration']} sec | "
              f"PRE shape={result.get('pre_shape')} | "
              f"POST shape={result.get('post_shape')}")
        total_pixels += (result["pixel_count"])
    durations = [result["duration"] for result in results]
    print("TOTAL FLOOD PIXELS :", total_pixels)
    print(f"TOTAL TILE PROCESSING TIME : "
          f"{processing_time:.2f} sec")
    #print(f"AVERAGE TILE TIME : "          f"{np.mean(durations):.2f} sec")
    for path in [local_pre, local_post]:
        try:
            os.remove(path)
        except OSError:
            pass

    overall_duration = (time.time() - overall_start)
    print(f"Total End-to-End Time : "
          f"{overall_duration:.2f} seconds")

if __name__ == "__main__":
    main()
