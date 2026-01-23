import time

import numpy as np
import rasterio
from rasterio import MemoryFile
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject
from rasterio.windows import Window
from pyspark.sql import SparkSession
import fsspec
import logging
import os
from rasterio.shutil import copy as rasterio_copy

logging.basicConfig(level=logging.INFO)

def verify_cog(path):
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020)
    with fs.open(path, "rb") as f:
        with rasterio.open(f) as src:
            print("\n--- COG VERIFICATION ---")
            print(f"Driver      : {src.driver}")
            print(f"Tiled       : {src.is_tiled}")
            print(f"Block size  : {src.block_shapes}")
            print(f"Overviews   : {src.overviews(1)}")
            print(f"CRS         : {src.crs}")
            print(f"Bounds      : {src.bounds}")

            if not src.is_tiled:
                return False, "NOT TILED"

            if not src.overviews(1):
                return False, "NO INTERNAL OVERVIEWS"

            if src.driver not in ("COG", "GTiff"):
                return False, "WRONG DRIVER"

    return True, "COG STRUCTURE OK"


def process_sar_tile(task_data):
    window_id, pre_path, post_path, win_coords = task_data

    os.environ["HADOOP_USER_NAME"] = "root"

    try:
        fs = fsspec.filesystem("hdfs", host="namenode", port=8020)
        win = Window(*win_coords)

        def read_window(path):
            clean_path = path.replace("hdfs://namenode:8020", "")
            gdal_opts = {
                "GDAL_CACHEMAX": 256,
                "GDAL_DISABLE_READDIR_ON_OPEN": "TRUE",
                "VSI_CACHE": "FALSE",
            }

            with fs.open(clean_path, "rb") as f:
                with rasterio.Env(**gdal_opts):
                    with rasterio.open(f, sharing=False) as src:
                        data = src.read(1, window=win)
                        transform = src.window_transform(win)
                        crs = src.crs
                        bounds = src.window_bounds(win)
            return data, transform, crs, bounds

        # --- READ SOURCE WINDOWS ---
        pre, _, _, _ = read_window(pre_path)
        post, src_transform, src_crs, (left, bottom, right, top) = read_window(post_path)

        if np.max(pre) <= 0:
            return (window_id, 0, "EMPTY_LAND")

        # --- LOG RATIO FLOOD LOGIC ---
        pre_db = 10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32))
        post_db = 10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32))

        flood_mask = (post_db - pre_db < -5.5).astype(np.uint8)

        pixel_count = int(flood_mask.sum())

        tile_meta = {
            "driver": "GTiff",
            "height": win.height,
            "width": win.width,
            "count": 1,
            "dtype": "uint8",
            "crs": src_crs,  # ← NO reprojection
            "transform": src_transform,
            "nodata": 0,
            "compress": "LZW",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
        }

        mask_path = f"/user/btcchl0040/sar/results/masks/tile_{window_id:02d}_mask.tif"

        with MemoryFile() as memfile:
            with memfile.open(**tile_meta) as dst:
                dst.write(flood_mask, 1)
                dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
                dst.update_tags(ns="rio_overview", resampling="nearest")

            with memfile.open() as src:
                rasterio_copy(
                    src,
                    mask_path,
                    driver="GTiff",
                    tiled=True,
                    blockxsize=256,
                    blockysize=256,
                    compress="LZW",
                    copy_src_overviews=True,
                )
        return (window_id, pixel_count, "SUCCESS")

    except Exception as e:
        return (window_id, 0, f"ERROR: {str(e)}")


def main():
    spark = (
        SparkSession.builder.appName("SAR_Flood_Tiling_COG").getOrCreate()
    )

    pre_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
    post_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

    verify_cog("/user/btcchl0040/sar/results/masks/tile_02_mask.tif")
    time.sleep(1000)
    tasks = []
    for r in range(1):
        for c in range(7):
            tasks.append(
                (r * 7 + c, pre_p, post_p, (c * 1024, r * 1024, 1024, 1024))
            )

    rdd = spark.sparkContext.parallelize(tasks, len(tasks))
    results = rdd.map(process_sar_tile).collect()

    total = 0
    print("\n--- FLOOD RESULTS ---")
    for wid, count, status in sorted(results):
        print(f"Tile {wid:02d}: {status} | Pixels: {count}")
        total += count

    print(f"TOTAL FLOOD PIXELS: {total}\n")

    spark.stop()



if __name__ == "__main__":
    main()
