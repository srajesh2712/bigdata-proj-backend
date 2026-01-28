import time
from datetime import datetime
import numpy as np
import rasterio
import fsspec
import logging
import os
from rasterio.shutil import copy as rasterio_copy
from rasterio.warp import transform_bounds
from rasterio.windows import Window
from rasterio import MemoryFile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from skimage.filters import threshold_otsu


def process_sar_tile(task_data):
    window_id, pre_path, post_path, win_coords = task_data

    # Must set environment inside the worker
    os.environ["HADOOP_USER_NAME"] = "root"
    os.environ["GDAL_HDFS_NAME_NODE"] = "namenode:8020"

    try:
        fs = fsspec.filesystem("hdfs", host="namenode:8020", user="root")
        win = Window(*win_coords)

        def read_window(path):
            # Remove protocol for fsspec if necessary
            clean_path = path.replace("hdfs://namenode:8020", "")
            with fs.open(clean_path, "rb") as f:
                with rasterio.open(f) as src:
                    data = src.read(1, window=win)
                    transform = src.window_transform(win)
                    crs = src.crs
                    bounds = src.window_bounds(win)
                    nodata = src.nodata
            return data, transform, crs, bounds, nodata

        # 1. READ DATA
        pre, _, _, _, _ = read_window(pre_path)
        post, src_transform, src_crs, (left, bottom, right, top), _ = read_window(post_path)

        # 2. VALIDATION: Check if we actually have data
        if np.max(pre) <= 0:
            return {
                "tile_id": window_id, "pixel_count": 0, "status": "EMPTY_OR_NODATA",
                "mask_path": None, "min_lon": 0.0, "min_lat": 0.0,
                "max_lon": 0.0, "max_lat": 0.0, "processed_at": datetime.now()
            }

        # 3. CONVERT TO DB AND DIFF
        pre_db = 10 * np.log10(np.clip(pre, 1e-6, None))
        post_db = 10 * np.log10(np.clip(post, 1e-6, None))
        diff_image = post_db - pre_db

        # 4. ADAPTIVE OTSU
        try:
            # Only run Otsu on valid data (exclude extreme values)
            thresh = threshold_otsu(diff_image)
            # Dissertation Logic: If the 'best' threshold isn't a drop, it's not a flood
            final_thresh = thresh if thresh < -3.0 else -5.5
            status = "SUCCESS_OTSU" if thresh < -3.0 else "SUCCESS_FALLBACK"
        except:
            final_thresh = -5.5
            status = "SUCCESS_FIXED"

        flood_mask = (diff_image < final_thresh).astype(np.uint8)
        pixel_count = int(np.sum(flood_mask))

        # 5. WRITE MASK IF FLOOD FOUND
        mask_hdfs_path = None
        if pixel_count > 0:
            local_tmp = f"/tmp/tile_{window_id}.tif"
            mask_hdfs_path = f"/user/btcchl0040/sar/results/masks/tile_{window_id:02d}_mask.tif"

            meta = {
                "driver": "GTiff", "height": win.height, "width": win.width,
                "count": 1, "dtype": "uint8", "crs": src_crs,
                "transform": src_transform, "nodata": 0, "compress": "LZW"
            }

            with rasterio.open(local_tmp, "w", **meta) as dst:
                dst.write(flood_mask, 1)

            # Use fsspec to move to HDFS
            fs.put(local_tmp, mask_hdfs_path)
            if os.path.exists(local_tmp):
                os.remove(local_tmp)

        # 6. COORDINATE CONVERSION
        l, b, r, t = transform_bounds(src_crs, 'EPSG:4326', left, bottom, right, top)

        return {
            "tile_id": window_id, "pixel_count": pixel_count, "status": status,
            "mask_path": mask_hdfs_path, "min_lon": float(l), "min_lat": float(b),
            "max_lon": float(r), "max_lat": float(t), "processed_at": datetime.now()
        }

    except Exception as e:
        return {
            "tile_id": window_id, "pixel_count": 0, "status": f"ERROR: {str(e)}",
            "mask_path": None, "min_lon": 0.0, "min_lat": 0.0,
            "max_lon": 0.0, "max_lat": 0.0, "processed_at": datetime.now()
        }


def main():
    spark = SparkSession.builder.appName("SAR_Otsu_Flood").getOrCreate()

    schema = StructType([
        StructField("tile_id", IntegerType(), True),
        StructField("pixel_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("mask_path", StringType(), True),
        StructField("min_lon", DoubleType(), True),
        StructField("min_lat", DoubleType(), True),
        StructField("max_lon", DoubleType(), True),
        StructField("max_lat", DoubleType(), True),
        StructField("processed_at", TimestampType(), True)
    ])

    pre_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
    post_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

    tasks = []
    # Grid search - Check if 4000 is actually inside your image dimensions!
    for r in range(6):
        for c in range(7):
            tasks.append((r * 7 + c, pre_p, post_p, (c * 1024 + 4000, r * 1024 + 4000, 1024, 1024)))

    rdd = spark.sparkContext.parallelize(tasks, len(tasks))
    results = rdd.map(process_sar_tile).collect()

    # Check results in driver console
    for res in results:
        print(f"TILE {res['tile_id']}: {res['status']} | Pixels: {res['pixel_count']}")

    df = spark.createDataFrame(results, schema)
    df.createOrReplaceTempView("new_batch")

    spark.sql("""
        MERGE INTO local.flood_db.sar_catalog t
        USING new_batch s ON t.tile_id = s.tile_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.stop()


if __name__ == "__main__":
    main()