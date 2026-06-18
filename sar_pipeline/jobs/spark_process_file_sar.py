import time

import numpy as np
import rasterio
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from rasterio import MemoryFile
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject
from rasterio.windows import Window
from pyspark.sql import SparkSession
import fsspec
import logging
import os
from rasterio.shutil import copy as rasterio_copy
from rasterio.warp import transform_bounds
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
    print(' entering into process sar tile ')
    logger = logging.getLogger("worker")
    window_id, pre_path, post_path, win_coords = task_data

    os.environ["HADOOP_USER_NAME"] = "root"
    os.environ["GDAL_HDFS_NAME_NODE"] = "namenode:8020"
    try:
        fs = fsspec.filesystem("hdfs", host="namenode", port=8020)
        win = Window(*win_coords)

        def read_window(path):
            clean_path = path.replace("hdfs://namenode:8020", "")
            gdal_opts = {
                "GDAL_HDFS_NAME_NODE": "namenode:8020",
                "HADOOP_USER_NAME": "root",
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
        print(f"Tile {window_id}: Pre-Max={np.max(pre)}, Post-Max={np.max(post)}")
        logger.info(f"Tile {window_id}: Pre-Max={np.max(pre)}, Post-Max={np.max(post)}")
        # IMPORTANT: Use the same dictionary structure for EMPTY_LAND
        if np.max(pre) <= 0:
            return {
                "tile_id": window_id, "pixel_count": 0, "status": "EMPTY_LAND",
                "mask_path": None, "min_lon": 0.0, "min_lat": 0.0,
                "max_lon": 0.0, "max_lat": 0.0, "processed_at": time.strftime('%Y-%m-%d %H:%M:%S')
            }

        # --- LOG RATIO FLOOD LOGIC ---
        pre_db = 10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32))
        post_db = 10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32))

        flood_mask = (post_db - pre_db < -5.5).astype(np.uint8)

        pixel_count = int(flood_mask.sum())
        logger.info(f"Tile {window_id}: Found {pixel_count} flood pixels")
        print(f"Tile {window_id}: Found {pixel_count} flood pixels")
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

        # 1. Create a local temporary path on the worker node
        local_tmp = f"/tmp/tile_{window_id:02d}.tif"
        mask_path = f"hdfs://namenode:8020/user/btcchl0040/sar/results/masks/tile_{window_id:02d}_mask.tif"
        gdal_config = {
            "GDAL_HDFS_NAME_NODE": "namenode:8020",
            "HADOOP_USER_NAME": "root"
        }
        with MemoryFile() as memfile:
            with memfile.open(**tile_meta) as dst:
                dst.write(flood_mask, 1)
                dst.build_overviews([2, 4, 8, 16], Resampling.nearest)
                dst.update_tags(ns="rio_overview", resampling="nearest")

            with memfile.open() as src:
                with rasterio.Env(**gdal_config):
                    rasterio_copy(
                        src,
                        local_tmp,
                        driver="GTiff",
                        tiled=True,
                        blockxsize=256,
                        blockysize=256,
                        compress="LZW",
                        copy_src_overviews=True,
                    )
            # 3. Use your existing 'fs' (fsspec) to move it to HDFS
            # This bypasses the GDAL HDFS driver entirely
            fs.put(local_tmp, mask_path)
        # Get the bounding box in the source CRS (meters)
        left, bottom, right, top = src.window_bounds(win)

        # Convert to Lat/Long (EPSG:4326) for easier querying
        left_lon, bottom_lat, right_lon, top_lat = transform_bounds(src_crs, 'EPSG:4326', left, bottom, right, top)

        # Return a dictionary instead of a tuple for easier Spark DataFrame creation
        return {
            "tile_id": window_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "mask_path": mask_path,
            "min_lon": float(left_lon),
            "min_lat": float(bottom_lat),
            "max_lon": float(right_lon),
            "max_lat": float(top_lat),
            "processed_at": time.strftime('%Y-%m-%d %H:%M:%S')
        }

    except Exception as e:
        return {
            "tile_id": window_id,
            "pixel_count": 0,
            "status": f"ERROR: {str(e)}",
            "mask_path": None,
            "min_lon": 0.0, "min_lat": 0.0, "max_lon": 0.0, "max_lat": 0.0,
            "processed_at": time.strftime('%Y-%m-%d %H:%M:%S')
        }


def main():
    schema = StructType([
        StructField("tile_id", IntegerType(), True),
        StructField("pixel_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("mask_path", StringType(), True),
        StructField("min_lon", DoubleType(), True),
        StructField("min_lat", DoubleType(), True),
        StructField("max_lon", DoubleType(), True),
        StructField("max_lat", DoubleType(), True),
        StructField("processed_at", StringType(), True)
    ])

    spark = (
        SparkSession.builder.appName("SAR_Flood_Tiling_COG").getOrCreate()
    )

    fs = fsspec.filesystem("hdfs", host="namenode", port=8020)
    mask_dir = "/user/btcchl0040/sar/results/masks"
    if not fs.exists(mask_dir):
        fs.makedirs(mask_dir)
        print(f"DRIVER: Created missing directory {mask_dir}")
    else:
        print(f"DRIVER: Directory {mask_dir} already exists.")


    print(f"Catalog 'local' warehouse: {spark.conf.get('spark.sql.catalog.local.warehouse', 'NOT FOUND')}")
    # Create the table if it doesn't exist
    spark.sql("""
              CREATE TABLE IF NOT EXISTS local.flood_db.sar_catalog
              (
                  tile_id
                  INT,
                  pixel_count
                  INT,
                  status
                  STRING,
                  mask_path
                  STRING,
                  min_lon
                  DOUBLE,
                  min_lat
                  DOUBLE,
                  max_lon
                  DOUBLE,
                  max_lat
                  DOUBLE,
                  processed_at
                  TIMESTAMP
              )
                  USING iceberg
                  PARTITIONED BY
              (
                  days
              (
                  processed_at
              ))
              """)

    pre_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
    post_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

    verify_cog("/user/btcchl0040/sar/results/masks/tile_02_mask.tif")
    #time.sleep(1000)
    tasks = []
    offset_x = 0
    offset_y = 0
    for r in range(6):
        for c in range(7):
            tasks.append(
                (r * 7 + c, pre_p, post_p, (c * 1024 + offset_x, r * 1024 + offset_y, 1024, 1024))
            )

    rdd = spark.sparkContext.parallelize(tasks, len(tasks))
    results = rdd.map(process_sar_tile).collect()
    results_df = spark.createDataFrame(results, schema=schema)
    from pyspark.sql.functions import col, to_timestamp
    results_df = results_df.withColumn("processed_at", to_timestamp(col("processed_at")))

    results_df.createOrReplaceTempView("new_batch")
    # Use MERGE INTO to update the Iceberg table
    spark.sql("""
        MERGE INTO local.flood_db.sar_catalog t
        USING new_batch s
        ON t.tile_id = s.tile_id
        WHEN MATCHED THEN 
            UPDATE SET *
        WHEN NOT MATCHED THEN 
            INSERT *
    """)

    print("Iceberg Catalog Updated Successfully.")
    total = 0
    print("\n--- FLOOD RESULTS ---")
    # Tell sorted() to use the 'tile_id' key for sorting
    for res in sorted(results, key=lambda x: x['tile_id']):
        wid = res['tile_id']
        count = res['pixel_count']
        status = res['status']
        print(f"Tile {wid:02d}: {status} | Pixels: {count}")
        total += count

    print(f"TOTAL FLOOD PIXELS: {total}\n")

    spark.stop()



if __name__ == "__main__":
    main()
