import time
import numpy as np
import zarr
import fsspec
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from datetime import datetime


logging.basicConfig(level=logging.INFO)


# -----------------------------
# HDFS ZARR READER
# -----------------------------
def open_zarr_hdfs(path):
    """
    Opens Zarr store directly from HDFS using fsspec
    """
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

    mapper = fs.get_mapper(path)
    store = zarr.open_consolidated(mapper, mode="r") if "_metadata" in fs.ls(path) else zarr.open(mapper, mode="r")

    return store


def get_band(store, band="vv"):
    if band not in store:
        raise KeyError(f"Band {band} not found. Available: {list(store.keys())}")
    return store[band]


# -----------------------------
# TILE PROCESSOR (SPARK WORKER)
# -----------------------------
def process_tile(task):
    tile_id, pre_path, post_path, win = task
    start = time.time()

    try:
        x, y, w, h = win

        fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

        # open Zarr
        pre_store = open_zarr_hdfs(pre_path)
        post_store = open_zarr_hdfs(post_path)

        pre = pre_store["band_data"][0, y:y + h, x:x + w]
        post = post_store["band_data"][0, y:y + h, x:x + w]

        # empty tile check
        if np.max(pre) <= 0:
            return {
                "tile_id": tile_id,
                "pixel_count": 0,
                "status": "EMPTY_LAND",
                "duration": round(time.time() - start, 2),
                "processed_at": datetime.utcnow().isoformat()
            }

        # -----------------------------
        # FLOOD LOGIC (same as yours)
        # -----------------------------
        pre_db = 10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32))
        post_db = 10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32))

        diff = post_db - pre_db

        flood_mask = (diff < -5.5).astype(np.uint8)

        pixel_count = int(np.sum(flood_mask))

        return {
            "tile_id": tile_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "duration": round(time.time() - start, 2),
            "processed_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        return {
            "tile_id": tile_id,
            "pixel_count": 0,
            "status": f"ERROR: {str(e)}",
            "duration": round(time.time() - start, 2),
            "processed_at": datetime.utcnow().isoformat()
        }


# -----------------------------
# MAIN SPARK DRIVER
# -----------------------------
def main():

    spark = SparkSession.builder.appName("ZARR_SAR_Flood_Spark").getOrCreate()

    schema = StructType([
        StructField("tile_id", IntegerType(), True),
        StructField("pixel_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("processed_at", StringType(), True),
    ])

    # -----------------------------
    # HDFS ZARR PATHS
    # -----------------------------
    pre_zarr = "hdfs://namenode:8020/user/btcchl0040/spark_preprocessed/7/43_tile.zarr"
    post_zarr = "hdfs://namenode:8020/user/btcchl0040/spark_preprocessed/8/40_tile.zarr"

    # -----------------------------
    # TILE GRID (same logic as yours)
    # -----------------------------
    TILE = 1024
    tasks = []

    for r in range(6):
        for c in range(7):
            tile_id = r * 7 + c
            win = (c * TILE, r * TILE, TILE, TILE)
            tasks.append((tile_id, pre_zarr, post_zarr, win))

    rdd = spark.sparkContext.parallelize(tasks, len(tasks))

    results = rdd.map(process_tile).collect()

    df = spark.createDataFrame(results, schema=schema)
    df.createOrReplaceTempView("flood_results")

    # -----------------------------
    # SUMMARY
    # -----------------------------
    total = sum(r["pixel_count"] for r in results)

    print("\n--- FLOOD RESULTS ---")
    for r in sorted(results, key=lambda x: x["tile_id"]):
        print(f"Tile {r['tile_id']:02d}: {r['status']} | Pixels: {r['pixel_count']}")

    print(f"\nTOTAL FLOOD PIXELS: {total}")

    spark.stop()


if __name__ == "__main__":
    overall_start = time.time()
    main()
    overall_duration = time.time() - overall_start

    print("\n===================================")
    print(f"Total end-to-end processing time : {overall_duration:.2f} seconds")
    print("===================================\n")
