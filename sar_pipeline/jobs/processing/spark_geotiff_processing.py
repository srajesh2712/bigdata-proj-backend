import time
import os
import socket
import logging

import numpy as np
import rasterio
import fsspec

from rasterio.windows import Window
from rasterio.warp import transform_bounds

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

from pyspark import TaskContext


# ============================================================
# CONFIGURATION
# ============================================================

HDFS_HOST = "namenode"
HDFS_PORT = 8020

TILE_SIZE = 1024

JOB_IDS = [37, 38]

PRE_PATH = (
    f"/user/btcchl0040/"
    f"dask_preprocessed/"
    f"{JOB_IDS[0]}/"
    f"{JOB_IDS[0]}_tile.tif"
)

POST_PATH = (
    f"/user/btcchl0040/"
    f"dask_preprocessed/"
    f"{JOB_IDS[1]}/"
    f"{JOB_IDS[1]}_tile.tif"
)


logging.basicConfig(
    level=logging.INFO
)


# ============================================================
# GET GEOTIFF DIMENSIONS
# ============================================================

def get_geotiff_shape(path):

    fs = fsspec.filesystem(
        "hdfs",
        host=HDFS_HOST,
        port=HDFS_PORT
    )

    local_path = "/tmp/spark_geotiff_info.tif"

    try:

        fs.get(
            path,
            local_path
        )

        with rasterio.open(local_path) as src:

            width = src.width
            height = src.height
            crs = src.crs
            transform = src.transform

        return (
            height,
            width,
            crs,
            transform
        )

    finally:

        try:

            if os.path.exists(local_path):
                os.remove(local_path)

        except OSError:
            pass


# ============================================================
# TILE PROCESSOR
# ============================================================

def process_sar_tile(task_data):

    tile_id, pre_path, post_path, win_coords = task_data

    start_time = time.time()

    logger = logging.getLogger("spark_worker")

    try:

        # ----------------------------------------------------
        # SPARK TASK INFORMATION
        # ----------------------------------------------------

        task_context = TaskContext.get()

        partition_id = (
            task_context.partitionId()
            if task_context is not None
            else -1
        )

        executor_host = socket.gethostname()

        # ----------------------------------------------------
        # HDFS
        # ----------------------------------------------------

        fs = fsspec.filesystem(
            "hdfs",
            host=HDFS_HOST,
            port=HDFS_PORT
        )

        # ----------------------------------------------------
        # TILE WINDOW
        # ----------------------------------------------------

        x, y, w, h = win_coords

        window = Window(
            x,
            y,
            w,
            h
        )

        # ----------------------------------------------------
        # READ PRE-EVENT WINDOW
        # ----------------------------------------------------

        def read_window(path):

            with fs.open(
                path,
                "rb"
            ) as f:

                with rasterio.open(
                    f,
                    sharing=False
                ) as src:

                    data = src.read(
                        1,
                        window=window
                    )

                    transform = (
                        src.window_transform(window)
                    )

                    crs = src.crs

                    bounds = (
                        src.window_bounds(window)
                    )

                    raster_shape = src.shape

            return (
                data,
                transform,
                crs,
                bounds,
                raster_shape
            )

        # ----------------------------------------------------
        # READ SOURCE DATA
        # ----------------------------------------------------

        pre, _, _, _, pre_shape = (
            read_window(pre_path)
        )

        (
            post,
            src_transform,
            src_crs,
            bounds,
            post_shape
        ) = read_window(post_path)

        logger.info(
            f"Tile {tile_id}: "
            f"Pre-Max={np.max(pre)}, "
            f"Post-Max={np.max(post)}"
        )

        # ====================================================
        # EMPTY TILE
        # ====================================================

        if pre.size == 0:

            duration = round(
                time.time() - start_time,
                2
            )

            return {

                "tile_id": tile_id,

                "pixel_count": 0,

                "status": "EMPTY_TILE",

                "duration": duration,

                "partition_id": partition_id,

                "executor": executor_host,

                "pre_shape": str(pre_shape),

                "post_shape": str(post_shape),

                "processed_at":
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
            }

        # ====================================================
        # EMPTY LAND
        # ====================================================

        if np.max(pre) <= 0:

            duration = round(
                time.time() - start_time,
                2
            )

            return {

                "tile_id": tile_id,

                "pixel_count": 0,

                "status": "EMPTY_LAND",

                "duration": duration,

                "partition_id": partition_id,

                "executor": executor_host,

                "pre_shape": str(pre_shape),

                "post_shape": str(post_shape),

                "processed_at":
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
            }

        # ====================================================
        # FLOOD DETECTION
        # ====================================================

        pre_db = (
            10
            * np.log10(
                np.clip(
                    pre,
                    1e-6,
                    None
                ).astype(np.float32)
            )
        )

        post_db = (
            10
            * np.log10(
                np.clip(
                    post,
                    1e-6,
                    None
                ).astype(np.float32)
            )
        )

        diff = post_db - pre_db

        flood_mask = (
            diff < -5.5
        ).astype(np.uint8)

        pixel_count = int(
            flood_mask.sum()
        )

        logger.info(
            f"Tile {tile_id}: "
            f"Found {pixel_count} flood pixels"
        )

        # ====================================================
        # COORDINATES
        # ====================================================

        left, bottom, right, top = bounds

        (
            left_lon,
            bottom_lat,
            right_lon,
            top_lat
        ) = transform_bounds(
            src_crs,
            "EPSG:4326",
            left,
            bottom,
            right,
            top
        )

        # ====================================================
        # RESULT
        # ====================================================

        duration = round(
            time.time() - start_time,
            2
        )

        return {

            "tile_id": tile_id,

            "pixel_count": pixel_count,

            "status": "SUCCESS",

            "duration": duration,

            "partition_id": partition_id,

            "executor": executor_host,

            "pre_shape": str(pre_shape),

            "post_shape": str(post_shape),

            "min_lon": float(left_lon),

            "min_lat": float(bottom_lat),

            "max_lon": float(right_lon),

            "max_lat": float(top_lat),

            "processed_at":
                time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
        }

    # ========================================================
    # ERROR HANDLING
    # ========================================================

    except Exception as e:

        duration = round(
            time.time() - start_time,
            2
        )

        return {

            "tile_id": tile_id,

            "pixel_count": 0,

            "status":
                f"ERROR : {str(e)}",

            "duration": duration,

            "partition_id": -1,

            "executor": socket.gethostname(),

            "pre_shape": None,

            "post_shape": None,

            "min_lon": 0.0,

            "min_lat": 0.0,

            "max_lon": 0.0,

            "max_lat": 0.0,

            "processed_at":
                time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
        }


# ============================================================
# MAIN
# ============================================================

def main():

    overall_start = time.time()

    print(
        "Starting Spark GeoTIFF distributed processing..."
    )

    # --------------------------------------------------------
    # SPARK SESSION
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("SAR_Flood_Spark_GeoTIFF")
        .getOrCreate()
    )

    print(
        "\nSpark Session:"
    )

    print(
        spark.sparkContext
    )

    # --------------------------------------------------------
    # GET GEOTIFF DIMENSIONS
    # --------------------------------------------------------

    print(
        "\nReading GeoTIFF dimensions..."
    )

    (
        height,
        width,
        crs,
        transform
    ) = get_geotiff_shape(
        PRE_PATH
    )

    print(
        f"Raster dimensions: "
        f"{width} x {height}"
    )

    print(
        f"CRS: {crs}"
    )

    # --------------------------------------------------------
    # CREATE TILES
    # --------------------------------------------------------

    tasks = []

    tile_id = 0

    for y in range(
        0,
        height,
        TILE_SIZE
    ):

        for x in range(
            0,
            width,
            TILE_SIZE
        ):

            w = min(
                TILE_SIZE,
                width - x
            )

            h = min(
                TILE_SIZE,
                height - y
            )

            tasks.append(
                (
                    tile_id,
                    PRE_PATH,
                    POST_PATH,
                    (
                        x,
                        y,
                        w,
                        h
                    )
                )
            )

            tile_id += 1

    print(
        f"\nTotal tiles: {len(tasks)}"
    )

    # --------------------------------------------------------
    # SPARK PARALLELIZATION
    # --------------------------------------------------------

    rdd = spark.sparkContext.parallelize(
        tasks,
        len(tasks)
    )

    # --------------------------------------------------------
    # DISTRIBUTED PROCESSING
    # --------------------------------------------------------

    pipeline_start = time.time()

    results = (
        rdd
        .map(process_sar_tile)
        .collect()
    )

    pipeline_time = (
        time.time() - pipeline_start
    )

    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    schema = StructType([

        StructField(
            "tile_id",
            IntegerType(),
            True
        ),

        StructField(
            "pixel_count",
            IntegerType(),
            True
        ),

        StructField(
            "status",
            StringType(),
            True
        ),

        StructField(
            "duration",
            DoubleType(),
            True
        ),

        StructField(
            "partition_id",
            IntegerType(),
            True
        ),

        StructField(
            "executor",
            StringType(),
            True
        ),

        StructField(
            "pre_shape",
            StringType(),
            True
        ),

        StructField(
            "post_shape",
            StringType(),
            True
        ),

        StructField(
            "min_lon",
            DoubleType(),
            True
        ),

        StructField(
            "min_lat",
            DoubleType(),
            True
        ),

        StructField(
            "max_lon",
            DoubleType(),
            True
        ),

        StructField(
            "max_lat",
            DoubleType(),
            True
        ),

        StructField(
            "processed_at",
            StringType(),
            True
        )
    ])

    results_df = spark.createDataFrame(
        results,
        schema=schema
    )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print(
        "\n------ FLOOD RESULTS ------\n"
    )

    total_pixels = 0

    for res in sorted(
        results,
        key=lambda x: x["tile_id"]
    ):

        print(
            f"Tile {res['tile_id']:02d} | "
            f"{res['status']} | "
            f"Pixels={res['pixel_count']} | "
            f"Duration={res['duration']} sec | "
            f"PRE shape={res.get('pre_shape')} | "
            f"POST shape={res.get('post_shape')} | "
            f"Partition={res.get('partition_id')} | "
            f"Executor={res.get('executor')}"
        )

        total_pixels += (
            res["pixel_count"]
        )

    # --------------------------------------------------------
    # STATISTICS
    # --------------------------------------------------------

    durations = [
        r["duration"]
        for r in results
    ]

    print(
        "\n--------------------------------"
    )

    print(
        "TOTAL FLOOD PIXELS :",
        total_pixels
    )

    print(
        f"TOTAL PIPELINE TIME : "
        f"{pipeline_time:.2f} sec"
    )

    print(
        f"AVERAGE TILE TIME : "
        f"{np.mean(durations):.2f} sec"
    )

    print(
        f"MAXIMUM TILE TIME : "
        f"{np.max(durations):.2f} sec"
    )

    print(
        f"MINIMUM TILE TIME : "
        f"{np.min(durations):.2f} sec"
    )

    print(
        "--------------------------------"
    )

    # --------------------------------------------------------
    # SPARK DATAFRAME
    # --------------------------------------------------------

    print(
        "\nSpark Result DataFrame:"
    )

    results_df.show(
        truncate=False
    )

    # --------------------------------------------------------
    # CLOSE SPARK
    # --------------------------------------------------------

    spark.stop()

    # --------------------------------------------------------
    # END-TO-END TIME
    # --------------------------------------------------------

    overall_duration = (
        time.time() - overall_start
    )

    print(
        "\n==================================="
    )

    print(
        f"Total End-to-End Time : "
        f"{overall_duration:.2f} seconds"
    )

    print(
        "===================================\n"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()

