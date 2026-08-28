import time
import numpy as np
import zarr
import fsspec
import logging
import socket

from datetime import datetime

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

PRE_ZARR = (
    f"/user/btcchl0040/"
    f"dask_preprocessed/"
    f"{JOB_IDS[0]}/"
    f"{JOB_IDS[0]}_tile.zarr"
)

POST_ZARR = (
    f"/user/btcchl0040/"
    f"dask_preprocessed/"
    f"{JOB_IDS[1]}/"
    f"{JOB_IDS[1]}_tile.zarr"
)


logging.basicConfig(
    level=logging.INFO
)


# ============================================================
# HDFS ZARR READER
# ============================================================

def open_zarr_hdfs(path):
    """
    Opens a Zarr store directly from HDFS using fsspec.
    """

    fs = fsspec.filesystem(
        "hdfs",
        host=HDFS_HOST,
        port=HDFS_PORT
    )

    mapper = fs.get_mapper(path)

    try:

        files = fs.ls(path)

        metadata_exists = any(
            str(item).endswith("_metadata")
            for item in files
        )

    except Exception:

        metadata_exists = False

    if metadata_exists:

        return zarr.open_consolidated(
            mapper,
            mode="r"
        )

    return zarr.open(
        mapper,
        mode="r"
    )


# ============================================================
# GET ZARR DIMENSIONS
# ============================================================

def get_zarr_shape(path):

    store = open_zarr_hdfs(path)

    data = store["band_data"]

    shape = data.shape

    return shape


# ============================================================
# TILE PROCESSOR
# ============================================================

def process_tile(task):

    tile_id, pre_path, post_path, win = task

    start_time = time.time()

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
        # TILE COORDINATES
        # ----------------------------------------------------

        x, y, w, h = win

        # ----------------------------------------------------
        # OPEN ZARR STORES
        # ----------------------------------------------------

        pre_store = open_zarr_hdfs(
            pre_path
        )

        post_store = open_zarr_hdfs(
            post_path
        )

        pre_data = pre_store["band_data"]

        post_data = post_store["band_data"]

        # ----------------------------------------------------
        # ORIGINAL ZARR DATASET SHAPE
        # ----------------------------------------------------

        pre_shape = pre_data.shape

        post_shape = post_data.shape

        # ----------------------------------------------------
        # READ REQUIRED TILE
        # ----------------------------------------------------

        pre = pre_data[
            0,
            y:y + h,
            x:x + w
        ]

        post = post_data[
            0,
            y:y + h,
            x:x + w
        ]

        # ----------------------------------------------------
        # SELECTED BAND SHAPE
        # ----------------------------------------------------

        selected_pre_shape = pre.shape

        selected_post_shape = post.shape

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

                "pre_shape": str(
                    selected_pre_shape
                ),

                "post_shape": str(
                    selected_post_shape
                ),

                "processed_at":
                    datetime.utcnow().isoformat()
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

                "pre_shape": str(
                    selected_pre_shape
                ),

                "post_shape": str(
                    selected_post_shape
                ),

                "processed_at":
                    datetime.utcnow().isoformat()
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
            np.sum(flood_mask)
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

            "pre_shape": str(
                selected_pre_shape
            ),

            "post_shape": str(
                selected_post_shape
            ),

            "processed_at":
                datetime.utcnow().isoformat()
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

            "processed_at":
                datetime.utcnow().isoformat()
        }


# ============================================================
# MAIN SPARK DRIVER
# ============================================================

def main():

    overall_start = time.time()

    print(
        "Starting Spark Zarr distributed processing..."
    )

    # --------------------------------------------------------
    # SPARK SESSION
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("SAR_Flood_Spark_Zarr")
        .getOrCreate()
    )

    print(
        "\nSpark Session:"
    )

    print(
        spark.sparkContext
    )

    # --------------------------------------------------------
    # READ ZARR DIMENSIONS
    # --------------------------------------------------------

    print(
        "\nReading Zarr dimensions..."
    )

    shape = get_zarr_shape(
        PRE_ZARR
    )

    print(
        f"Zarr shape: {shape}"
    )

    # --------------------------------------------------------
    # ZARR SHAPE
    #
    # Expected:
    # (bands, height, width)
    # --------------------------------------------------------

    if len(shape) != 3:

        raise ValueError(
            f"Expected Zarr shape "
            f"(bands, height, width), "
            f"but received {shape}"
        )

    bands = shape[0]

    height = shape[1]

    width = shape[2]

    print(
        f"Bands: {bands}"
    )

    print(
        f"Raster dimensions: "
        f"{width} x {height}"
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
                    PRE_ZARR,
                    POST_ZARR,
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
    # SPARK RDD
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
        .map(process_tile)
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
            "processed_at",
            StringType(),
            True
        )
    ])

    # --------------------------------------------------------
    # DATAFRAME
    # --------------------------------------------------------

    results_df = spark.createDataFrame(
        results,
        schema=schema
    )

    results_df.createOrReplaceTempView(
        "flood_results"
    )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print(
        "\n------ FLOOD RESULTS ------\n"
    )

    total_pixels = 0

    for r in sorted(
        results,
        key=lambda x: x["tile_id"]
    ):

        print(
            f"Tile {r['tile_id']:02d} | "
            f"{r['status']} | "
            f"Pixels={r['pixel_count']} | "
            f"Duration={r['duration']} sec | "
            f"PRE shape={r.get('pre_shape')} | "
            f"POST shape={r.get('post_shape')} | "
            f"Partition={r.get('partition_id')} | "
            f"Executor={r.get('executor')}"
        )

        total_pixels += (
            r["pixel_count"]
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
    # SHOW DATAFRAME
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
