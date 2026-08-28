import time
import numpy as np
import zarr
import fsspec
import logging

from datetime import datetime


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

    return data.shape


# ============================================================
# TILE PROCESSOR
# ============================================================

def process_tile(
    tile_id,
    pre_data,
    post_data,
    x,
    y,
    w,
    h
):

    start_time = time.time()

    try:

        # ----------------------------------------------------
        # READ TILE
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

        pre_shape = pre.shape
        post_shape = post.shape

        # ----------------------------------------------------
        # EMPTY TILE
        # ----------------------------------------------------

        if pre.size == 0:

            return {
                "tile_id": tile_id,
                "pixel_count": 0,
                "status": "EMPTY_TILE",
                "duration": round(
                    time.time() - start_time,
                    2
                ),
                "pre_shape": str(pre_shape),
                "post_shape": str(post_shape),
                "processed_at":
                    datetime.utcnow().isoformat()
            }

        # ----------------------------------------------------
        # EMPTY LAND
        # ----------------------------------------------------

        if np.max(pre) <= 0:

            return {
                "tile_id": tile_id,
                "pixel_count": 0,
                "status": "EMPTY_LAND",
                "duration": round(
                    time.time() - start_time,
                    2
                ),
                "pre_shape": str(pre_shape),
                "post_shape": str(post_shape),
                "processed_at":
                    datetime.utcnow().isoformat()
            }

        # ----------------------------------------------------
        # FLOOD DETECTION
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        return {
            "tile_id": tile_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "duration": round(
                time.time() - start_time,
                2
            ),
            "pre_shape": str(pre_shape),
            "post_shape": str(post_shape),
            "processed_at":
                datetime.utcnow().isoformat()
        }

    except Exception as e:

        return {
            "tile_id": tile_id,
            "pixel_count": 0,
            "status": f"ERROR: {str(e)}",
            "duration": round(
                time.time() - start_time,
                2
            ),
            "pre_shape": None,
            "post_shape": None,
            "processed_at":
                datetime.utcnow().isoformat()
        }


# ============================================================
# MAIN STANDALONE PROCESSING
# ============================================================

def main():

    overall_start = time.time()

    print(
        "Starting Standalone Zarr processing..."
    )

    # --------------------------------------------------------
    # OPEN ZARR STORES ONCE
    # --------------------------------------------------------

    print(
        "\nOpening pre-event Zarr..."
    )

    pre_store = open_zarr_hdfs(
        PRE_ZARR
    )

    print(
        "Opening post-event Zarr..."
    )

    post_store = open_zarr_hdfs(
        POST_ZARR
    )

    pre_data = pre_store["band_data"]

    post_data = post_store["band_data"]

    # --------------------------------------------------------
    # DATASET DIMENSIONS
    # --------------------------------------------------------

    pre_shape = pre_data.shape
    post_shape = post_data.shape

    print(
        f"\nPre-event Zarr shape : {pre_shape}"
    )

    print(
        f"Post-event Zarr shape: {post_shape}"
    )

    if len(pre_shape) != 3:

        raise ValueError(
            "Expected Zarr shape "
            "(bands, height, width)"
        )

    bands = pre_shape[0]
    height = pre_shape[1]
    width = pre_shape[2]

    print(
        f"Bands: {bands}"
    )

    print(
        f"Raster dimensions: "
        f"{width} x {height}"
    )

    # --------------------------------------------------------
    # CREATE TILE GRID
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
                    x,
                    y,
                    w,
                    h
                )
            )

            tile_id += 1

    print(
        f"\nTotal tiles: {len(tasks)}"
    )

    # --------------------------------------------------------
    # SEQUENTIAL TILE PROCESSING
    # --------------------------------------------------------

    processing_start = time.time()

    results = []

    for task in tasks:

        tile_id, x, y, w, h = task

        print(
            f"\nProcessing tile {tile_id:02d}..."
        )

        result = process_tile(
            tile_id,
            pre_data,
            post_data,
            x,
            y,
            w,
            h
        )

        results.append(result)

        print(
            f"Tile {tile_id:02d} | "
            f"{result['status']} | "
            f"Pixels={result['pixel_count']} | "
            f"Duration={result['duration']} sec"
        )

    processing_time = (
        time.time() - processing_start
    )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print(
        "\n"
        "------ FLOOD RESULTS ------"
    )

    total_pixels = 0

    for result in sorted(
        results,
        key=lambda x: x["tile_id"]
    ):

        print(
            f"Tile {result['tile_id']:02d} | "
            f"{result['status']} | "
            f"Pixels={result['pixel_count']} | "
            f"Duration={result['duration']} sec | "
            f"PRE shape={result.get('pre_shape')} | "
            f"POST shape={result.get('post_shape')}"
        )

        total_pixels += (
            result["pixel_count"]
        )

    # --------------------------------------------------------
    # TILE STATISTICS
    # --------------------------------------------------------

    durations = [
        r["duration"]
        for r in results
        if r["status"] == "SUCCESS"
    ]

    print(
        "\n--------------------------------"
    )

    print(
        f"TOTAL FLOOD PIXELS : "
        f"{total_pixels}"
    )

    print(
        f"TOTAL PROCESSING TIME : "
        f"{processing_time:.2f} sec"
    )

    if durations:

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