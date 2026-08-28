import time
from datetime import datetime

import fsspec
import numpy as np
import rasterio
from dask.distributed import Client, get_worker


def get_geotiff_shape(path):
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

    local_path = "/tmp/geotiff_info.tif"

    try:
        fs.get(path, local_path)

        with rasterio.open(local_path) as src:
            width = src.width
            height = src.height
            crs = src.crs
            transform = src.transform

        return height, width, crs, transform

    finally:

        try:
            import os
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass


def process_tile(task):
    tile_id, pre_path, post_path, win = task

    start = time.time()

    try:

        worker = get_worker()

        worker_id = worker.address
        worker_name = worker.name

        # TILE COORDINATES

        x, y, w, h = win

        window = rasterio.windows.Window(x, y, w, h)

        # HDFS

        fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

        # LOCAL TEMP FILES

        local_pre = f"/tmp/dask_pre_{tile_id}.tif"
        local_post = f"/tmp/dask_post_{tile_id}.tif"

        # DOWNLOAD PRE-EVENT

        fs.get(pre_path, local_pre)

        # DOWNLOAD POST-EVENT

        fs.get(post_path, local_post)

        # READ ONLY THE REQUIRED WINDOW

        with rasterio.open(local_pre) as src_pre:

            pre = src_pre.read(1, window=window)

            pre_shape = src_pre.shape

        with rasterio.open(local_post) as src_post:

            post = src_post.read(1, window=window)

            post_shape = src_post.shape

        # CLEAN DOWNLOADED FILES

        import os

        try:
            os.remove(local_pre)
            os.remove(local_post)
        except OSError:
            pass

        # EMPTY TILE CHECK

        if pre.size == 0:
            return {"tile_id": tile_id, "pixel_count": 0, "status": "EMPTY_TILE", "duration": round(time.time() - start, 2), "worker": worker_id, "worker_name": worker_name, "processed_at": datetime.utcnow().isoformat(), "pre_shape": pre_shape, "post_shape": post_shape, }

        if np.max(pre) <= 0:
            return {"tile_id": tile_id, "pixel_count": 0, "status": "EMPTY_LAND", "duration": round(time.time() - start, 2), "worker": worker_id, "worker_name": worker_name, "processed_at": datetime.utcnow().isoformat(), "pre_shape": pre_shape, "post_shape": post_shape, }

        # FLOOD DETECTION

        pre_db = (10 * np.log10(np.clip(pre, 1e-6, None).astype(np.float32)))

        post_db = (10 * np.log10(np.clip(post, 1e-6, None).astype(np.float32)))

        diff = post_db - pre_db

        flood_mask = (diff < -5.5).astype(np.uint8) # This is the condition i am using to determine a pixel as flood

        pixel_count = int(flood_mask.sum())

        # RESULT

        return {
            "tile_id": tile_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "duration": round(time.time() - start, 2),
            "worker": worker_id,
            "worker_name": worker_name,
            "processed_at": datetime.utcnow().isoformat(),
            "pre_shape": pre_shape,
            "post_shape": post_shape,
        }

    except Exception as e:
        return {
            "tile_id": tile_id,
            "pixel_count": 0,
            "status": f"ERROR : {e}",
            "duration": round(time.time() - start, 2),
            "worker": None,
            "worker_name": None,
            "processed_at": datetime.utcnow().isoformat(),
        }


# MAIN DRIVER

def main():
    print("Starting Dask GeoTIFF processing...")
    # DASK CLIENT
    client = Client("tcp://dask-scheduler:8786")
    print(client)
    # JOB IDS
    job_ids = [37, 38]
    # HDFS GEOTIFF PATHS
    pre_tif = (f"/user/btcchl0040/"
               f"dask_preprocessed/"
               f"{job_ids[0]}/"
               f"{job_ids[0]}_tile.tif")
    post_tif = (f"/user/btcchl0040/"
                f"dask_preprocessed/"
                f"{job_ids[1]}/"
                f"{job_ids[1]}_tile.tif")

    print(f"Pre-event : {pre_tif}")
    print(f"Post-event: {post_tif}")
    # GET RASTER DIMENSIONS
    print("\nReading GeoTIFF dimensions...")
    height, width, crs, transform = (get_geotiff_shape(pre_tif))
    print(f"Raster dimensions: "
          f"{width} x {height}")
    print(f"CRS: {crs}")
    # TILE SIZE

    TILE = 1024
    tasks = []
    tile_id = 0
    # creating multiple jobs
    for y in range(0, height, TILE):
        for x in range(0, width, TILE):
            w = min(TILE, width - x)
            h = min(TILE, height - y)
            tasks.append((tile_id, pre_tif, post_tif, (x, y, w, h)))
            tile_id += 1
    print(f"\nTotal tiles: {len(tasks)}")
    # SUBMIT TASKS
    start = time.time()
    futures = [
        client.submit(process_tile, task, pure=False)
        for task in tasks
    ]
    # COLLECT RESULTS
    results = client.gather(futures)
    pipeline_time = (time.time() - start)
    # TOTAL FLOOD PIXELS
    total_pixels = sum(r["pixel_count"] for r in results)
    # PRINT RESULTS
    print("\n------ FLOOD RESULTS ------\n")
    for r in sorted(results, key=lambda x: x["tile_id"]):
        print(f"Tile {r['tile_id']:02d} | "
              f"{r['status']} | "
              f"Pixels={r['pixel_count']} | "
              f"Duration={r['duration']} sec | "
              f"PRE shape={r.get('pre_shape')} | "
              f"POST shape={r.get('post_shape')} | "
              f"Worker ID={r.get('worker')} | "
              f"Worker Name={r.get('worker_name')}")

    # SUMMARY
    print("\n--------------------------------")
    print("TOTAL FLOOD PIXELS :", total_pixels)
    print(f"TOTAL PIPELINE TIME : "
          f"{pipeline_time:.2f} sec")
    print("--------------------------------")

    # STATISTICS

    durations = [r["duration"] for r in results]
    print("\nTask Statistics")
    print(f"Average Tile Time : "
          f"{np.mean(durations):.2f} sec")
    print(f"Maximum Tile Time : "
          f"{np.max(durations):.2f} sec")
    print(f"Minimum Tile Time : "
          f"{np.min(durations):.2f} sec")
    # CLOSE CLIENT

    client.close()
# ENTRY POINT
if __name__ == "__main__":
    overall = time.time()
    main()
    print(f"Total End-to-End Time : "
          f"{time.time() - overall:.2f} sec")