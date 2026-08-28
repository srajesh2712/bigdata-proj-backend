from dask.distributed import Client
import time
import numpy as np
import zarr
import fsspec
import logging
from datetime import datetime
from dask.distributed import get_worker
logging.basicConfig(level=logging.INFO)

# ----------------------------------------
# HDFS ZARR READER
# ----------------------------------------
def open_zarr_hdfs(path):

    fs = fsspec.filesystem("hdfs",
                           host="namenode",
                           port=8020)

    mapper = fs.get_mapper(path)

    if "_metadata" in fs.ls(path):
        return zarr.open_consolidated(mapper, mode="r")

    return zarr.open(mapper, mode="r")


# ----------------------------------------
# TILE PROCESSOR
# ----------------------------------------
def process_tile(task):

    tile_id, pre_path, post_path, win = task

    start = time.time()

    try:
        worker = get_worker()

        worker_id = worker.address
        worker_name = worker.name
        x, y, w, h = win

        pre_store = open_zarr_hdfs(pre_path)
        post_store = open_zarr_hdfs(post_path)

        pre_shape = pre_store["band_data"].shape
        post_shape = post_store["band_data"].shape

        pre = pre_store["band_data"][0, y:y+h, x:x+w]
        post = post_store["band_data"][0, y:y+h, x:x+w]

        # Empty tile
        if np.max(pre) <= 0:

            return {
                "tile_id": tile_id,
                "pixel_count": 0,
                "status": "EMPTY_LAND",
                "duration": round(time.time()-start,2),
                "worker": worker_id,
                "worker_name": worker_name,
                "processed_at": datetime.utcnow().isoformat(),
                "pre_shape": pre_shape,
                "post_shape": post_shape,
            }

        # ----------------------------------------
        # Flood Detection
        # ----------------------------------------

        pre_db = 10*np.log10(np.clip(pre,1e-6,None).astype(np.float32))
        post_db = 10*np.log10(np.clip(post,1e-6,None).astype(np.float32))

        diff = post_db - pre_db

        flood_mask = (diff < -5.5).astype(np.uint8)

        pixel_count = int(flood_mask.sum())

        return {

            "tile_id": tile_id,
            "pixel_count": pixel_count,
            "status": "SUCCESS",
            "duration": round(time.time()-start,2),
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
            "duration": round(time.time()-start,2),
            "processed_at": datetime.utcnow().isoformat(),


        }


# ----------------------------------------
# MAIN DRIVER
# ----------------------------------------
def main():

    client = Client("tcp://dask-scheduler:8786")

    print(client)

    #pre_zarr = "hdfs://namenode:8020/user/btcchl0040/spark_preprocessed/7/43_tile.zarr"

    #post_zarr = "hdfs://namenode:8020/user/btcchl0040/spark_preprocessed/8/40_tile.zarr"
    
    job_ids = [37, 38]

    pre_zarr = (
        f"/user/btcchl0040/dask_preprocessed/"
        f"{job_ids[0]}/{job_ids[0]}_tile.zarr"
    )

    post_zarr = (
        f"/user/btcchl0040/dask_preprocessed/"
        f"{job_ids[1]}/{job_ids[1]}_tile.zarr"
    )
    
    
    TILE = 1024

    tasks = []
    tile_id = 0
    pre_store = open_zarr_hdfs(pre_zarr)
    shape = pre_store["band_data"].shape
    height = shape[1]
    width = shape[2]
    for y in range(0, height, TILE):

        for x in range(0, width, TILE):
            w = min(TILE, width - x)
            h = min(TILE, height - y)

            tasks.append(
                (
                    tile_id,
                    pre_zarr,
                    post_zarr,
                    (x, y, w, h)
                )
            )

            tile_id += 1

    start = time.time()

    futures = [

        client.submit(
            process_tile,
            task,
            pure=False
        )

        for task in tasks

    ]

    results = client.gather(futures)

    pipeline_time = time.time() - start

    total_pixels = sum(r["pixel_count"] for r in results)

    print("\n------ FLOOD RESULTS ------\n")

    for r in sorted(results,
                    key=lambda x:x["tile_id"]):

        print(
            f"Tile {r['tile_id']:02d} | "
            f"{r['status']} | "
            f"Pixels={r['pixel_count']} | "
            f"Duration={r['duration']} sec"
            f"PRE shape={r.get('pre_shape')} | "
            f"POST shape={r.get('post_shape')}"
            f"Worker ID={r.get('worker')} | "
            f"Worker Name={r.get('worker_name')}"
        )

    print("\n--------------------------------")
    print("TOTAL FLOOD PIXELS :", total_pixels)
    print(f"TOTAL PIPELINE TIME : {pipeline_time:.2f} sec")
    print("--------------------------------")

    client.close()


if __name__ == "__main__":

    overall = time.time()

    main()

    print("\n===================================")
    print(f"Total End-to-End Time : {time.time()-overall:.2f} sec")
    print("===================================\n")
