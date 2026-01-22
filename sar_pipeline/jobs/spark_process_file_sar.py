import numpy as np
import rasterio
from rasterio.windows import Window
from pyspark.sql import SparkSession
import fsspec
import logging
import os
import io
# Set logging to see what's happening
logging.basicConfig(level=logging.INFO)


def process_sar_tile(task_data):
    window_id, pre_path, post_path, win_coords = task_data

    # Ensure the worker knows who it is for HDFS
    os.environ['HADOOP_USER_NAME'] = 'root'

    try:
        # 1. Open the HDFS filesystem lazily
        # This uses the native libhdfs bridge we built into the image
        fs = fsspec.filesystem("hdfs", host="namenode", port=8020)

        def stream_window(path, window_obj):
            clean_path = path.replace("hdfs://namenode:8020", "")
            gdal_options = {
                'GDAL_CACHEMAX': 256,  # Limit cache to 256MB
                'GDAL_DISABLE_READDIR_ON_OPEN': 'TRUE',
                'VSI_CACHE': 'FALSE'  # Do not cache large chunks of the file
            }
            # fs.open returns a file-like object that supports 'seek'
            # Rasterio uses 'seek' to jump to the exact pixel coordinates
            with fs.open(clean_path, 'rb') as f:
                with rasterio.Env(**gdal_options):
                    with rasterio.open(f, sharing=False) as src:
                        data = src.read(1, window=window_obj)
                        transform = src.window_transform(window_obj)
                        meta = src.meta.copy()
                        return data, transform, meta

        win = Window(*win_coords)

        # 2. Only the required bytes are transferred here
        tile_pre, _, _  = stream_window(pre_path, win)
        tile_post,tile_transform, tile_meta = stream_window(post_path, win)

        # 3. Check for empty data before doing heavy math
        if np.max(tile_pre) <= 0:
            return (window_id, 0, "EMPTY_LAND")

        # 4. Math: Log-Ratio (dB)
        # Using 32-bit floats to save even more memory
        pre_dB = 10 * np.log10(np.clip(tile_pre, 1e-6, None).astype(np.float32))
        post_dB = 10 * np.log10(np.clip(tile_post, 1e-6, None).astype(np.float32))

        flood_mask = (post_dB - pre_dB < -5.5).astype(np.uint8)


        pixel_count = int(np.sum(flood_mask))

        # 6. Update Metadata for the Tile
        tile_meta.update({
            "driver": "GTiff",
            "height": win.height,
            "width": win.width,
            "transform": tile_transform,
            "dtype": 'float32',
            "count": 1,
            "compress": 'lzw'
        })

        # 7. Stream Write back to HDFS
        mask_filename = f"/user/btcchl0040/sar/results/masks/tile_{window_id:02d}_mask.tif"

        # We build the TIFF structure in memory and push to HDFS
        buffer = io.BytesIO()
        with rasterio.open(buffer, 'w', **tile_meta) as dst:
            dst.write(flood_mask, 1)

        buffer.seek(0)
        with fs.open(mask_filename, 'wb') as f_out:
            f_out.write(buffer.read())

        return (window_id, pixel_count, "SUCCESS")

    except Exception as e:
        return (window_id, 0, f"ERROR: {str(e)}")


def main():
    spark = SparkSession.builder \
        .appName("SAR_Efficient_Tiling") \
        .getOrCreate()

    # Paths
    pre_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
    post_p = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

    # Define 42 tasks
    tasks = []
    for r in range(6):
        for c in range(7):
            tasks.append((r * 7 + c, pre_p, post_p, (c * 1024, r * 1024, 1024, 1024)))

    # Process in small chunks to keep the host machine responsive
    rdd = spark.sparkContext.parallelize(tasks, numSlices=len(tasks))
    results = rdd.map(process_sar_tile).collect()

    print("\n--- FLOOD DETECTION RESULTS ---")
    total = 0
    for wid, count, status in sorted(results):
        print(f"Tile {wid:02d}: {status} | Pixels: {count}")
        total += count
    print(f"TOTAL FLOOD PIXELS: {total}\n")

    spark.stop()


if __name__ == "__main__":
    main()
