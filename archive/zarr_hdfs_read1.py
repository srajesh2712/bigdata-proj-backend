import xarray as xr
import fsspec
import os
import time
import numpy as np

HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020
HDFS_ZARR_PATH = "/user/btcchl0040/dask_preprocessed/1/13_tile.storage"

os.environ["HADOOP_USER_NAME"] = HADOOP_USER


def read_from_hdfs():
    print("--- Connecting to HDFS via fsspec (Zarr v2) ---")

    fs = fsspec.filesystem(
        "hdfs",
        host=HDFS_NAMENODE,
        port=HDFS_PORT,
        user=HADOOP_USER
    )
    print("FS TYPE:", type(fs))
    print("FS MODULE:", fs.__class__.__module__)
    mapper = fs.get_mapper(HDFS_ZARR_PATH)

    start_open = time.time()
    ds = xr.open_zarr(mapper, consolidated=True)
    print(f"✅ Metadata Loaded in {time.time() - start_open:.4f}s")

    print("\n📌 Dataset Variables:", list(ds.data_vars))
    print("📌 Dataset Coords:", list(ds.coords))

    # rename for convenience
    ds = ds.rename({"__xarray_dataarray_variable__": "band_data"})

    # choose band
    band_index = 0

    # read a small chunk (example 200x200 window)
    print("\n🔍 Reading small window to search non-zero pixels...")
    window = ds.band_data.isel(band=band_index, x=slice(0, 200), y=slice(0, 200)).values

    # find non-zero locations
    nonzero = np.argwhere(window != 0)

    if nonzero.shape[0] == 0:
        print("❌ No non-zero pixels found in first 200x200 window.")
        return

    print(f"✅ Found {nonzero.shape[0]} non-zero pixels in first 200x200 window")

    # print first 10 non-zero pixels
    print("\n📌 First few non-zero pixels (y, x, value):")
    for i in range(min(10, nonzero.shape[0])):
        y, x = nonzero[i]
        print(f"   y={y}, x={x}, value={window[y, x]}")

    # also print their real coordinate values
    print("\n📌 Real coordinate positions:")
    for i in range(min(5, nonzero.shape[0])):
        y, x = nonzero[i]
        real_x = float(ds.x.values[x])
        real_y = float(ds.y.values[y])
        print(f"   coord_x={real_x}, coord_y={real_y}, value={window[y, x]}")


if __name__ == "__main__":
    read_from_hdfs()
