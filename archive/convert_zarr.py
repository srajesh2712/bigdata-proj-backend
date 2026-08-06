import os
import fsspec
import rioxarray
import dask

dask.config.set(scheduler="synchronous")

def convert_hdfs_tiff_to_zarr(hdfs_tiff_path):

    local_zarr_path = "/tmp/43_tile.storage"

    fs = fsspec.filesystem("hdfs", host="localhost", port=8020)

    # Read TIFF from HDFS
    with fs.open(hdfs_tiff_path, "rb") as f:
        da = rioxarray.open_rasterio(f)

    da = da.drop_vars("spatial_ref", errors="ignore")

    # Write local Zarr
    da.to_dataset(name="band_data").to_zarr(
        local_zarr_path,
        mode="w",
        consolidated=False
    )

    # HDFS destination
    hdfs_zarr_path = "/user/btcchl0040/spark_preprocessed/7/43_tile.zarr"

    # Upload entire directory to HDFS
    fs.put(
        local_zarr_path,
        hdfs_zarr_path,
        recursive=True
    )

    return hdfs_zarr_path


hdfs_path = convert_hdfs_tiff_to_zarr(
    "/user/btcchl0040/spark_preprocessed/7/43_tile.tif"
)

print(hdfs_path)