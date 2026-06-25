import os
import fsspec
import rioxarray
import dask

dask.config.set(scheduler="synchronous")


def convert_hdfs_tiff_to_zarr(hdfs_tiff_path):

    local_zarr_path = "/tmp/40_tile.zarr"   # TRUE local FS

    fs = fsspec.filesystem("hdfs", host="localhost", port=8020)

    # READ from HDFS
    with fs.open(hdfs_tiff_path, "rb") as f:
        da = rioxarray.open_rasterio(f)

    da = da.drop_vars("spatial_ref", errors="ignore")

    # WRITE LOCALLY (IMPORTANT FIX)
    da.to_dataset(name="band_data").to_zarr(
        local_zarr_path,
        mode="w",
        consolidated=False
    )

    return local_zarr_path


convert_hdfs_tiff_to_zarr(
    "/user/btcchl0040/spark_preprocessed/8/40_tile.tif"
)