import os
import fsspec
import numcodecs
import rioxarray
import xarray as xr
HADOOP_USER = os.environ.get("HADOOP_USER_NAME", "btcchl0040")
def convert_hdfs_tiff_to_zarr(hdfs_tiff_path, chunk_size=512):
    """
    Reads a preprocessed GeoTIFF file directly from HDFS and exports it
    as a consolidated Zarr data cube within the same target directory.

    Parameters:
        hdfs_tiff_path (str): Full HDFS path to the target .tif file
                              (e.g., '/user/btcchl0040/dask_preprocessed/8/101_tile.tif')
        chunk_size (int): Spatial dimension for chunking (default: 512x512)

    Returns:
        str: The consolidated HDFS path URL to the created Zarr store.
    """
    # 1. Establish the target HDFS Zarr directory path swap (.tif -> .zarr)
    hdfs_zarr_path = hdfs_tiff_path.replace(".tif", ".zarr")
    zarr_hdfs_url = f"hdfs://namenode:8020{hdfs_zarr_path}"

    try:
        # 2. Verify target directory environment via fsspec
        fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
        zarr_dir = os.path.dirname(hdfs_zarr_path)
        if not fs.exists(zarr_dir):
            fs.makedirs(zarr_dir)

        # 3. Stream rasterio source out of HDFS with spatial chunk allocations
        # (This avoids loading the entire uncompressed scene directly into RAM)
        tiff_hdfs_url = f"hdfs://namenode:8020{hdfs_tiff_path}"
        rds = rioxarray.open_rasterio(tiff_hdfs_url, chunks={'x': chunk_size, 'y': chunk_size})

        # 4. Define an aggressive, cloud-native compression profile (Blosc + Zstd)
        compressor = numcodecs.Blosc(
            cname="zstd",
            clevel=3,
            shuffle=numcodecs.Blosc.SHUFFLE
        )
        encoding = {rds.name: {"compressor": compressor}} if rds.name else {}

        # 5. Commit write stream directly into HDFS cluster storage
        rds.to_zarr(
            zarr_hdfs_url,
            mode="w",
            encoding=encoding,
            consolidated=True
        )

        return zarr_hdfs_url

    except Exception as e:
        raise RuntimeError(f"Failed HDFS Zarr compilation for {hdfs_tiff_path}: {str(e)}")

convert_hdfs_tiff_to_zarr('/user/btcchl0040/dask_preprocessed/8/40_tile.tif')
