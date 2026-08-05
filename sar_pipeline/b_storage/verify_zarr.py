import os
import fsspec
import xarray as xr
import rioxarray

HADOOP_USER = "btcchl0040"
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

fs = fsspec.filesystem(
    "hdfs",
    host="namenode",
    port=8020,
    user=HADOOP_USER
)

zarr_path = "/user/btcchl0040/dask_preprocessed/31/31_tile.zarr"

ds = xr.open_zarr(
    fs.get_mapper(zarr_path),
    consolidated=False
)

ds = ds.rio.write_crs(
    ds.spatial_ref.attrs["crs_wkt"],
    inplace=False
)

print(ds.band_data.rio.crs)
print(ds)

print("\nSpatial Reference Metadata:")
print(ds.spatial_ref.attrs)


print("\nCRS from rioxarray:")
print(ds.band_data.rio.crs)