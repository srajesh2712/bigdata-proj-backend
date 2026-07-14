import xarray as xr
import fsspec

fs = fsspec.filesystem(
    "hdfs",
    host="namenode",
    port=8020
)

mapper = fs.get_mapper(
    "/user/btcchl0040/spark_preprocessed/7/43_tile.zarr"
)

ds = xr.open_zarr(mapper)

print("\n===== DATASET =====")
print(ds)

print("\n===== DATASET ATTRS =====")
print(ds.attrs)

print("\n===== VARIABLE ATTRS =====")
print(ds.band_data.attrs)

try:
    print(ds.x.values[0])
    print(ds.x.values[1])

    print(ds.y.values[0])
    print(ds.y.values[1])
    print("\n===== CRS =====")
    print(ds.band_data.rio.crs)

    print("\n===== TRANSFORM =====")
    print(ds.band_data.rio.transform())

except Exception as e:
    print("RIO ERROR:", e)