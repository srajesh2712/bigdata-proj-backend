import os

files = [
    "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_dask_preprocessed.tif",
    "/home/btcchl0040/Documents/SAR_Data/validation/40_tile.zarr",
    "/home/btcchl0040/Documents/SAR_Data/validation/Snap_preprocessed.tif",
    "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_spark_preprocessed.tif",
    "/home/btcchl0040/Documents/SAR_Data/validation/Pysnap_preprocessed.tif",
]

for file in files:
    if os.path.isdir(file):
        size = sum(
            os.path.getsize(os.path.join(root, f))
            for root, _, filenames in os.walk(file)
            for f in filenames
        )
    else:
        size = os.path.getsize(file)

    print(f"{file:<35} {size / (1024 * 1024):>10.2f} MB")