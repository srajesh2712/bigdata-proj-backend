import rasterio
import numpy as np
print(rasterio.__version__)
from rasterio.windows import Window
from matplotlib.colors import ListedColormap
pre_flood_file = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Preflood-May24-2024\\20240524\\subset_3_of_S1A_IW_GRDH_1SDV_20240524T115717_20240524T115742_054013_069101_5DC9_Orb_Cal_Spk_TC.tif'
post_flood_file  = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Flood-June5-2024\\20240605\\subset_0_of_S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB_Orb_Cal_Spk_TC.tif'



# Define tile size
tile_size = 512

# Output flood mask path
output_path = "flood_mask.tif"

# Open both pre- and post-flood TIFFs
with rasterio.open(pre_flood_file) as src_pre, rasterio.open(post_flood_file) as src_post:

    profile = src_post.profile.copy()
    profile.update(dtype=rasterio.uint8, count=1, compress='lzw')  # optimize output

    width = src_post.width
    height = src_post.height

    with rasterio.open(output_path, "w", **profile) as dst:

        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                w = min(tile_size, width - x)
                h = min(tile_size, height - y)
                window = Window(x, y, w, h)

                pre_tile = src_pre.read(2, window=window)
                post_tile = src_post.read(2, window=window)

                pre_dB = 10 * np.log10(np.clip(pre_tile, 1e-6, None))
                post_dB = 10 * np.log10(np.clip(post_tile, 1e-6, None))
                #print(pre_dB)
                # Basic flood logic: drop in backscatter > 2 dB
                diff_tile = post_dB - pre_dB
                mask_tile = (diff_tile < -4.5).astype(np.uint8)
                #print(mask_tile)
                dst.write(mask_tile , 1, window=window)



import matplotlib.pyplot as plt
import rasterio

with rasterio.open("flood_mask.tif") as src:
    mask = src.read(1)

mask = (mask > 0).astype(np.uint8)  # 1 = flood, 0 = non-flood



# Plot
plt.figure(figsize=(8, 8))
plt.imshow(mask, cmap="Blues", vmin=0, vmax=1)  # force value range for cmap
plt.title("Flood Mask (Blue = Flooded)", fontsize=14)
plt.axis('off')
plt.show()

