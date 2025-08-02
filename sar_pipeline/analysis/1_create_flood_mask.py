import rasterio
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
print(rasterio.__version__)
from rasterio.windows import Window
from matplotlib.colors import ListedColormap
pre_flood_file = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250802_151438.tif'
post_flood_file  = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250802_151542.tif'



# Define tile size
tile_size = 512

# Output flood mask path
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file =  f"{timestamp}.tif"
print(os.getenv('BASE_PATH'))
print(os.getenv('FLOOD_MASK'))
output_path = os.path.join(os.getenv('BASE_PATH'),os.getenv('FLOOD_MASK'),f"{timestamp}.tif")

# Open both pre- and post-flood TIFFs
with rasterio.open(pre_flood_file) as src_pre, rasterio.open(post_flood_file) as src_post:

    profile = src_post.profile.copy()
    profile.update(dtype=rasterio.uint8, count=1, compress='lzw')  # optimize output

    width = src_post.width
    height = src_post.height
    print(output_path)
    with rasterio.open(output_path, "w", **profile) as dst:

        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                w = min(tile_size, width - x)
                h = min(tile_size, height - y)
                window = Window(x, y, w, h)

                pre_tile = src_pre.read(1, window=window)
                post_tile = src_post.read(1, window=window)

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


with rasterio.open(output_path) as src:
    mask = src.read(1)

mask = (mask > 0).astype(np.uint8)  # 1 = flood, 0 = non-flood



# Plot
plt.figure(figsize=(8, 8))
plt.imshow(mask, cmap="Blues", vmin=0, vmax=1)  # force value range for cmap
plt.title("Flood Mask (Blue = Flooded)", fontsize=14)
plt.axis('off')
plt.show()

