import rasterio
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
load_dotenv()
print(rasterio.__version__)
from rasterio.windows import Window
from matplotlib.colors import ListedColormap
pre_folder = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/split'
post_folder  = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/split'
output_base = os.getenv('BASE_PATH')
flood_mask_subfolder = os.getenv('FLOOD_MASK')
threshold_db = -5.5
# Define tile size
tile_size = 512

# Output flood mask path
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file =  f"{timestamp}.tif"
output_folder = os.path.join(output_base, flood_mask_subfolder, timestamp)
os.makedirs(output_folder, exist_ok=True)
# Open both pre- and post-flood TIFFs
flood_counts = []
for tile_filename in sorted(os.listdir(pre_folder)):
    print(f'Starting time{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    if not tile_filename.endswith(".tif"):
        continue

    pre_tile_path = os.path.join(pre_folder, tile_filename)
    post_tile_path = os.path.join(post_folder, tile_filename)

    if not os.path.exists(post_tile_path):
        print(f"⚠️ Missing post tile: {tile_filename}")
        continue

    output_tif_path = os.path.join(output_folder, f"{tile_filename.replace('.tif', '')}_mask.tif")
    output_png_path = output_tif_path.replace(".tif", ".png")
    with rasterio.open(pre_tile_path) as src_pre, rasterio.open(post_tile_path) as src_post:

        profile = src_post.profile.copy()
        profile.update(dtype=rasterio.uint8, count=1, compress='lzw')  # optimize output

        width, height = src_post.width, src_post.height
        total_flood_pixels = 0
        with rasterio.open(output_tif_path, "w", **profile) as dst:

            mask_full = np.zeros((height, width), dtype=np.uint8)

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
                    mask_tile = (diff_tile < -5.5).astype(np.uint8)
                    mask_full[y:y + h, x:x + w] = mask_tile
                    flood_pixel_count = np.sum(mask_tile)
                    total_flood_pixels += flood_pixel_count
                    #print(mask_tile)
                    dst.write(mask_tile , 1, window=window)
        flood_counts.append((output_tif_path, total_flood_pixels))
        plt.imsave(output_png_path, mask_full, cmap="Blues", vmin=0, vmax=1)
    print(f'Ending time{datetime.now().strftime("%Y%m%d_%H%M%S")}')

import matplotlib.pyplot as plt
import rasterio


# Visualize one flood mask
flood_counts.sort(key=lambda x: x[1], reverse=True)
most_flooded_tile, count = flood_counts[0]
print(f"Most flooded tile: {os.path.basename(most_flooded_tile)} with {count} pixels")

# Visualize the tile with most flood
with rasterio.open(most_flooded_tile) as src:
    mask = src.read(1)

plt.figure(figsize=(8, 8))
plt.imshow(mask, cmap="Blues", vmin=0, vmax=1)
plt.title(f"Most Flooded Tile: {os.path.basename(most_flooded_tile)}", fontsize=12)
plt.axis("off")
plt.tight_layout()
plt.show()
