import rasterio
import numpy as np

# Path to your flood mask GeoTIFF
mask_path = "/home/btcchl0040/Documents/SAR_Data/FLOOD_MASK/20250802_182055.tif"

with rasterio.open(mask_path) as src:
    mask = src.read(1)  # Read first band

# Count flooded pixels (value == 1)
flooded_pixels = np.sum(mask == 1)

print(f"Flooded pixel count: {flooded_pixels}")

non_flooded_pixels = np.sum(mask == 0)

print(f"Non Flooded pixel count: {non_flooded_pixels}")
total_pixels = mask.size
percent_flooded = (flooded_pixels / total_pixels) * 100

print(f"Total pixels: {total_pixels}")
print(f"Flooded: {flooded_pixels} ({percent_flooded:.2f}%)")
print(f"Non-flooded: {non_flooded_pixels} ({100 - percent_flooded:.2f}%)")

