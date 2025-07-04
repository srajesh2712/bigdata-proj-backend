import rasterio
from rasterio.windows import Window
import matplotlib.pyplot as plt
import numpy as np

# Path to your COG
cog_path = "output_cog.tif"

# Preview output path
preview_path = "preview_tile.tif"

# Desired pixel window (top-left corner at 0,0)
x_off, y_off = 76, 886
width, height = 1512, 1512

with rasterio.open(cog_path) as src:
    window = Window(x_off, y_off, width, height)
    transform = src.window_transform(window)

    # Read window from first band
    data = src.read(1, window=window)

    # Clean profile for writing
    profile = src.profile.copy()
    profile.update({
        "driver": "GTiff",
        "height": height,
        "width": width,
        "transform": transform,
        "count": 1,
        "compress": "lzw",
        "tiled": False
    })

    with rasterio.open(preview_path, "w", **profile) as dst:
        dst.write(data, 1)
        print("✅ Preview tile written:", preview_path)

# 🖼️ Display the preview tile
plt.figure(figsize=(6, 6))
plt.imshow(data, cmap="gray")
plt.title("Preview: Top-left 512×512 pixels")
plt.axis("off")
plt.show()
