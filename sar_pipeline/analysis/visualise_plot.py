import matplotlib.pyplot as plt
import rasterio
import numpy as np

with rasterio.open('/home/btcchl0040/Documents/SAR_Data/FLOOD_MASK/20250802_154907.tif') as src:
    window = rasterio.windows.Window(0, 0, 512, 512)  # Read a 512x512 tile
    mask = src.read(1, window=window)

mask = (mask > 0).astype(np.uint8)  # 1 = flood, 0 = non-flood



# Plot
plt.figure(figsize=(8, 8))
plt.imshow(mask, cmap="Blues", vmin=0, vmax=1)  # force value range for cmap
plt.title("Flood Mask (Blue = Flooded)", fontsize=14)
plt.axis('off')
plt.show()