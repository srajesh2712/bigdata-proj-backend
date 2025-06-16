import rasterio
import numpy as np
import matplotlib.pyplot as plt


print(rasterio.__version__)


# Load the GeoTIFF
with rasterio.open('E:\\Big Data\\Summer Project\\Assam-June5-2025\\Flood-June5-2025\\20240605\\output\\1750044647.7367983\\output_file.tif') as src:
    backscatter = src.read(1)
    profile = src.profile

# Basic visualization
plt.imshow(backscatter, cmap='gray', vmin=-25, vmax=0)
plt.title('Sigma0_VV Backscatter')
plt.colorbar(label='dB')
plt.show()

# Threshold-based flood mask
threshold = -17  # You can tweak this!
flood_mask = backscatter < threshold

# Visualize mask
plt.imshow(flood_mask, cmap='Blues')
plt.title('Detected Flood Areas')
plt.show()

# Optional: Save flood mask as GeoTIFF
profile.update(dtype=rasterio.uint8, count=1)
with rasterio.open('flood_mask.tif', 'w', **profile) as dst:
    dst.write(flood_mask.astype(rasterio.uint8), 1)
