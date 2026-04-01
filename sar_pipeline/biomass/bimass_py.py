import rasterio
import matplotlib.pyplot as plt
import numpy as np

# Load the files
with rasterio.open('../../bio_s1_scs__1s_20251129t220107_20251129t220128_t_g01_m01_c03_t043_f228_i_abs.tiff') as amp_src:
    amp = amp_src.read(1).astype(np.float32)
    amp_nodata = amp_src.nodata

with rasterio.open('../../bio_s1_scs__1s_20251129t220107_20251129t220128_t_g01_m01_c03_t043_f228_i_phase.tiff') as phase_src:
    phase = phase_src.read(1).astype(np.float32)

# 1. Handle NoData/NaN (Convert to NaN so they don't affect scaling)
if amp_nodata is not None:
    amp[amp == amp_nodata] = np.nan

# 2. Convert Amplitude to dB and Clip Outliers
# We use percentiles (2nd and 98th) to ensure the image isn't "washed out"
amp_db = 20 * np.log10(amp + 1e-6)
vmin, vmax = np.nanpercentile(amp_db, [2, 98])

# 3. Visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# Amplitude Plot with Robust Scaling
im1 = ax1.imshow(amp_db, cmap='gray', vmin=vmin, vmax=vmax,aspect='auto')
ax1.set_title(f"Amplitude (dB) [Scaled {vmin:.1f} to {vmax:.1f}]")
fig.colorbar(im1, ax=ax1, fraction=0.046, pad=0.04)

# Phase Plot (Phase is usually -pi to pi, so we use a fixed range)
im2 = ax2.imshow(phase, cmap='hsv', vmin=-np.pi, vmax=np.pi,aspect='auto')
ax2.set_title("Phase (Radians) [-π to π]")
fig.colorbar(im2, ax=ax2, fraction=0.046, pad=0.04)

plt.tight_layout()
plt.show()

# Print stats to debug if it's still white
print(f"Amplitude stats: Min={np.nanmin(amp):.2f}, Max={np.nanmax(amp):.2f}, Mean={np.nanmean(amp):.2f}")