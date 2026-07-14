import numpy as np
import rasterio
import matplotlib.pyplot as plt
from scipy.ndimage import uniform_filter


def load_complex(amp_path, phase_path):
    """Loads separate TIFFs and reconstructs the complex SAR signal."""
    with rasterio.open(amp_path) as src_amp, rasterio.open(phase_path) as src_phase:
        amp = src_amp.read(1).astype(np.float32)
        phase = src_phase.read(1).astype(np.float32)

        # Identify NoData and set to NaN
        nodata = src_amp.nodata
        if nodata is not None:
            amp[amp == nodata] = np.nan

        # Reconstruct: A * e^(j*phi)
        return amp * np.exp(1j * phase)


# --- Paths ---
path_1 = '/home/btcchl0040/Documents/SAR_Data/Biomass/BIO_S2_STA__1S_20251212T011542_20251212T011603_T_G01_M01_C01_T001_F222_01_DKVA65/measurement/'
path_2 = '/home/btcchl0040/Documents/SAR_Data/Biomass/BIO_S2_STA__1S_20251215T011543_20251215T011604_T_G01_M01_C02_T001_F222_01_DKVA6F/measurement/'

# --- 1. Load images ---
img1 = load_complex(f'{path_1}bio_s2_sta__1s_20251212t011542_20251212t011603_t_g01_m01_c01_t001_f222_i_abs.tiff',
                    f'{path_1}bio_s2_sta__1s_20251212t011542_20251212t011603_t_g01_m01_c01_t001_f222_i_phase.tiff')
img2 = load_complex(f'{path_2}bio_s2_sta__1s_20251215t011543_20251215t011604_t_g01_m01_c02_t001_f222_i_abs.tiff',
                    f'{path_2}bio_s2_sta__1s_20251215t011543_20251215t011604_t_g01_m01_c02_t001_f222_i_phase.tiff')

# --- 2. Generate the Interferogram ---
interferogram_complex = img1 * np.conj(img2)

# --- 3. Apply Multi-looking ---
window_size = 10
# We use nan_to_num so the filter doesn't spread NaNs to the whole image
real_part = np.nan_to_num(interferogram_complex.real)
imag_part = np.nan_to_num(interferogram_complex.imag)

real_avg = uniform_filter(real_part, size=window_size)
imag_avg = uniform_filter(imag_part, size=window_size)
smoothed_interferogram = real_avg + 1j * imag_avg

# Extract fringes
fringes = np.angle(smoothed_interferogram)

# Mask out areas where there was no data (amplitude was 0 or NaN)
mask = np.isnan(np.abs(img1)) | (np.abs(img1) == 0)
fringes[mask] = np.nan

# --- 4. Visualization ---
fig, ax = plt.subplots(figsize=(10, 12))

# Use aspect='auto' to stretch the narrow strip
# 'hsv' is critical for phase
im = ax.imshow(fringes, cmap='hsv', aspect='auto', vmin=-np.pi, vmax=np.pi)

plt.colorbar(im, label='Phase Difference (Radians)', ticks=[-np.pi, 0, np.pi])
ax.set_title("Interferometric Fringes (Biomass P-Band)")

# Print debug info to console
print(f"Fringes range: {np.nanmin(fringes):.2f} to {np.nanmax(fringes):.2f}")
print(f"Shape: {fringes.shape}")

plt.show()