import rasterio
import matplotlib.pyplot as plt
import numpy as np

def visualize_tiff(tif_path):
    with rasterio.open(tif_path) as src:
        data = src.read()  # shape: (bands, height, width)

        if data.shape[0] == 1:
            # Single-band: show grayscale
            plt.imshow(data[0], cmap='Blues',vmin=0, vmax=1)
            plt.title("Single-band TIFF")
            plt.colorbar(label='Pixel Value')
        elif data.shape[0] >= 3:
            # Multi-band (e.g., RGB): take first 3 bands
            rgb = np.stack([data[0], data[1], data[2]], axis=-1)
            # Normalize to 0–1 for matplotlib
            rgb = (rgb - rgb.min()) / (rgb.max() - rgb.min())
            plt.imshow(rgb)
            plt.title("RGB Composite (Bands 1-3)")
        else:
            print("Unsupported band count for visualization.")
            return

        plt.axis('off')
        plt.tight_layout()
        # ... inside your visualize_tiff function, just before plt.show():
        #plt.savefig("visualization_output.png", dpi=300)
        #plt.close()

        plt.show()

# Example usage
visualize_tiff("/home/btcchl0040/Documents/SAR_Data/FLOOD_MASK/20250802_182055.tif")
