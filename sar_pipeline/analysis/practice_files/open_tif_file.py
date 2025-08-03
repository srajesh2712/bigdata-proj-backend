import rasterio
import matplotlib.pyplot as plt
import numpy as np
import os
def has_flood_pixels(tif_path,threshold=0):
    with rasterio.open(tif_path) as src:
        data = src.read(1)  # First band
        return np.any(data > threshold)


def check_all_tiles(directory, extension=".tif"):
    flooded_files = []
    all_files = sorted([f for f in os.listdir(directory) if f.endswith(extension)])

    for file in all_files:
        full_path = os.path.join(directory, file)
        if has_flood_pixels(full_path):
            flooded_files.append(file)

    print(f"\nFlooded files ({len(flooded_files)} found):")
    for f in flooded_files:
        print(f)

    return flooded_files



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
visualize_tiff("/home/btcchl0040/Documents/SAR_Data/FLOOD_MASK/20250802_182055.tif") #tile_8192_4096  tile_8192_6144 tile_8192_8192
folder_path="/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE"
#check_all_tiles(folder_path)