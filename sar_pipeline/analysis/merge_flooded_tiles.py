import os
from rasterio.merge import merge
import rasterio
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
def merge_flooded_tiles(mask_path,output_path,threshold_percent=10):


    tile_infos = []

    for tile_file in sorted(os.listdir(mask_path)):
        tile_path = os.path.join(mask_path, tile_file)
        with rasterio.open(tile_path) as src:
            data = src.read(1)
            total_pixels = data.size
            flooded_pixels = np.sum(data == 1)
            flooded_percent = (flooded_pixels / total_pixels) * 100
            print(flooded_percent)
            if flooded_percent > threshold_percent:
                tile_infos.append({
                    "path": tile_path,
                    "flooded_pixels": flooded_pixels,
                    "flooded_percent": flooded_percent
                })
                print(f"Keeping {tile_path}: {flooded_pixels} flooded pixels ({flooded_percent:.2f}%)")
            else:
                print(f"Skipping {tile_path}: only {flooded_percent:.2f}% flooded")


            print(f"{tile_file}: {flooded_pixels} flooded pixels ({flooded_percent:.2f}%)")


    selected_tiles = [t["path"] for t in tile_infos if t["flooded_percent"] > 5]




    src_files_to_mosaic = [rasterio.open(fp) for fp in selected_tiles]
    mosaic, out_trans = merge(src_files_to_mosaic)

    out_meta = src_files_to_mosaic[0].meta.copy()
    out_meta.update({
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans
    })
    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(mosaic)

    open_merged_file(output_path)

def open_merged_file(file_to_open):


    with rasterio.open(file_to_open) as src:
        data = src.read(1)

    # Create a color map: 0 = black, 1 = red

    cmap = ListedColormap(['white', 'blue'])

     # two discrete values
    cmap.set_under('black')  # anything <1 is black
    cmap.set_over('blue')     # anything >1 is red (optional)

    # Plot
    plt.figure(figsize=(10, 10))
    plt.imshow(data, cmap=cmap, vmin=0, vmax=1)
    plt.colorbar(label='Flood Mask (0 = No Flood, 1 = Flood)')
    plt.title("Flood Mosaic (Filtered Tiles)")
    plt.axis('off')
    plt.tight_layout()
    plt.show()
