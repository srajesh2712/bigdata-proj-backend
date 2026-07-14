import os
import rasterio
from rasterio.merge import merge
import matplotlib.pyplot as plt
import numpy as np

def merge_flooded_tiles(mask_path, output_path, threshold_db=-5.5, threshold_percent=10):
    """
    Merge flood mask tiles into a single mosaic.

    Parameters:
        mask_path (str): Folder containing flood mask tiles (.tif)
        output_path (str): Output merged TIFF path
        threshold_db (float): Threshold in dB to consider a pixel flooded
        threshold_percent (float): Minimum % of flooded pixels to include a tile
    """

    tile_infos = []

    # --- Step 1: Select tiles based on threshold ---
    for tile_file in sorted(os.listdir(mask_path)):
        if not tile_file.endswith(".tif"):
            continue
        tile_path = os.path.join(mask_path, tile_file)
        with rasterio.open(tile_path) as src:
            data = src.read(1)
            flooded_pixels = np.sum(data < threshold_db)
            flooded_percent = (flooded_pixels / data.size) * 100
            flooded_percent =100
            if flooded_percent >= threshold_percent:
                tile_infos.append(tile_path)
                print(f"Keeping {tile_path}: {flooded_pixels} flooded pixels ({flooded_percent:.2f}%)")
            else:
                print(f"Skipping {tile_path}: only {flooded_percent:.2f}% flooded")

    if not tile_infos:
        print("No tiles selected for merging. Exiting.")
        return

    # --- Step 2: Flip tiles if needed and prepare paths for merging ---
    tiles_to_merge = []
    tmp_files = []  # keep track of temporary flipped files
    for fp in tile_infos:
        with rasterio.open(fp) as src:
            if src.transform.e > 0:  # upside-down
                data = np.flipud(src.read(1))
                meta = src.meta.copy()
                meta.update(height=data.shape[0],
                            width=data.shape[1],
                            transform=rasterio.Affine(
                                meta['transform'].a, meta['transform'].b, meta['transform'].c,
                                meta['transform'].d, -meta['transform'].e,
                                meta['transform'].f + meta['transform'].e * data.shape[0]
                            ))
                tmp_fp = fp.replace(".tif", "_flipped.tif")
                with rasterio.open(tmp_fp, "w", **meta) as dst:
                    dst.write(data, 1)
                tiles_to_merge.append(tmp_fp)
                tmp_files.append(tmp_fp)
            else:
                tiles_to_merge.append(fp)

    # --- Step 3: Merge tiles ---
    mosaic, out_trans = merge(tiles_to_merge)

    # --- Step 4: Save output ---
    with rasterio.open(tiles_to_merge[0]) as src0:
        out_meta = src0.meta.copy()
    out_meta.update({
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans
    })
    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(mosaic)

    # --- Step 5: Clean temporary flipped files ---
    for tmp_fp in tmp_files:
        os.remove(tmp_fp)

    print(f"Mosaic saved to {output_path}")

    # --- Step 6: Show merged mosaic ---
    open_merged_file(output_path)


def open_merged_file(file_to_open):
    with rasterio.open(file_to_open) as src:
        data = src.read(1)
    plt.figure(figsize=(10, 10))
    plt.imshow(data, cmap="Blues", vmin=np.min(data), vmax=np.max(data))
    plt.colorbar(label='Flood Mask')
    plt.title("Flood Mosaic")
    plt.axis('off')
    plt.tight_layout()
    plt.show()
