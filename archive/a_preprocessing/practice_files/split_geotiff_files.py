import rasterio
from rasterio.windows import Window
import os
from datetime import datetime
from archive.db import insert_start_time, update_end_time


def split_files(input_tif,output_dir):
    log_id, start = insert_start_time('SPLIT_GEOTIFF_FILES')
    print(f"Started at {start}")
    tile_size = 2048  # pixels

    os.makedirs(output_dir, exist_ok=True)

    with rasterio.open(input_tif) as src:
        width = src.width
        height = src.height

        for i in range(0, width, tile_size):
            for j in range(0, height, tile_size):
                window = Window(i, j, tile_size, tile_size)
                transform = src.window_transform(window)
                out_profile = src.profile.copy()
                out_profile.update({
                    "height": tile_size,
                    "width": tile_size,
                    "transform": transform
                })

                tile_filename = f"{output_dir}/tile_{j}_{i}.tif"
                with rasterio.open(tile_filename, "w", **out_profile) as dest:
                    dest.write(src.read(window=window))
    end = update_end_time(log_id)
    print(f"Ended at {end}")
if __name__ == '__main__':
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/'
    input_tif = '/home/btcchl0040/Documents/SAR_Data/OUTPUT/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250802_151438.tif'
    output_dir =os.path.join(base_dir,f'split/{timestamp}')
    split_files(input_tif,output_dir)
