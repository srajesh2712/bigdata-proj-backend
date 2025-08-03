from sar_pipeline.preprocessing.split_geotiff_files import split_files
from datetime import datetime
import os
if __name__ == '__main__':
    # split files
    sar_folders = ['S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE'
                   'S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE']
    base_dir = '/home/btcchl0040/Documents/SAR_Data'
    dir_list = ['OUTPUT/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250802_151438.tif',
                'OUTPUT/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250802_151542.tif'
                ]


    for file in sar_folders:
        processed_geotiff= os.path.join(base_dir, file , 'OUTPUT', file, f)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_path = os.path.join(base_dir,files)

        parent_folder = os.path.dirname(input_path)
        output_dir = os.path.join(parent_folder, 'split', f'{timestamp}')
        print(' Tiles created at  ',output_dir)
        split_files(os.path.join(base_dir,files), output_dir)
