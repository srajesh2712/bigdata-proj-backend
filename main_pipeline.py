import random

from sar_pipeline.preprocessing.preprocess_sar import preprocess_sar_files
from sar_pipeline.preprocessing.split_geotiff_files import split_files

from datetime import datetime
import os
if __name__ == '__main__':

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # split files
    job_id=timestamp #'JOB_1'
    print("Job ID: ", job_id)

    # step 1 - preprocess the sar file using graph xml and gpt command from snap
    preprocess_sar_files(job_id)

    # step 2 - split the preprocessed files into chunks
    sar_folders = ['S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE',
                   'S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE']
    base_dir = '/home/btcchl0040/Documents/SAR_Data'


    for file in sar_folders:
        processed_geotiff= os.path.join(base_dir,f'{job_id}', 'PREPROCESSING',file , f'{file}_{job_id}.tif')
        #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f'processed_geotiff  {file}','\n')

        parent_folder = os.path.dirname(processed_geotiff)
        output_dir = os.path.join(parent_folder, 'SPLIT')
        print(' Tiles created at  ',output_dir,'\n')
        split_files(processed_geotiff, output_dir)
