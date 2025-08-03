import random

from sar_pipeline.analysis.create_flood_mask import create_flood_mask
from sar_pipeline.preprocessing.preprocess_sar import preprocess_sar_files
from sar_pipeline.preprocessing.split_geotiff_files import split_files
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://rajesh:rajesh@localhost/eo")
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
from sar_pipeline.schema.schema import SafeFile, Base
from datetime import datetime
import os
def fetch_processing_files():
    pending_files = session.query(SafeFile).filter_by(active=True, status='pending').all()
    return pending_files
if __name__ == '__main__':
    message = 'step3'

    if message == 'step1':
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # split files
        job_id=timestamp #'JOB_1'
        print("Job ID: ", job_id)


        pending_files = fetch_processing_files()
        # step 1 - preprocess the sar file using graph xml and gpt command from snap
        preprocess_sar_files(job_id, pending_files)
    elif message == 'step2':
        # step 2 - split the preprocessed files into chunks

        base_dir = '/home/btcchl0040/Documents/SAR_Data'


        for file in pending_files:
            folder_name = file.folder_path
            processed_geotiff= os.path.join(base_dir,f'{job_id}', 'PREPROCESSING',file , f'{file}_{job_id}.tif')

            print(f'processed_geotiff  {file}','\n')

            parent_folder = os.path.dirname(processed_geotiff)
            output_dir = os.path.join(parent_folder, 'SPLIT')
            print(' Tiles created at  ',output_dir,'\n')
            split_files(processed_geotiff, output_dir)
    elif message == 'step3':
        # step 3 - create mask by comparing each tile and form a tile mask
        pre_folder = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/SPLIT'
        post_folder = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/SPLIT'
        job_id = '20250803_163826'
        create_flood_mask(pre_folder,post_folder,job_id)