import random

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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # split files
    job_id=timestamp #'JOB_1'
    print("Job ID: ", job_id)


    pending_files = fetch_processing_files()
    # step 1 - preprocess the sar file using graph xml and gpt command from snap
    preprocess_sar_files(job_id, pending_files)

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
