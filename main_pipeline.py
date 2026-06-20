import random

from sar_pipeline.analysis.create_flood_mask import create_flood_mask,create_mask
from sar_pipeline.analysis.merge_flooded_tiles import merge_flooded_tiles
from sar_pipeline.db import fetch_processing_files, update_processing_files_by_jobid, insert_job
from sar_pipeline.a_preprocessing.preprocess_standalone import preprocess_sar_files
from sar_pipeline.a_preprocessing.split_geotiff_files import split_files
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://rajesh:rajesh@localhost/eo")
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
from sar_pipeline.schema.schema import SafeFile, Base
from datetime import datetime
import os
base_dir = '/home/btcchl0040/Documents/SAR_Data'

if __name__ == '__main__':
    message = 'step1'
    pending_files = fetch_processing_files(session)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = insert_job('started');
    #job_id = timestamp  # 'JOB_1'
    print("Job ID: ", job_id)
    if message == 'step1':

        '''
        step 1 - preprocess the sar file using graph xml and gpt command from snap
        Series of steps have to be applied for a_preprocessing
        fetching all the folder names which are not preprocessed and pre process the same 
        '''
        starttime = datetime.now().strftime("%Y%m%d_%H%M%S")
        preprocess_sar_files(job_id, pending_files)
        stoptime = datetime.now().strftime("%Y%m%d_%H%M%S")
        update_processing_files_by_jobid(session,job_id)
        print('Standalone starting',starttime)
        print('standalone stopping',stoptime)
    elif message == 'step3':
        # step 3 - create mask by comparing each tile and form a tile mask
        pre_folder = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/SPLIT'
        post_folder = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/SPLIT'
        job_id = '20250803_163826'
        create_flood_mask(pre_folder,post_folder,job_id)
