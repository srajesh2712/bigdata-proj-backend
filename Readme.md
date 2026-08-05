# Demonstration 

# Scenes Taken 
4 - S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE
6 - S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE
5 - S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.SAFE
7 - S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE

# Querries 

## Reset data 
update sar.processing_job set job_status = 'CANCELLED' where job_status = 'CREATED';

SELECT * FROM sar.sar_scene_master ORDER BY scene_id ASC ;
select * from sar.processing_job order by job_id desc limit 20 ;
select * from sar.processing_artifacts order by artifact_id limit 10;



## AOI  Sorted by largest to smallest 
-- Inserting records into the processing_job table for the AOIs

POLYGON((
-3.65 56.35,
-3.25 56.35,
-3.25 56.75,
-3.65 56.75,
-3.65 56.35
))
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');


POLYGON((
-3.55 56.45,
-3.35 56.45,
-3.35 56.65,
-3.55 56.65,
-3.55 56.45
))

INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');


POLYGON((
-3.50 56.50,
-3.40 56.50,
-3.40 56.60,
-3.50 56.60,
-3.50 56.50
))
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');


## check file size in hdfs 
hdfs du -sh hdfs://namenode:8020/user/btcchl0040/spark_preprocessed/25/25_tile.zarr


## Remove a file from hdfs 
hdfs rm -rf hdfs://namenode:8020/user/btcchl0040/dask_preprocessed/29/29_tile.zarr


# Storage comparison 
Prerequisites - Execute the below commands to set the env variables . 
This folder contains the steps and command needed  

# 1. Point to your actual Java 21 installation
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 2. Point to your local Hadoop folder
export HADOOP_HOME=$HOME/Documents/summer-project/hadoop-3.4.1-bin

# 3. Tell PyArrow where the native HDFS library is
export ARROW_LIBHDFS_DIR=$HADOOP_HOME/lib/native

# 4. Link the JVM and Hadoop libraries (Critical for the .so loader)
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH

# 5. Generate the Classpath (This should work perfectly now)
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)

# 6. Set the Hadoop User
export HADOOP_USER_NAME=btcchl0040


## excecute the below python program 
 python sar_pipeline/b_storage/hdfs_size_comparison.py 