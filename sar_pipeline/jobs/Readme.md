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


# Preprocessing 
## pysnap 
python main.py 

## Spark based - command to execute 

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 1G   --executor-memory 6G   --executor-cores 2   /opt/spark-jobs/preprocessing/preprocess_spark_db.py" 


## Dask based 

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocess_dask_db.py



docker exec -it dask-client python /opt/spark-jobs/processing/zar_tif.py

# Downloading file from Hadoop to local
hdfs get hdfs://localhost:8020/user/btcchl0040/spark_preprocessed/8/40_tile.tif 40_tile_spark_preprocessed.tif

# Processing 
docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 1G   --executor-memory 6G   --executor-cores 2   /opt/spark-jobs/spark_process_file_sar.py"

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 1G   --executor-memory 6G   --executor-cores 2   /opt/spark-jobs/spark_process_zarr.py"


docker exec -it dask-client python /opt/spark-jobs/dask_process_file_sar.py

docker exec -it dask-client python /opt/spark-jobs/dask_process_zarr.py
