This folder contains the steps and command needed  

# main program 
python main.py 

# Spark based - command to execute 

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 1G   --executor-memory 6G   --executor-cores 2   /opt/spark-jobs/preprocessing/preprocess_spark_db.py" 

# Dask based 

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocess_dask_db.py



docker exec -it dask-client python /opt/spark-jobs/processing/zar_tif.py

# Downloading file from Hadoop to local
hdfs get hdfs://localhost:8020/user/btcchl0040/spark_preprocessed/8/40_tile.tif 40_tile_spark_preprocessed.tif


