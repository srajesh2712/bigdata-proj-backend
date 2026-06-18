This folder contains spark based solution for preprocessing 


Spark based - command to execute 

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 2G   --executor-memory 6G   --executor-cores 4   /opt/spark-jobs/preprocessing/preprocess_spark_db.py" 


docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 2G   --executor-memory 6G   --executor-cores 4   /opt/spark-jobs/preprocessing/preprocess_spark_db.py" 

Dask based 

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocessing_dask.py

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocess_dask_db.py



docker exec -it dask-client python /opt/spark-jobs/processing/zar_tif.py
