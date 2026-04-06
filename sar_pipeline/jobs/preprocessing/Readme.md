This folder contains spark based solution for preprocessing 


Spark based - command to execute 

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --driver-memory 512M   --executor-memory 2G   --executor-cores 1   /opt/spark-jobs/preprocessing/preprocess_spark.py" 
  

docker exec -it  spark-submit bash -c " /opt/spark/bin/spark-submit --jars /opt/spark-jars/postgresql-42.6.0.jar   --master spark://spark-master:7077   --driver-memory 512M   --executor-memory 2G   --executor-cores 1   /opt/spark-jobs/preprocessing/preprocess_spark_db.py" 

Dask based 

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocessing_dask.py

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocess_dask_db.py




