This folder contains spark based solution for preprocessing 


Spark based - command to execute 

 docker exec -it -u root spark-submit bash -c "
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 1 \
  /opt/spark-jobs/preprocessing/preprocess.py"
  
  
  
Dask based 

docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocessing_dask.py




