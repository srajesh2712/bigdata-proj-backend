https://documentation.dataspace.copernicus.eu/APIs/SentinelHub.html


//POINT(92.6603 26.0344)


standalone
python main_pipeline.py 



spark based 

docker exec -it -u root spark-submit bash -c "
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 1 \
  /opt/spark-jobs/preprocessing/preprocess.py"




dask based 
docker exec -it dask-client python /opt/spark-jobs/preprocessing/preprocessing_dask.py

