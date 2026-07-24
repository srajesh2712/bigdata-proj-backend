# ingest competition
docker exec -it spark-submit /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --driver-memory 512m   --executor-memory 512m   --conf spark.cores.max=1   /opt/spark-jobs/ingest_competitions.py


# ingest matches
docker exec -it spark-submit /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --driver-memory 512m   --executor-memory 512m   --conf spark.cores.max=1   /opt/spark-jobs/ingest_matches.py
