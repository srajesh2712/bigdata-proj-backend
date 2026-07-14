# test_sedona.py
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# Start Spark Session with Sedona config
spark = SparkSession.builder \
    .appName("SedonaTest") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .getOrCreate()

# Register Sedona functions
SedonaRegistrator.registerAll(spark)

# Sample WKT data
wkt_data = [("1", "POINT (1 1)"), ("2", "POINT (2 2)"), ("3", "POINT (3 3)")]

# Create DataFrame
df = spark.createDataFrame(wkt_data, ["id", "wkt"]) \
    .withColumnRenamed("wkt", "geom")

# Register as temp view and use Sedona SQL
df.createOrReplaceTempView("spatial_df")
spark.sql("SELECT ST_GeomFromWKT(geom) AS geometry FROM spatial_df").show(truncate=False)

spark.stop()

