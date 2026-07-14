
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator





# 1. Start Spark
spark = SparkSession.builder \
    .appName("Village Flood Mapping") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .getOrCreate()

# Register Sedona functions
SedonaRegistrator.registerAll(spark)



# 2. Load village shapefile or GeoJSON
village_df = spark.read.format("geojson").load("ASSAM_VILLAGES.geojson")  # or "shapefile"
print(village_df)
print('loaded map ')