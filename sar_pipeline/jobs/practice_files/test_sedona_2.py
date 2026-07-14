# test_spatial_predicates.py
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# Initialize Spark session with Sedona configs
spark = SparkSession.builder \
    .appName("SedonaSpatialPredicates") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .getOrCreate()

# Register Sedona SQL functions
SedonaRegistrator.registerAll(spark)

# === Sample flood polygon and point data ===

# A sample rectangular flood area
flood_data = [
    ("f1", "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))")  # square from (0,0) to (5,5)
]

# A few test points
point_data = [
    ("p1", "POINT (1 1)"),  # inside
    ("p2", "POINT (5 5)"),  # on boundary
    ("p3", "POINT (6 6)"),  # outside
    ("p4", "POINT (2 2)")   # inside
]

# Create DataFrames
flood_df = spark.createDataFrame(flood_data, ["id", "wkt"])
points_df = spark.createDataFrame(point_data, ["id", "wkt"])

# Convert WKT to geometry
flood_df.createOrReplaceTempView("flood")
points_df.createOrReplaceTempView("points")

spark.sql("SELECT id, ST_GeomFromWKT(wkt) AS geom FROM flood").createOrReplaceTempView("flood_geom")
spark.sql("SELECT id, ST_GeomFromWKT(wkt) AS geom FROM points").createOrReplaceTempView("points_geom")

# === Perform spatial predicates ===

# ST_Within (point is within polygon)
print("\n== ST_Within (point inside flood zone) ==")
spark.sql("""
    SELECT p.id AS point_id, f.id AS flood_id
    FROM points_geom p, flood_geom f
    WHERE ST_Within(p.geom, f.geom)
""").show()

# ST_Contains (flood zone contains the point — same as ST_Within but reverse args)
print("\n== ST_Contains (flood contains point) ==")
spark.sql("""
    SELECT f.id AS flood_id, p.id AS point_id
    FROM flood_geom f, points_geom p
    WHERE ST_Contains(f.geom, p.geom)
""").show()

# ST_Intersects (boundary/inside overlaps)
print("\n== ST_Intersects (any overlap) ==")
spark.sql("""
    SELECT f.id AS flood_id, p.id AS point_id
    FROM flood_geom f, points_geom p
    WHERE ST_Intersects(f.geom, p.geom)
""").show()

# Stop the session
spark.stop()

