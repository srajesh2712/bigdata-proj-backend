from pyspark.sql import SparkSession
import os

# 1. Paths
conda_python = '/home/btcchl0040/miniconda3/envs/cnn_study/bin/python'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-21-openjdk-amd64'
os.environ['PYSPARK_PYTHON'] = conda_python
os.environ['PYSPARK_DRIVER_PYTHON'] = conda_python

# 2. Builder - Switch to local[*] and Spark 3.5 compatible JAR
spark = SparkSession.builder.appName("IcebergCareerProject") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/home/btcchl0040/Documents/git-contribution/lakehouse-db") \
    .master("local[*]") \
    .getOrCreate()

print("\n--- Spark 3.5 + Iceberg Session Started ---\n")

# 3. Create Table
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.career_db")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.career_db.history (
    event_id BIGINT,
    company STRING,
    role STRING,
    event_date DATE
) USING iceberg
""")

print("Table 'history' is ready.")
spark.sql("SHOW TABLES IN local.career_db").show()

spark.stop()