from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb-Competitions-Ingestion") \
        .getOrCreate()

    # 1. Define Explicit Schema (Standard Practice)
    schema = StructType([
        StructField("competition_id", IntegerType(), True),
        StructField("season_id", IntegerType(), True),
        StructField("country_name", StringType(), True),
        StructField("competition_name", StringType(), True),
        StructField("competition_gender", StringType(), True),
        StructField("season_name", StringType(), True),
        StructField("match_updated", StringType(), True),
        StructField("match_available", StringType(), True)
    ])

    json_path = "/opt/spark/data/competitions.json"
    
    # 2. Read with Schema
    raw_df = spark.read \
        .option("multiLine", "true") \
        .schema(schema) \
        .json(json_path)

    # 3. Transform with explicit casting
    clean_df = raw_df.select(
        "competition_id",
        "season_id",
        "country_name",
        "competition_name",
        "competition_gender",
        "season_name",
        F.to_timestamp("match_updated").alias("last_updated"),
        "match_available"
    ).dropDuplicates(["competition_id", "season_id"]) # Ensure uniqueness

    # 4. Standard Iceberg Write Pattern
    table_name = "local.football.competitions"
    
    # partitioningBy is standard for Big Data tables
    clean_df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .tableProperty("write.format.default", "parquet") \
        .partitionedBy("competition_gender") \
        .createOrReplace()

    print(f"--- Ingestion Complete: {table_name} ---")
    spark.table(table_name).show(5)

if __name__ == "__main__":
    main()
