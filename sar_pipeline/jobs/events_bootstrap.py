from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

def bootstrap_table():
    spark = SparkSession.builder \
        .appName("StatsBomb-Table-Bootstrap") \
        .getOrCreate()

    # Define the schema explicitly to match your clean_events
    table_name = "local.football.events"
    
    # Create an empty DataFrame with the correct schema
    # Note: match_id is a string here because we extract it from the filename
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("index", LongType(), True),
        StructField("period", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("possession_team", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("match_id", StringType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)

    print(f"Creating empty Iceberg table: {table_name}")
    
    # Create the table with partitioning
    empty_df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .partitionedBy("match_id") \
        .createOrReplace()
    
    print("Table created successfully. You can now run the Incremental Script.")

if __name__ == "__main__":
    bootstrap_table()
