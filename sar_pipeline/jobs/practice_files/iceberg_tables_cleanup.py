from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import traceback


spark = SparkSession.builder \
        .appName("StatsBomb-Full-Events-Ingestion") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
 
from datetime import datetime, timedelta

table_name = "local.football.events"
# We'll use the raw name for the internal argument
raw_table_name = "football.events" 

# Set the timestamp for 1 minute in the future to bypass the 3-day grace period
future_ts = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')

print(f"Force-cleaning orphans older than {future_ts}...")

# Use the catalog.system.procedure(table => '...') syntax
spark.sql(f"""
    CALL local.system.remove_orphan_files(
        table => '{raw_table_name}',
        older_than => TIMESTAMP '{future_ts}'
    )
""").show()
