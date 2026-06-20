from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import traceback

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb-Full-Events-Ingestion") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    table_name = "local.football.events"
    events_path = "/opt/spark/data/events"

    # 1. Read raw JSON
    raw_events = spark.read.option("multiLine", "true") \
                           .option("recursiveFileLookup", "true") \
                           .json(events_path)

    try:
        # 2. Extract & process (Keeping lineup as the original complex object)
        processed_df = raw_events.withColumn("match_id_str", F.regexp_extract(F.input_file_name(), r'(\d+)\.json$', 1)) \
            .select(
                F.col("id").cast("string").alias("source_id"),
                F.col("match_id_str").cast("string").alias("source_match_id"),
                F.col("index").cast("int"),
                F.col("period").cast("int"),
                F.col("timestamp").cast("string"),
                F.col("minute").cast("int"),
                F.col("second").cast("int"),
                F.col("type.id").cast("int").alias("type_id"),
                F.col("type.name").alias("type_name"),
                F.col("location").getItem(0).cast("double").alias("x"),
                F.col("location").getItem(1).cast("double").alias("y"),
                F.col("possession").cast("int"),
                F.col("possession_team.id").cast("int").alias("possession_team_id"),
                F.col("possession_team.name").alias("possession_team_name"),
                F.col("play_pattern.id").cast("int").alias("play_pattern_id"),
                F.col("play_pattern.name").alias("play_pattern_name"),
                F.col("team.id").cast("int").alias("team_id"),
                F.col("team.name").alias("team_name"),
                F.col("duration").cast("double"),
                F.col("tactics.formation").cast("int").alias("formation"),
                F.col("tactics.lineup").alias("lineup") # Removed F.to_json()
            )

        # 3. Circuit Breaker
        print("Breaking lineage by collecting data to Driver...")
        local_data = processed_df.collect()
        
        if not local_data:
            print("No data found.")
            return

        static_source = spark.createDataFrame(local_data, processed_df.schema)
        static_source.createOrReplaceTempView("incoming_data")
        
        print(f"Prepared {len(local_data)} rows for merging...")

        if spark.catalog.tableExists(table_name):
            print(f"Merging into Iceberg: {table_name}")
            spark.sql(f"""
                MERGE INTO {table_name} AS t
                USING incoming_data AS s
                ON t.id = s.source_id AND t.match_id = s.source_match_id
                WHEN MATCHED THEN
                    UPDATE SET
                        t.index = s.index,
                        t.period = s.period,
                        t.timestamp = s.timestamp,
                        t.minute = s.minute,
                        t.second = s.second,
                        t.type_id = s.type_id,
                        t.type_name = s.type_name,
                        t.x = s.x,
                        t.y = s.y,
                        t.possession = s.possession,
                        t.possession_team_id = s.possession_team_id,
                        t.possession_team_name = s.possession_team_name,
                        t.play_pattern_id = s.play_pattern_id,
                        t.play_pattern_name = s.play_pattern_name,
                        t.team_id = s.team_id,
                        t.team_name = s.team_name,
                        t.duration = s.duration,
                        t.formation = s.formation,
                        t.lineup = s.lineup
                WHEN NOT MATCHED THEN
                    INSERT (
                        id, index, period, timestamp, minute, second, 
                        type_id, type_name, x, y, possession, possession_team_id, 
                        possession_team_name, play_pattern_id, play_pattern_name, 
                        team_id, team_name, duration, formation, lineup, match_id
                    )
                    VALUES (
                        s.source_id, s.index, s.period, s.timestamp, s.minute, s.second, 
                        s.type_id, s.type_name, s.x, s.y, s.possession, s.possession_team_id, 
                        s.possession_team_name, s.play_pattern_id, s.play_pattern_name, 
                        s.team_id, s.team_name, s.duration, s.formation, s.lineup, s.source_match_id
                    )
            """)
            print("--- Merge Successful ---")
        else:
            print(f"Creating new table: {table_name}")
            static_source.withColumnRenamed("source_id", "id") \
                         .withColumnRenamed("source_match_id", "match_id") \
                         .writeTo(table_name) \
                         .tableProperty("format-version", "2") \
                         .partitionedBy("match_id","type_name") \
                         .create()

    except Exception as e:
        print(f"Job Failed: {e}")
        traceback.print_exc()
if __name__ == "__main__":
    main()
