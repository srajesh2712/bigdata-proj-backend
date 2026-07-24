from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb-Full-Events-Ingestion") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    matches_df = spark.table("local.football.matches")
    competitions = [row['competition_name'] for row in matches_df.select("competition_name").distinct().collect()]
    
    table_name = "local.football.events"

    for comp in competitions:
        print(f"\n--- Processing Competition: {comp} ---")
        match_ids_rows = [str(row['match_id']) for row in matches_df.filter(F.col("competition_name") == comp).select("match_id").collect()]
        
         
        
        paths = [f"/opt/spark/data/events/{mid}.json" for mid in match_ids_rows]
        print(f"\n--- Processing : {paths} ---")
        try:
            # multiLine is required for StatsBomb JSON structure
            raw_events = spark.read.option("multiLine", "true").json(paths)
            
            # 1. EXTRACT EVERY FIELD FROM YOUR JSON
            clean_events = raw_events.withColumn("source_file", F.input_file_name()) \
                .select(
                    F.col("id").alias("event_id"),
                    F.col("index").cast("int"),
                    F.col("period").cast("int"),
                    F.col("timestamp"),
                    F.col("minute").cast("int"),
                    F.col("second").cast("int"),
                    
                    # Type Object
                    F.col("type.id").cast("int").alias("type_id"),
                    F.col("type.name").alias("type_name"),
                    
                    # Possession & Team Data
                    F.col("possession").cast("int"),
                    F.col("possession_team.id").cast("int").alias("possession_team_id"),
                    F.col("possession_team.name").alias("possession_team_name"),
                    F.col("play_pattern.id").cast("int").alias("play_pattern_id"),
                    F.col("play_pattern.name").alias("play_pattern_name"),
                    F.col("team.id").cast("int").alias("team_id"),
                    F.col("team.name").alias("team_name"),
                    
                    # Duration & Tactics (Lineup is an Array of Structs)
                    F.col("duration").cast("double"),
                    F.col("tactics.formation").cast("int").alias("formation"),
                    F.col("tactics.lineup").alias("lineup"), 
                    
                    # Metadata & Join Keys
                    F.regexp_extract("source_file", r'(\d+)\.json$', 1).cast("long").alias("match_id")
                )

            # 2. SCHEMA EVOLUTION & ALIGNMENT
            if spark.catalog.tableExists(table_name):
                print(f"Aligning schema for {comp}...")
                
                # Check for new columns and ADD via DDL
                existing_cols = spark.read.table(table_name).columns
                for field in clean_events.schema:
                    if field.name not in existing_cols:
                        print(f"Adding new column: {field.name}")
                        spark.sql(f"ALTER TABLE {table_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
                
                spark.catalog.refreshTable(table_name)
                
                # PHYSICAL REORDERING: The fix for "Out of Order" errors
                actual_table_columns = spark.read.table(table_name).columns
                final_cols = []
                for col_name in actual_table_columns:
                    if col_name in clean_events.columns:
                        final_cols.append(F.col(col_name))
                    else:
                        # This prevents the UNRESOLVED_COLUMN error
                        print(f"Column {col_name} missing in JSON, filling with NULL")
                        final_cols.append(F.lit(None).alias(col_name))
                aligned_df = clean_events.select(*final_cols)
                
                # Write to the existing table repartition(1).
                aligned_df.repartition(2).sort("match_id", "type_name").write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(table_name)
            else:
                # First time creation
                print(f"Creating new Events table: {table_name}")
                clean_events.writeTo(table_name) \
                    .tableProperty("format-version", "2") \
                    .partitionedBy("match_id") \
                    .create()

            print(f"--- Successfully Ingested {comp} ---")
            
        except Exception as e:
            print(f"Skipping {comp} or Error: {str(e)}")

    print("\n--- Ingestion Job Complete ---")

if __name__ == "__main__":
    main()
