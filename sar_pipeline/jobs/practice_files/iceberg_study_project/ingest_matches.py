from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("StatsBomb-Matches-Full-Ingestion") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # 1. Load and Flatten as before
    matches_path = "/opt/spark/data/matches"
    raw_matches = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(matches_path)
    
    # 2. Flattening (Keep your existing select logic here)
    full_matches = raw_matches.select(
        F.col("match_id").cast("long"),
        F.to_date("match_date").alias("match_date"),
        F.col("kick_off"),
        F.col("competition.competition_id").cast("int").alias("competition_id"),
        F.col("competition.competition_name"),
        F.col("competition.country_name").alias("competition_country"),
        F.col("season.season_id").cast("int").alias("season_id"),
        F.col("season.season_name"),
        F.col("home_team.home_team_id").cast("int").alias("home_team_id"),
        F.col("home_team.home_team_name").alias("home_team"),
        F.col("home_team.home_team_gender").alias("home_team_gender"),
        F.col("home_team.home_team_group").alias("home_team_group"),
        F.col("home_team.country.name").alias("home_team_country"),
        F.col("home_team.country.id").alias("home_team_country_id"),
        F.col("home_team.managers").alias("home_managers"),
        F.col("away_team.away_team_id").cast("int").alias("away_team_id"),
        F.col("away_team.away_team_name").alias("away_team"),
        F.col("away_team.away_team_gender").alias("away_team_gender"),
        F.col("away_team.away_team_group").alias("away_team_group"),
        F.col("away_team.country.name").alias("away_team_country"),
        F.col("away_team.country.id").alias("away_team_country_id"),
        F.col("away_team.managers").alias("away_managers"),
        F.col("home_score").cast("int").alias("home_score"),
        F.col("away_score").cast("int").alias("away_score"),
        F.col("match_status"),
        F.col("match_status_360"),
        F.to_timestamp("last_updated").alias("last_updated"),
        F.to_timestamp("last_updated_360").alias("last_updated_360"),
        F.col("metadata.data_version").alias("data_version"),
        F.col("metadata.shot_fidelity_version").alias("shot_fidelity_version"),
        F.col("metadata.xy_fidelity_version").alias("xy_fidelity_version"),
        F.col("match_week").cast("int").alias("match_week"),
        F.col("competition_stage.id").alias("stage_id"),
        F.col("competition_stage.name").alias("stage_name"),
        F.col("stadium.id").cast("int").alias("stadium_id"),
        F.col("stadium.name").alias("stadium_name"),
        F.col("stadium.country.name").alias("stadium_country"),
        F.col("stadium.country.id").alias("stadium_country_id"),
        F.col("referee.id").cast("int").alias("referee_id"),
        F.col("referee.name").alias("referee_name"),
        F.col("referee.country.name").alias("referee_country"),
        F.col("referee.country.id").alias("referee_country_id")
    )

    table_name = "local.football.matches"
    
    if spark.catalog.tableExists(table_name):
        print(f"Table exists. Forcing Column Alignment...")
        
        # A. Add columns that don't exist yet
        existing_cols = spark.read.table(table_name).columns
        for field in full_matches.schema:
            if field.name not in existing_cols:
                print(f"Adding {field.name} to Iceberg...")
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
        
        # B. REFRESH is mandatory here
        spark.catalog.refreshTable(table_name)
        
        # C. THE FIX: Select columns in the EXACT order they exist in the target table
        # This solves the 'kick_off out of order' issue permanently.
        actual_table_columns = spark.read.table(table_name).columns
        aligned_df = full_matches.select(*actual_table_columns)
        
        # D. Write with Name-based mapping fallback
        aligned_df.repartition(1).write \
            .format("iceberg") \
            .mode("overwrite") \
            .save(table_name)
            
    else:
        full_matches.writeTo(table_name).partitionedBy("competition_name").create()

    print(f"Done! Final count: {spark.table(table_name).count()}")

if __name__ == "__main__":
    main()
