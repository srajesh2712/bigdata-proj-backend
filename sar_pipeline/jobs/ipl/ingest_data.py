import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, lit, expr, explode_outer

# 1. Spark Session
spark = SparkSession.builder \
    .appName("T20_Always_Append_Ingestion") \
    .getOrCreate()

source_dir = "/opt/spark/data/ipl/"
all_files = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.endswith('.json')]

batch_size = 50
is_first_batch = True

for i in range(0, len(all_files), batch_size):
    batch_files = all_files[i:i + batch_size]
    print(f"Processing Batch {i // batch_size + 1}...")

    # 2. Load JSON
    raw_df = spark.read.option("multiLine", "true").json(batch_files)

    # --- TABLE 1: MATCH INFO ---
    match_df = raw_df.select(
        to_date(col("info.dates")[0]).alias("date"),
        col("info.match_type"),
        col("info.teams")[0].alias("team_a"),
        col("info.teams")[1].alias("team_b"),
        col("info.venue"),
        col("info.outcome.winner").alias("winner")
    )

    # --- TABLE 2: PLAYER LINEUPS ---
    players_raw_df = raw_df.select(
        to_date(col("info.dates")[0]).alias("date"),
        col("info.players")
    )
    team_cols = players_raw_df.select("players.*").columns
    stack_expr = f"stack({len(team_cols)}, " + ", ".join(
        [f"'{c}', players.`{c}`" for c in team_cols]) + ") as (team_name, player_list)"

    players_df = players_raw_df.select("date", expr(stack_expr)).select(
        "date", "team_name", explode("player_list").alias("player_name")
    )

    # --- TABLE 3: BALL STATS (Flattened with Wickets) ---
    ball_df = raw_df.select(
        to_date(col("info.dates")[0]).alias("date"),
        explode("innings").alias("inn")
    ).select(
        "date", col("inn.team").alias("batting_team"), explode("inn.overs").alias("ovr")
    ).select(
        "date", "batting_team", col("ovr.over").alias("over_num"), explode("ovr.deliveries").alias("del")
    ).select(
        "date", "batting_team", "over_num",
        col("del.batter").alias("player_name"),
        col("del.runs.batter").alias("runs_scored"),
        col("del.bowler").alias("bowler_name"),
        col("del.runs.total").alias("total_runs"),
        col("del.wickets").alias("w_array")
    ).select(
        "*", explode_outer("w_array").alias("w")
    ).select(
        "date", "batting_team", "over_num", "player_name", "runs_scored",
        "bowler_name", "total_runs",
        col("w.kind").alias("wickets_kind"),
        col("w.player_out").alias("player_out")
    )

    # 3. Write to Iceberg
    tables = {
        "local.ipl.matches": match_df,
        "local.ipl.players": players_df,
        "local.ipl.ball_stats": ball_df
    }

    for table_name, df in tables.items():
        # Prüfen, ob die Tabelle ÜBERHAUPT schon existiert
        if spark.catalog.tableExists(table_name):
            # Wenn sie existiert: Einfach nur anhängen
            print(f"Appending to {table_name}...")
            df.writeTo(table_name).append()
        else:
            # Nur wenn die Tabelle brandneu ist: Erstellen
            print(f"Creating table {table_name}...")
            df.writeTo(table_name).partitionedBy("date").create()
    # Flip the flag after the first 50 files
    is_first_batch = False

print("Ingestion completed using Append Mode.")