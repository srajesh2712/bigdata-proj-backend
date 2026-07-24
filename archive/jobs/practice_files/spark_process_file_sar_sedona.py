from pyspark.sql import SparkSession
from sedona.spark import *

from pyspark.sql.functions import expr

def main():
    # 1. Initialize Spark with Sedona Configuration
    # Sedona requires specific serializers to handle spatial data across the cluster
    config = (SedonaContext.builder() \
        .appName("Low_RAM_SAR_Flood") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.sources.commitProtocolClass","org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.sql.sources.default", "geotiff") \
        .config("sedona.global.index", "true") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020").getOrCreate())

    # Register Sedona Functions
    sedona = SedonaContext.create(config)

    raster_format = "org.apache.sedona.spark.raster.DefaultSource"

    # 2. Define Paths
    pre_path = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
    post_path = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"
    output_path = "/user/btcchl0040/sar/results/masks_sedona"

    # 3. Load Rasters
    # Sedona's GeoTiff loader automatically handles the tiling and distribution
    # We load them and select the first band (index 0)
    # Load the whole file as a binary blob (this is fine, it stays on disk/buffer)

    # Convert to Raster and IMMEDIATELY break into 512x512 windows
    pre_tiled_df = sedona.read.format("binaryFile").load(pre_path) \
        .selectExpr("RS_FromGeoTiff(content) as raw_rast") \
        .selectExpr("RS_TileExplode(raw_rast, 512, 512)") \
        .selectExpr("tile as pre_rast", "x", "y")

    post_tiled_df = sedona.read.format("binaryFile").load(post_path) \
        .selectExpr("RS_FromGeoTiff(content) as raw_rast") \
        .selectExpr("RS_TileExplode(raw_rast, 512, 512)") \
        .selectExpr("tile as post_rast", "x", "y")


    joined_df = pre_tiled_df.alias("pre").join(
        post_tiled_df.alias("post"),
        expr("ST_Intersects(RS_Envelope(pre.pre_rast), RS_Envelope(post.post_rast))")
    )
    # 4. Spatial Join
    # This aligns the "pre" and "post" tiles based on their geographic location
    # even if the files aren't perfectly cropped to the same size

    # 5. Apply Map Algebra (The Math)
    # We use a single SQL expression to:
    # - Clip values to 1e-6 (avoid log of zero)
    # - Convert to dB: 10 * log10(val)
    # - Calculate difference and apply the -5.5 threshold
    # The result is a binary mask (1 for flood, 0 for no flood)

    flood_df = joined_df.selectExpr(
        """
        RS_MapAlgebra(
            pre_rast, post_rast, NULL,
            '10 * log10(max(b0, 0.000001)) - 10 * log10(max(b1, 0.000001)) < -5.5 ? 1 : 0',
            NULL
        ) AS flood_mask
        """
    )

    # 6. Calculate Total Flood Pixels
    # Instead of manual sums, use Sedona's optimized band summation

    stats_df = flood_df.selectExpr("RS_SummaryStats(flood_mask,'sum',0, true) as stats")
    result = stats_df.first()[0]()
    total_pixels = result[0][0] if result and result[0][0] is not None else 0
    print(f"\n--- FLOOD DETECTION RESULTS ---")
    print(f"TOTAL FLOOD PIXELS: {int(total_pixels)}")
    print(f"--------------------------------\n")

    # 7. Save results back to HDFS as a GeoTiff
    # This saves the actual spatial mask, which you can open in QGIS
    flood_df.write.format("geotiff").save(output_path)
# Save as GeoParquet for maximum efficiency on low-spec hardware
    flood_df.write.format("geoparquet").save(output_path)

    config.stop()

if __name__ == "__main__":
    main()
