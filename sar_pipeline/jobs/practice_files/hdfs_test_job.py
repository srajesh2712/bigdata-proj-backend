from pyspark.sql import SparkSession
import sys

# Define the HDFS URI using the NameNode's service hostname
# We use the NameNode's RPC port 8020
HDFS_NN_HOST = "hadoop-namenode"
HDFS_NN_PORT = "8020"
HDFS_URI = f"hdfs://{HDFS_NN_HOST}:{HDFS_NN_PORT}"

# Define the path to the file you previously uploaded
# NOTE: Replace 'tile_0_0.tif' with a path that definitely exists in your HDFS.
HDFS_FILE_PATH = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/output_flood_mask.tif"

def main():
    """
    Initializes Spark, reads the HDFS file, and prints the line count.
    A successful execution confirms both network connectivity AND application-level configuration.
    """
    try:
        spark = SparkSession.builder \
            .appName("HDFS_Connectivity_Test") \
            .getOrCreate()
        
        # Log the connection details
        print(f"--- Spark Session initialized. Testing HDFS URI: {HDFS_URI} ---")
        
        # Load the file from HDFS
        # We read it as a simple text file for the test
        hdfs_rdd = spark.sparkContext.textFile(HDFS_URI + HDFS_FILE_PATH)
        
        # Perform an action (count) to force Spark to execute the read operation
        count = hdfs_rdd.count()

        print(f"\n--- SUCCESS! File found and accessed! ---")
        print(f"File Path: {HDFS_FILE_PATH}")
        print(f"Total lines/rows counted: {count}")
        print(f"--- Connectivity to HDFS is Confirmed. ---")
        
        spark.stop()

    except Exception as e:
        print("\n--- FAILURE: HDFS Connection or File Access Error ---")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {e}")
        print(f"Possible Cause: If the error mentions 'Connection refused', 'UnknownHostException', or 'No such file or directory', the network bridge is likely still broken.")
        sys.exit(1)

if __name__ == "__main__":
    main()

