#
# PySpark job for distributed Flood Masking (Change Detection) using Sedona/GeoSpark.
#
# CRITICAL NOTE: The Python bindings for sedona.spark.raster appear to be missing 
# or corrupted in the container. We must bypass the Python wrapper and use the 
# Java bridge (spark._jvm) when implementing the full Sedona I/O logic.
#

from pyspark.sql import SparkSession
from pyspark import TaskContext
import os
from py4j.java_gateway import java_import
import time
import datetime
import numpy as np
import sys # New import for path manipulation

# --- CRITICAL RUNTIME FIX ---
# We keep the sys.path append for other modules, but the failing module is skipped.
PACKAGE_ROOT = "/usr/local/lib/python3.8/dist-packages"
try:
    sys.path.append(PACKAGE_ROOT)
except Exception:
    pass
# ----------------------------

# The imports that were failing should now work for the available modules:
from sedona.register import SedonaRegistrator 
from sedona.utils import SedonaKryoRegistrator, KryoSerializer 

# NOTE: The following import fails because the file is missing from the container:
# from sedona.spark.raster import RasterFileRDD 
#
# WORKAROUND: When reading the GeoTIFFs, you will need to access the Java class 
# directly using the Py4J bridge after SparkSession is created:
# E.g., java_raster_rdd = spark._jvm.org.apache.sedona.spark.raster.RasterFileRDD(spark._jsc)
# java_raster_rdd.read(HDFS_PRE_FLOOD_PATH)


# --- Configuration ---
HDFS_PRE_FLOOD_PATH = "hdfs://namenode:8020/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
HDFS_POST_FLOOD_PATH = "hdfs://namenode:8020/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

NUM_PARTITIONS = 42
SPARK_APP_NAME = "DistributedFloodMasking"
HDFS_URI = "hdfs://namenode:8020"
HDFS_FINAL_OUTPUT_PATH = "hdfs://namenode:8020/user/btcchl0040/sar/processed/flood-mask.tif"


# --- Utility Function for HDFS Check (omitted for brevity) ---
def check_hdfs_file_existence(spark, hdfs_path):
    """ Forces a connection to HDFS using the JVM bridge to verify the file exists. """
    try:
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        if not fs.exists(hadoop_path):
            raise FileNotFoundError(f"HDFS File Not Found: {hdfs_path}")
        print(f"--- HDFS Check OK: File found at {hdfs_path} ---")
        return True
    except Exception as e:
        print(f"--- FATAL HDFS CONNECTION/PATH ERROR ---")
        print(f"Error checking file existence for {hdfs_path}: {e}")
        raise

# --- Distributed Flood Masking Function (omitted for brevity) ---
def create_mask_spark(raster_pair):
    """ Core flood masking logic, executed in parallel by Spark Executors. """
    partition_id = raster_pair[0]
    pre_tile_data = raster_pair[1]
    post_tile_data = raster_pair[2]
    task_context = TaskContext.get()
    
    time.sleep(0.1) # Simulate processing time
    
    # Core processing logic (NumPy operations)
    pre_dB = 10 * np.log10(np.clip(pre_tile_data, 1e-6, None))
    post_dB = 10 * np.log10(np.clip(post_tile_data, 1e-6, None))
    
    diff_tile = post_dB - pre_dB
    mask_tile = (diff_tile < -5.5).astype(np.uint8)
    
    flood_pixel_count = np.sum(mask_tile)
    
    # NOTE: Sedona's ImageMosaicking expects the RDD key/value structure from the
    # Java layer. For now, we return the simulated key and the NumPy array.
    result_tile_for_mosaicking = (partition_id, mask_tile) 
    
    current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    window_shape = f"{mask_tile.shape[0]}x{mask_tile.shape[1]}"
    
    output_log = (
        f"[{current_time}] EXECUTOR: {task_context.taskAttemptId()} | "
        f"Window ID: {partition_id} | Shape: {window_shape} | "
        f"FLOOD PIXELS: {flood_pixel_count}"
    )
    
    return (result_tile_for_mosaicking, output_log, flood_pixel_count)


# --- Main Spark Application Logic ---
def process_windows(spark):
    """ Performs parallel processing and final mosaicking using the full execution flow. """
    try:
        # 1. HDFS FILE EXISTENCE CHECK (using placeholder paths)
        check_hdfs_file_existence(spark, HDFS_PRE_FLOOD_PATH)
        check_hdfs_file_existence(spark, HDFS_POST_FLOOD_PATH)
        
        # 2. RDD CREATION (TEMP SIMULATION)
        print(f"--- Proceeding with distributed flood masking on {NUM_PARTITIONS} windows ---")
        
        window_data = []
        for i in range(NUM_PARTITIONS):
            # Simulated data tiles
            pre_tile = np.random.randint(500, 1500, size=(1024, 1024), dtype=np.uint16)
            post_tile = np.random.randint(50, 1000, size=(1024, 1024), dtype=np.uint16)
            window_data.append((i, pre_tile, post_tile)) 
            
        rdd_to_map = spark.sparkContext.parallelize(window_data, numSlices=NUM_PARTITIONS)
        
        # 3. PARALLEL MASKING EXECUTION
        results_rdd = rdd_to_map.map(create_mask_spark)
        
        log_and_count_rdd = results_rdd.map(lambda x: (x[1], x[2]))
        # This RDD contains (partition_id, mask_tile) tuples
        result_tiles_rdd = results_rdd.map(lambda x: x[0]) 
        
        results = log_and_count_rdd.collect()
        total_flood_pixels = sum([count for log, count in results])
        
        print("\n--- Parallel Flood Masking Output (Collected from Executors) ---")
        for log, count in results:
            print(log)
        print("--------------------------------------------------------------------")
        print(f"TOTAL FLOOD PIXELS ACROSS ALL WINDOWS (Simulated): {total_flood_pixels}")
        
        
        # 4. REAL HDFS WRITE TEST (Implement Java Bridge Mosaicking)
        print("\n*** REAL I/O EXECUTION START (Mosaicking Write Test) ***")
        
        # --- CRITICAL PY4J DEBUG & FIX ---
        # 1. Import the class using java_import (optional, but good practice)
        java_import(spark._jvm, "org.apache.sedona.spark.operation.ImageMosaicking")
        
        # 2. Reverting to the most reliable explicit loader
        JavaImageMosaicking = spark._jvm.load_class("org.apache.sedona.spark.operation.ImageMosaicking")
        
        # DEBUG: Print the type and callable status to see why it keeps failing
        print(f"DEBUG CHECK 1: Java Class Object Type: {type(JavaImageMosaicking)}")
        print(f"DEBUG CHECK 2: Is Java Class Object Callable: {callable(JavaImageMosaicking)}")
        # ----------------------------------

        print(f"STEP 1: Attempting to call Java Bridge for Mosaicking Write...")
        
        # Convert the Python RDD of results back to a Java RDD handle.
        # This RDD contains Python objects (NumPy arrays) which will cause a 
        # serialization error (the NEXT expected error) when the Java method 
        # tries to process them, but it verifies the Java method can be called successfully.
        java_rdd_handle = result_tiles_rdd._jrdd.rdd() 

        # Perform the Mosaicking write using the Java class
        JavaImageMosaicking.mosaickToGeoTiff(
            java_rdd_handle, 
            HDFS_FINAL_OUTPUT_PATH, 
            True # overwrite
        )
        
        print(f"STEP 2: Mosaicking call successful (via Java Bridge). Check HDFS for final GeoTIFF output at:")
        print(f"-> {HDFS_FINAL_OUTPUT_PATH}")
        print("*** REAL I/O EXECUTION COMPLETE ***\n")

    except Exception as e:
        # Catch and print the error specifically for debugging the Java Bridge call
        print(f"\n--- JOB ABORTED ---")
        print(f"Reason: Failed during real Sedona Java Bridge call implementation. Error: {e}")


if __name__ == "__main__":
    
    HDFS_URI = "hdfs://namenode:8020"

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.hadoop.fs.defaultFS", HDFS_URI) \
        .getOrCreate()
    
    # Register Sedona components after SparkSession creation (crucial for linking Python to Java)
    try:
        SedonaRegistrator.registerAll(spark)
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        print(f"WARNING: Sedona registration failed. Check Java JARs and Python/Java bridge. Error: {e}")


    process_windows(spark)
    
    spark.stop()

