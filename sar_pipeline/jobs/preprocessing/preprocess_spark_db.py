import os
import subprocess
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime
import fsspec
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, unix_timestamp
from pyspark.sql.types import StringType
import rioxarray

from datetime import datetime


def current_time():
    return datetime.now()

# ----------------------------
# Configuration
# ----------------------------
JDBC_URL = "jdbc:postgresql://postgres:5432/eo"
DB_PROPERTIES = {
    "user": "rajesh",
    "password": "rajesh",
    "driver": "org.postgresql.Driver"
}
HADOOP_USER = "btcchl0040"
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

BASE_PATH = "/opt/spark/data"
TEMPLATE_XML_PATH = "/opt/spark-jobs/templates/preprocess_graphs.xml"
HDFS_BASE = "/user/btcchl0040/spark_preprocessed"

# ----------------------------
# Spark session
# ----------------------------
spark = SparkSession.builder.appName("SAR_DB_Driven").getOrCreate()


import tempfile
import shutil
import fsspec
import rioxarray

def convert_hdfs_tiff_to_zarr(hdfs_tiff_path):
    """
    Converts a GeoTIFF stored in HDFS to a Zarr dataset
    and stores it in the same HDFS directory.
    """

    fs = fsspec.filesystem(
        "hdfs",
        host="namenode",
        port=8020,
        user=HADOOP_USER
    )

    hdfs_zarr_path = hdfs_tiff_path.replace(".tif", ".zarr")

    # Temporary local directory
    local_zarr = tempfile.mkdtemp(prefix="zarr_")

    try:

        # Read TIFF directly from HDFS
        with fs.open(hdfs_tiff_path, "rb") as f:
            da = rioxarray.open_rasterio(f)

        da = da.drop_vars("spatial_ref", errors="ignore")

        da.to_dataset(name="band_data").to_zarr(
            local_zarr,
            mode="w",
            consolidated=False
        )

        # Upload entire Zarr directory
        fs.put(
            local_zarr,
            hdfs_zarr_path,
            recursive=True
        )

        return hdfs_zarr_path

    finally:
        shutil.rmtree(local_zarr, ignore_errors=True)



# HDFS upload helper

def write_to_hdfs(local_file_path, hdfs_target_path):
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
    hdfs_dir = os.path.dirname(hdfs_target_path)
    if not fs.exists(hdfs_dir):
        fs.makedirs(hdfs_dir)
    fs.put(local_file_path, hdfs_target_path)
    return hdfs_target_path


# SNAP Graph updater

def update_snap_graph(xml_path, new_input_safe_path, new_geo_region, new_band_names, new_output_tiff_path, output_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    def find_node_param(node_id, param_name):
        for node in root.findall(".//node"):
            if node.attrib.get("id") == node_id:
                params = node.find("parameters")
                if params is not None:
                    return params.find(param_name)
        return None

    if (read_file := find_node_param("Read", "file")) is not None:
        read_file.text = new_input_safe_path
    if (subset_region := find_node_param("Subset", "geoRegion")) is not None:
        subset_region.text = new_geo_region
    if (subset_bands := find_node_param("Subset", "sourceBands")) is not None:
        subset_bands.text = new_band_names
    if (write_file := find_node_param("Write", "file")) is not None:
        write_file.text = new_output_tiff_path

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    return output_path

def process_tiles_in_partition(records):
    """
    Processes an entire partition block of tiles sequentially on the same worker,
    minimizing setup overhead where possible.
    """
    gpt_command = "/opt/snap/bin/gpt"
    if not os.path.exists(gpt_command):
        raise FileNotFoundError(f"GPT binary not found at {gpt_command}")
        
    partition_results = []

    for payload in records:
        pipeline_start = current_time()
        temp_graph = os.path.join("/tmp", f"graph_{payload['job_id']}.xml")
        
        try:
            # Step 1: Update XML
            update_snap_graph(
                xml_path=TEMPLATE_XML_PATH,
                new_input_safe_path=payload['local_path'],
                new_geo_region=payload['region_wkt'],
                new_band_names="",
                new_output_tiff_path=payload['output_tiff'],
                output_path=temp_graph
            )

            # Step 2: Synchronized execution arguments matching Standalone environment
            cmd = [
                gpt_command, temp_graph,

                "-e",  # Enable detailed error diagnostics
                "-c", "2G",  # Allocate 2 GB to the internal SNAP tile cache
                "-J-Xmx6G",  # Limit Java Virtual Machine heap memory to 6 GB
                "-q", "2",  # Restrict execution thread pool to 2 threads
                "-J-Duser.home=/tmp",  # Set temporary user home directory to prevent container permission conflicts
                "-Dsnap.jai.defaultTileSize=512",  # Set JAI processing tile size to 512x512 pixels
                "-Dsnap.dataio.reader.tileWidth=512",  # Set image reader tile width to 512 pixels
                "-Dsnap.dataio.reader.tileHeight=512",  # Set image reader tile height to 512 pixels
                "-Djava.awt.headless=true",  # Disable GUI rendering components for headless server environments
                "-Dsnap.productlibrary.disable=true",  # Disable product library updates to accelerate initialization
                "-PexternalOrbitFile=none"  # Suppress external precise orbit file downloads
            ]
            
            try:
                preprocess_start = current_time()
                # Use DEVNULL to bypass background log processing chains
                subprocess.run(
                    cmd,  # Executes the command stored in the 'cmd' list.
                    stdout=subprocess.DEVNULL,  # Discards all standard output (stdout).
                    stderr=subprocess.STDOUT,  # Redirects standard error (stderr) to stdout (also discarded).
                    check=True  # Raises a CalledProcessError if the command exits with a non-zero status.
                )
                preprocess_end = current_time()

                preprocessing_seconds = (
                        preprocess_end - preprocess_start
                ).total_seconds()
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"GPT failed inside Spark worker with code {e.returncode}")

            # Step 3: HDFS Upload
            file_size = os.path.getsize(payload['output_tiff'])
            hdfs_upload_start = current_time()
            hdfs_path = write_to_hdfs(payload['output_tiff'], payload['hdfs_output_path'])
            hdfs_upload_end = current_time()
            hdfs_upload_seconds = (
                    hdfs_upload_end - hdfs_upload_start
            ).total_seconds()


            # converting tif to zarr
            zarr_start = current_time()
            hdfs_zarr_path = convert_hdfs_tiff_to_zarr(hdfs_path)
            zarr_end = current_time()

            zarr_conversion_seconds = (
                    zarr_end - zarr_start
            ).total_seconds()

            pipeline_end = current_time()
            pipeline_seconds = (
                    pipeline_end - pipeline_start
            ).total_seconds()
            # Append successful result record
            partition_results.append({
                
                "job_id": payload['job_id'],
                "scene_id": payload['scene_id'],
                "status": "FINISHED",
                "code": 0,
                "hdfs_path": hdfs_path,
                "zarr_path": hdfs_zarr_path,
                "file_size": file_size,
                "preprocessing_seconds": preprocessing_seconds,
                "hdfs_upload_seconds": hdfs_upload_seconds,
                "zarr_conversion_seconds": zarr_conversion_seconds,
                "pipeline_seconds": pipeline_seconds,
                "format": "TIFF",
                "type": "PREPROCESSED_TILE",
                "region_wkt": payload['region_wkt'],
            })

        except Exception as e:
            partition_results.append({
                "job_id": payload['job_id'],
                "scene_id": payload['scene_id'],
                "status": "ERROR",
                "code": -1,
                "msg": f"{str(e)} | Traceback: {traceback.format_exc()}",
                "pipeline_seconds": pipeline_seconds,
                "region_wkt": payload['region_wkt']
            })
            
    return iter(partition_results)

# ----------------------------
# Main: fetch tasks and process
# ----------------------------
#def preprocess_sar_from_db(job_ids):
def preprocess_sar_from_db():
    #job_ids_str = ",".join(map(str, job_ids))
    
    query = f"""
    (SELECT j.job_name,
            j.region_wkt,
            j.job_id,
            s.scene_id,
            '{BASE_PATH}/INPUT/' || s.scene_name  AS local_path,
            '{BASE_PATH}/INPUT/' || s.scene_name || '_task_' || j.job_id || '_output.tif' AS output_tiff,
            '{HDFS_BASE}/' || j.job_id || '/' || j.job_id || '_tile.tif' AS hdfs_output_path
     FROM sar.processing_job j
     JOIN sar.sar_scene_master s ON j.scene_id = s.scene_id
     WHERE j.job_status IN ('CREATED','QUEUED') AND j.engine='SPARK'
    ) as job_payload
    """
    # WHERE j.job_id IN ({job_ids_str}) AND j.job_status IN ('CREATED','QUEUED') AND j.engine='SPARK'
    payload_df = spark.read.jdbc(JDBC_URL, query, properties=DB_PROPERTIES)
    print("DEBUG: Preparing to send the following payload to Workers:")
    payload_df.show(truncate=False)
    
    # Convert to standard dictionary RDD tracking
    base_rdd = payload_df.rdd.map(lambda row: row.asDict())
    
    # Force partitioning dynamically down to node job groups at the RDD layer
    # len(job_ids) as i have two spark executors i am using 2 partition
    payload_rdd = base_rdd.keyBy(lambda r: r["job_id"]) \
                      .partitionBy(2) \
                      .values()

    results = payload_rdd.mapPartitions(process_tiles_in_partition).collect()

    for res in results:
        if res['status'] == "ERROR":
            print(f"❌ Task {res['job_id']} (Job {res.get('job_id')}): FAILED")
            print(f"   Reason: {res.get('msg')}")
        else:
            print(f"✅ Task {res['job_id']} (Job {res.get('job_id')}): {res['status']} (Path: {res.get('hdfs_path')})")
            
    success_rows = [r for r in results if r['status'] == "FINISHED"]
    if success_rows:
        raw_artifact_df = spark.createDataFrame(success_rows)
        
        artifact_df = raw_artifact_df.select(
            col("job_id").cast("long"),
            col("scene_id").cast("long"),
            col("type").alias("artifact_type"),
            col("format").alias("file_format"),
            col("hdfs_path"),
            lit("SPARK").cast(StringType()).alias("local_path"), 
            col("file_size").alias("file_size_bytes").cast("long"),
            col("preprocessing_seconds"),
            col("hdfs_upload_seconds"),
            col("zarr_conversion_seconds"),
            col("pipeline_seconds"),
            col("region_wkt").alias("region_wkt")
        )

        try:
            artifact_df.write.jdbc(
                url=JDBC_URL, 
                table="sar.processing_artifacts", 
                mode="append", 
                properties=DB_PROPERTIES
            )
            print(f"✅ Logged {len(success_rows)} artifacts to DB.")
        except Exception as db_err:
            print(f"❌ JDBC Write Failed: {str(db_err)}")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    #TARGET_JOBS = [8, 7, 6, 5]
    preprocess_sar_from_db()
