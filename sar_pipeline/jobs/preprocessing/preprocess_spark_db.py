import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, to_timestamp, unix_timestamp
from datetime import datetime
from pyspark.sql.types import StringType
import os
import subprocess
import fsspec
import xml.etree.ElementTree as ET

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

# ----------------------------
# HDFS upload helper
# ----------------------------
def write_to_hdfs(local_file_path, hdfs_target_path):
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
    hdfs_dir = os.path.dirname(hdfs_target_path)
    if not fs.exists(hdfs_dir):
        fs.makedirs(hdfs_dir)
    fs.put(local_file_path, hdfs_target_path)
    return hdfs_target_path

# ----------------------------
# SNAP Graph updater
# ----------------------------
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

# ----------------------------
# Tile processor
# ----------------------------
def process_tile_worker(payload):
    print(f"DEBUG: Attempting to set 'Read' file to: {payload}")
    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_graph = os.path.join("/tmp", f"graph_{payload['task_id']}.xml")
    
    try:
        # Step 1: Update XML
        try:
            update_snap_graph(
                xml_path=TEMPLATE_XML_PATH,
                new_input_safe_path=payload['local_path'],
                new_geo_region=payload['region_wkt'],
                new_band_names="",
                new_output_tiff_path=payload['output_tiff'],
                output_path=temp_graph
            )
        except Exception as xml_err:
            raise RuntimeError(f"XML Update Failed: {str(xml_err)}")

        # Step 2: Run GPT
        gpt_command = "/opt/snap/bin/gpt"
        if not os.path.exists(gpt_command):
            raise FileNotFoundError(f"GPT binary not found at {gpt_command}")

        cmd = [
            gpt_command, temp_graph,
            "-c", "1536M",
            "-J-Xmx2500M",
            f"-J-Duser.home=/tmp",
            "-Dsnap.net.timeout=5",
            "-Djava.awt.headless=true",
            "-Dsnap.productlibrary.disable=true",
            "-PexternalOrbitFile=none"
        ]
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Capture logs for debugging
        captured_logs = []
        for line in proc.stdout:
            log_line = line.strip()
            captured_logs.append(log_line)
            print(f"[{payload['task_id']}] {log_line}")
        
        proc.wait()
        
        if proc.returncode != 0:
            last_logs = "\n".join(captured_logs[-50:]) # Get last 5 lines of error
            raise RuntimeError(f"GPT exited with code {proc.returncode}. Last logs: {last_logs}")

        # Step 3: Upload to HDFS
        try:
            file_size = os.path.getsize(payload['output_tiff'])
            hdfs_path = write_to_hdfs(payload['output_tiff'], payload['hdfs_output_path'])
        except Exception as hdfs_err:
            raise RuntimeError(f"HDFS Upload Failed: {str(hdfs_err)}")

        return {
            "task_id": payload['task_id'],
            "job_id": payload['job_id'],
            "scene_id": payload['scene_id'],
            "status": "FINISHED",
            "code": 0,
            "hdfs_path": hdfs_path,
            "start_time": starttime,
            "stop_time": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "hdfs_path": hdfs_path,
            "file_size": file_size,
            "format": "TIFF",
            "type": "PREPROCESSED_TILE"
        }

    except Exception as e:
        # This catches everything and sends the full traceback back to the driver
        return {
            "task_id": payload['task_id'],
            "status": "ERROR",
            "code": -1,
            "msg": f"{str(e)} | Traceback: {traceback.format_exc()}",
            "start_time": starttime,
            "stop_time": datetime.now().strftime("%Y%m%d_%H%M%S")
        }

# ----------------------------
# Main: fetch tasks and process
# ----------------------------
def preprocess_sar_from_db(job_id):
    # Join tables: job_tasks -> processing_job -> sar_scene_master
    query = f"""
    (SELECT t.task_id,
            t.task_name,
            t.region_wkt,
            j.job_id,
            s.scene_id,
            '{BASE_PATH}/INPUT/' || s.local_path  AS local_path,
            '{BASE_PATH}/INPUT/' || s.scene_name || '_output.tif' AS output_tiff,
            '{HDFS_BASE}/' || j.job_id || '/' || t.task_id || '_tile.tif' AS hdfs_output_path
     FROM sar.job_tasks t
     JOIN sar.processing_job j ON t.job_id = j.job_id
     JOIN sar.sar_scene_master s ON j.scene_id = s.scene_id
     WHERE t.job_id = {job_id} AND t.task_status IN ('CREATED','QUEUED')
    ) as job_payload
    """
    payload_df = spark.read.jdbc(JDBC_URL, query, properties=DB_PROPERTIES)
    print("DEBUG: Preparing to send the following payload to Workers:")
    payload_df.show(truncate=False)
    # Convert to RDD for parallel processing
    payload_rdd = payload_df.rdd.map(lambda row: row.asDict())

    results = payload_rdd.map(process_tile_worker).collect()

    for res in results:
        print(res)
        
        if res['status'] == "ERROR":
            print(f"❌ Task {res['task_id']}: FAILED")
            print(f"   Reason: {res.get('msg')}")
        else:
            print(f"✅ Task {res['task_id']}: {res['status']} (Path: {res.get('hdfs_path')})")
    success_rows = [r for r in results if r['status'] == "FINISHED"]
    if success_rows:
        # 1. Create DataFrame (columns will be 'type', 'format', etc.)
        raw_artifact_df = spark.createDataFrame(success_rows)
        
        # 2. Select and Rename columns to match your Postgres Table DDL
        artifact_df = raw_artifact_df.select(
            col("job_id").cast("long"),
            col("task_id").cast("long"),
            col("scene_id").cast("long"),
            col("type").alias("artifact_type"),
            col("format").alias("file_format"),
            col("hdfs_path"),
            # FIX: Explicitly cast the Null to a String
            lit(None).cast(StringType()).alias("local_path"), 
            col("file_size").alias("file_size_bytes").cast("long"),
            to_timestamp(col("start_time"), "yyyyMMdd_HHmmss").alias("start_time"),
            to_timestamp(col("stop_time"), "yyyyMMdd_HHmmss").alias("stop_time"),
           (
                unix_timestamp(col("stop_time"), "yyyyMMdd_HHmmss") - 
                unix_timestamp(col("start_time"), "yyyyMMdd_HHmmss")
            ).alias("duration_seconds")
        )

        # 3. Write to JDBC
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
    JOB_ID = 1  # Replace with your actual job_id
    preprocess_sar_from_db(JOB_ID)
