from pyspark import SparkContext, SparkConf
import os
import time
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

# --- Keep all your existing functions exactly as they are ---

import fsspec
import os

def write_to_hdfs(local_file_path, hdfs_target_path):
    """
    Uploads a local file into HDFS using fsspec.
    Example:
        local_file_path = "/tmp/tile_Q1.tif"
        hdfs_target_path = "/user/btcchl0040/dask_preprocessed/job123/tile_Q1.tif"
    """
    os.environ["HADOOP_USER_NAME"] = "root" 
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020,user="btcchl0040")

    # Ensure HDFS directory exists
    hdfs_dir = os.path.dirname(hdfs_target_path)
    if not fs.exists(hdfs_dir):
        fs.makedirs(hdfs_dir)

    fs.put(local_file_path, hdfs_target_path)
    return hdfs_target_path
    
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
    #if (pixel_region := find_node_param("Read", "pixelRegion")) is not None:
    #    pixel_region.text = new_pixel_region
    if (subset_region := find_node_param("Subset", "geoRegion")) is not None:
        subset_region.text = new_geo_region
    if (subset_bands := find_node_param("Subset", "sourceBands")) is not None:
        subset_bands.text = new_band_names
    if (write_file := find_node_param("Write", "file")) is not None:
        write_file.text = new_output_tiff_path

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    return output_path
 
def process_tile_worker(payload):
    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Force environment inside the worker
    base = '/opt/spark/data'
    input_file = payload['input_safe']
    output_tiff = payload['output_tiff']
    worker_home = "/tmp"
    # 2. Print to STDOUT so you see it in the Worker Logs
    print(f"DEBUG: Worker starting tile {payload['tile_id']}")
    print(f"DEBUG: Input: {input_file}")
    
    # Check if input exists
    if not os.path.exists(input_file):
        print(f"DEBUG: ERROR - Input file not found at {input_file}")
        return {
            "tile_id": payload.get('tile_id', 'Unknown'), # Add this
            "status": "Failed", 
            "code": 404,                                 # Add this
            "msg": f"Input file not found: {input_file}"
        }

    temp_graph = os.path.join("/tmp", f"graph_{payload['tile_id']}.xml")
    
    try:
        update_snap_graph(
            xml_path=payload['template_xml'],
            new_input_safe_path=input_file,
            new_geo_region=payload['geo_region'],
            new_band_names=payload['bands'],
            new_output_tiff_path=output_tiff,
            output_path=temp_graph
        )
        print(f"DEBUG: Temp graph written to {temp_graph}")
        
        # Use Popen to avoid the buffer hang
        gpt_command = "/opt/snap/bin/gpt"
        cmd = [
            gpt_command, temp_graph,
            "-c", "1536M",
            "-J-Xmx2500M",
            f"-J-Duser.home={worker_home}",           
            "-Dsnap.net.timeout=5",
            "-Djava.awt.headless=true",
            # This is the "Force Offline" combination:
            
            "-Dsnap.productlibrary.disable=true",
            "-PexternalOrbitFile=none" 
        ]
        print(f"DEBUG: Executing: {' '.join(cmd)}")
        
        # 3. Use Popen to stream the logs
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        for line in proc.stdout:
            print(f"[{payload['tile_id']}] {line.strip()}")


        proc.wait()
        stoptime = datetime.now().strftime("%Y%m%d_%H%M%S")
        print('Spark starting',starttime)
        print('Spark stopping',stoptime)
        # -------------------------------
        # Upload result to HDFS
        # -------------------------------
        hdfs_written_path = write_to_hdfs(output_tiff, payload['hdfs_output_path'])
        return {
            "tile_id": payload["tile_id"],
            "status": "Finished" if proc.returncode == 0 else "Failed",
            "code": proc.returncode
        }
        
    except Exception as e:
        print(f"DEBUG: CRITICAL ERROR: {str(e)}")
        return {
            "tile_id": payload.get('tile_id', 'Unknown'), 
            "status": "Error", 
            "code": -1, 
            "msg": str(e)
        }

def preprocess_sar_spark(job_id, pending_files):
    #conf = SparkConf().setAppName("SAR_Tiled_Processing")
    conf = SparkConf().setAppName("SAR_Tiled_Processing").setMaster("local[2]")
    # Note: When submitting via spark-submit, don't hardcode .setMaster("local[*]")
    # It allows the spark-submit command to decide the resources.
    sc = SparkContext.getOrCreate(conf=conf)

    S_JOB_ID = str(job_id)
    HDFS_BASE = "/user/btcchl0040/spark_preprocessed"
    
    for file in pending_files:
        base_path = os.getenv('BASE_PATH', '/home/btcchl0040/Documents/SAR_Data')
        TEMPLATE_XML = os.path.join(os.getenv('TEMPLATE_PATH'), os.getenv('GRAPH_FILE_NAME'))
        INPUT_FILE = os.path.join(base_path, os.getenv('INPUT_FOLDER_NAME'), file.folder_path)
        OUTPUT_DIR = os.path.join(base_path, S_JOB_ID, "PREPROCESSING", file.folder_path)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        HDFS_OUTPUT_DIR = f"{HDFS_BASE}/{S_JOB_ID}/{ file.folder_path}"
 
        os.chmod(os.path.join(base_path, S_JOB_ID), 0o777) # Open the timestamp folder
        os.chmod(OUTPUT_DIR, 0o777)
        
        tiles = [
              {
            "tile_id": "Quadrant_1",
            "geo_region": "POLYGON ((-3.7 56.2, -3.1 56.2, -3.1 56.7, -3.7 56.7, -3.7 56.2))",
            "output_tiff": os.path.join(OUTPUT_DIR, f"tile_Q1_{S_JOB_ID}.tif"),
            "input_safe": INPUT_FILE,
            "template_xml": TEMPLATE_XML,
            "bands": "",
             "hdfs_output_path": f"{HDFS_OUTPUT_DIR}/tile_Q2_{S_JOB_ID}.tif"
        },
          {
            "tile_id": "Quadrant_2",
            "geo_region": "POLYGON ((-3.4600000381469727 56.380001068115234, -3.4200000762939453 56.380001068115234, -3.4200000762939453 56.40999984741211, -3.4600000381469727 56.40999984741211, -3.4600000381469727 56.380001068115234, -3.4600000381469727 56.380001068115234))",
            "output_tiff": os.path.join(OUTPUT_DIR, f"tile_Q2_{S_JOB_ID}.tif"),
            "input_safe": INPUT_FILE,
            "template_xml": TEMPLATE_XML,
            "bands": "",
             "hdfs_output_path": f"{HDFS_OUTPUT_DIR}/tile_Q2_{S_JOB_ID}.tif"
        }
             
        ]

        print(f"🚀 Processing {file.folder_path} via Spark...")
        tile_rdd = sc.parallelize(tiles, numSlices=len(tiles))
        results = tile_rdd.map(process_tile_worker).collect()

        for res in results:
            print(f"Tile {res['tile_id']}: {res['status']} (code={res['code']})")

    sc.stop()

# --- NEW: Entry Point for spark-submit ---

if __name__ == "__main__":
    # Define a simple class for file info since we aren't using the DB inside the Spark job
    class FileInfo:
        def __init__(self, path):
            self.folder_path = path

    # Provide the Job ID and file to process
    S_JOB_ID = str(time.time())
    FILES = [FileInfo("S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE")]

    # Crucial: Ensure environment variables match your file system
    os.environ['BASE_PATH'] = '/opt/spark/data' 
    os.environ['TEMPLATE_PATH'] = '/opt/spark-jobs/templates'
    os.environ['GRAPH_FILE_NAME'] = 'preprocess_graphs.xml'
    os.environ['INPUT_FOLDER_NAME'] = 'INPUT/Jan2026'
    
    preprocess_sar_spark(S_JOB_ID, FILES)

