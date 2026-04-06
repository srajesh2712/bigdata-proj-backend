import os
import time
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from dask.distributed import Client
 
# --- Helper Functions ---

import fsspec
import os

def write_to_hdfs(local_file_path, hdfs_target_path):
    """
    Uploads a local file into HDFS using fsspec.
    Example:
        local_file_path = "/tmp/tile_Q1.tif"
        hdfs_target_path = "/user/btcchl0040/dask_preprocessed/job123/tile_Q1.tif"
    """
    #os.environ["HADOOP_USER_NAME"] = "root" 
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
                if params is not None: return params.find(param_name)
        return None

    if (f := find_node_param("Read", "file")) is not None: f.text = new_input_safe_path
    #if (p := find_node_param("Read", "pixelRegion")) is not None: p.text = new_pixel_region
    #if (s := find_node_param("Subset", "region")) is not None: s.text = new_subset_region
    if (b := find_node_param("Subset", "sourceBands")) is not None: b.text = new_band_names
    if (w := find_node_param("Write", "file")) is not None: w.text = new_output_tiff_path
    if (g := find_node_param("Subset", "geoRegion")) is not None: 
        g.text = new_geo_region

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    return output_path

def process_tile_worker(payload):
    """Executes inside the dask-worker container"""
    start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file = payload['input_safe']
    output_tiff = payload['output_tiff']
    hdfs_output_path = payload['hdfs_output_path']
    worker_tmp = f"/tmp/tile_{payload['tile_id']}.xml"
    gpt_bin = "/opt/snap/bin/gpt"
    
    if not os.path.exists(input_file):
        return {"tile_id": payload['tile_id'], "status": "Error", "msg": f"Input not found: {input_file}"}

    try:
        # Ensure the sub-directory for this job exists on the worker's shared volume
        os.makedirs(os.path.dirname(output_tiff), exist_ok=True)

        update_snap_graph(
            xml_path=payload['template_xml'],
            new_input_safe_path=input_file,
            new_geo_region=payload['geo_region'],
            new_band_names=payload['bands'],
            new_output_tiff_path=output_tiff,
            output_path=worker_tmp
        )

        cmd = [
            gpt_bin, worker_tmp,
            "-c", "1024M",
            "-J-Xmx3072M",
            "-Djava.awt.headless=true",
            "-Dsnap.productlibrary.disable=true",
            "-PexternalOrbitFile=none"
        ]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            print(f"[{payload['tile_id']}] {line.strip()}")
        proc.wait()
        # -------------------------------
        # Upload result to HDFS
        # -------------------------------
        hdfs_written_path = write_to_hdfs(output_tiff, hdfs_output_path)

        return {
            "tile_id": payload["tile_id"],
            "status": "Finished",
            "runtime": f"{start_time} to {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "local_output": output_tiff,
            "hdfs_output": hdfs_written_path
        }
    except Exception as e:
        return {"tile_id": payload['tile_id'], "status": "Exception", "msg": str(e)}

# --- Main Entry Point ---

if __name__ == "__main__":
    # 1. Environment and Path Logic
    os.environ['BASE_PATH'] = '/opt/spark/data' 
    os.environ['TEMPLATE_PATH'] = '/opt/spark-jobs/templates'
    os.environ['GRAPH_FILE_NAME'] = 'preprocess_graphs.xml'
    os.environ['INPUT_FOLDER_NAME'] = 'INPUT/Jan2026'

    # Generate unique Job ID and handle file paths
    S_JOB_ID = str(time.time())
    #SAFE_NAME = "S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE"
    SAFE_NAME = "S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE"
    
    BASE_PATH = os.getenv('BASE_PATH')
    TEMPLATE_XML = os.path.join(os.getenv('TEMPLATE_PATH'), os.getenv('GRAPH_FILE_NAME'))
    INPUT_FILE = os.path.join(BASE_PATH, os.getenv('INPUT_FOLDER_NAME'), SAFE_NAME)
    
    # 2. Pattern-based Output Directory: [base]/[job_id]/PREPROCESSING/[safe_name]
    OUTPUT_DIR = os.path.join(BASE_PATH, S_JOB_ID, "PREPROCESSING", SAFE_NAME)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    HDFS_BASE = "/user/btcchl0040/dask_preprocessed"
    HDFS_OUTPUT_DIR = f"{HDFS_BASE}/{S_JOB_ID}/{SAFE_NAME}"
    
    # 3. Connect to the Dask Scheduler
    client = Client('dask-scheduler:8786')
    print(f"Connected to Scheduler. Dashboard: {client.dashboard_link}")
    print(f"Job Output Path: {OUTPUT_DIR}")

    # 4. Define Tiles with the correct output pattern
    tiles = [
       
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

    print(f"🚀 Launching Dask Jobs...")
    futures = client.map(process_tile_worker, tiles)
    results = client.gather(futures)

    for r in results:
        status_msg = r.get('runtime') if r['status'] == "Finished" else r.get('msg', 'Unknown Error')
        print(f"Result {r}")

    client.close()
