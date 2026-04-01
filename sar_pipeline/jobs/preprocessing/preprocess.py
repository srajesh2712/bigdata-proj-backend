from pyspark import SparkContext, SparkConf
import os
import time
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

# --- Keep all your existing functions exactly as they are ---

def update_snap_graph(xml_path, new_input_safe_path, new_pixel_region, 
                      new_subset_region, new_band_names, new_output_tiff_path, output_path):
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
    if (pixel_region := find_node_param("Read", "pixelRegion")) is not None:
        pixel_region.text = new_pixel_region
    if (subset_region := find_node_param("Subset", "region")) is not None:
        subset_region.text = new_subset_region
    if (subset_bands := find_node_param("Subset", "sourceBands")) is not None:
        subset_bands.text = new_band_names
    if (write_file := find_node_param("Write", "file")) is not None:
        write_file.text = new_output_tiff_path

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    return output_path
 
def process_tile_worker(payload):
    # 1. Force environment inside the worker
    base = '/opt/spark/data'
    input_file = payload['input_safe']
    output_tiff = payload['output_tiff']
    worker_home = "/root"
    # 2. Print to STDOUT so you see it in the Worker Logs
    print(f"DEBUG: Worker starting tile {payload['tile_id']}")
    print(f"DEBUG: Input: {input_file}")
    
    # Check if input exists
    if not os.path.exists(input_file):
        print(f"DEBUG: ERROR - Input file not found at {input_file}")
        return {"status": "Failed", "error": "Input Missing"}

    temp_graph = os.path.join("/tmp", f"graph_{payload['tile_id']}.xml")
    
    try:
        update_snap_graph(
            xml_path=payload['template_xml'],
            new_input_safe_path=input_file,
            new_pixel_region=payload['pixel_region'],
            new_subset_region=payload['subset_region'],
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
            "-Dsnap.userdir=/root/.snap",
            "-Dsnap.auxdata.dir=/root/.snap/auxdata",
           
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

        return {
            "tile_id": payload["tile_id"],
            "status": "Finished" if proc.returncode == 0 else "Failed",
            "code": proc.returncode
        }
        
    except Exception as e:
        print(f"DEBUG: CRITICAL ERROR: {str(e)}")
        return {"status": "Error", "msg": str(e)}

def preprocess_sar_spark(job_id, pending_files):
    #conf = SparkConf().setAppName("SAR_Tiled_Processing")
    conf = SparkConf().setAppName("SAR_Tiled_Processing").setMaster("local[*]")
    # Note: When submitting via spark-submit, don't hardcode .setMaster("local[*]")
    # It allows the spark-submit command to decide the resources.
    sc = SparkContext.getOrCreate(conf=conf)

    s_job_id = str(job_id)

    for file in pending_files:
        base_path = os.getenv('BASE_PATH', '/home/btcchl0040/Documents/SAR_Data')
        template_xml = os.path.join(os.getenv('TEMPLATE_PATH'), os.getenv('GRAPH_FILE_NAME'))
        input_safe = os.path.join(base_path, os.getenv('INPUT_FOLDER_NAME'), file.folder_path)
        output_dir = os.path.join(base_path, s_job_id, "PREPROCESSING", file.folder_path)
        os.makedirs(output_dir, exist_ok=True)
        
 
        os.chmod(os.path.join(base_path, s_job_id), 0o777) # Open the timestamp folder
        os.chmod(output_dir, 0o777)
        x1, y1 = 17226, 5220
        x2, y2 = 21538, 10977
        width = x2 - x1
        height = y2 - y1
        tiles = [
            {
                "tile_id": "Quadrant_1",
                "pixel_region": "9,0,25846,16734",
                "subset_region": "17226,5220,25855,16735", # Format: x,y,w,h
                "output_tiff": os.path.join(output_dir, f"tile_Q1_{s_job_id}.tif")
            }
        ]

        for t in tiles:
            t.update({
                "input_safe": input_safe,
                "template_xml": template_xml,
                "output_dir": output_dir,
                "bands": "" 
            })

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
    JOB_ID = str(time.time())
    FILES = [FileInfo("S1C_IW_GRDH_1SDV_20250905T041254_20250905T041319_003984_007ED4_10D5.SAFE")]

    # Crucial: Ensure environment variables match your file system
    os.environ['BASE_PATH'] = '/opt/spark/data' 
    os.environ['TEMPLATE_PATH'] = '/opt/spark-jobs/templates'
    os.environ['GRAPH_FILE_NAME'] = 'preprocess_graphs.xml'
    os.environ['INPUT_FOLDER_NAME'] = 'INPUT'
    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    preprocess_sar_spark(JOB_ID, FILES)
    stoptime = datetime.now().strftime("%Y%m%d_%H%M%S")
    print('Spark starting',starttime)
    print('Spark stopping',stoptime)
