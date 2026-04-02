import os
import time
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from dask.distributed import Client, LocalCluster

# --- Core Logic Functions (Keep as they are) ---

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
    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file = payload['input_safe']
    output_tiff = payload['output_tiff']
    worker_home = "/root"
    
    print(f"DEBUG: Worker starting tile {payload['tile_id']}")
    
    if not os.path.exists(input_file):
        return {"status": "Failed", "error": f"Input Missing: {input_file}"}

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
        
        gpt_command = "/opt/snap/bin/gpt"
        cmd = [
            gpt_command, temp_graph,
            "-c", "1024M",        # Adjusted Cache
            "-J-Xmx3072M",       # Adjusted Heap
            f"-J-Duser.home={worker_home}",
            "-Dsnap.userdir=/root/.snap",
            "-Dsnap.net.timeout=5",
            "-Djava.awt.headless=true",
            "-Dsnap.productlibrary.disable=true",
            "-PexternalOrbitFile=none" 
        ]
        
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
        
        return {
            "tile_id": payload["tile_id"],
            "status": "Finished" if proc.returncode == 0 else "Failed",
            "code": proc.returncode,
            "start": starttime,
            "stop": stoptime
        }
        
    except Exception as e:
        return {"status": "Error", "msg": str(e)}

# --- Dask Implementation ---

def preprocess_sar_dask(job_id, pending_files):
    # Initialize Dask Client
    # threads_per_worker=1 is crucial for heavy SNAP/Java tasks
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, memory_limit='8GB')
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    s_job_id = str(job_id)

    for file in pending_files:
        base_path = os.getenv('BASE_PATH', '/opt/spark/data')
        template_xml = os.path.join(os.getenv('TEMPLATE_PATH'), os.getenv('GRAPH_FILE_NAME'))
        input_safe = os.path.join(base_path, os.getenv('INPUT_FOLDER_NAME'), file.folder_path)
        output_dir = os.path.join(base_path, s_job_id, "PREPROCESSING", file.folder_path)
        os.makedirs(output_dir, exist_ok=True)
        
        tiles = [
            {
                "tile_id": "Quadrant_1",
                "pixel_region": "9,0,25846,16734",
                "subset_region": "17226,5220,25855,16735",
                "output_tiff": os.path.join(output_dir, f"tile_Q1_{s_job_id}.tif")
            },
            {
                "tile_id": "Quadrant_2",
                "pixel_region": "9,0,25846,16734",
                "subset_region": "17226,5220,25855,16735",
                "output_tiff": os.path.join(output_dir, f"tile_Q2_{s_job_id}.tif")
            }
        ]

        for t in tiles:
            t.update({
                "input_safe": input_safe,
                "template_xml": template_xml,
                "output_dir": output_dir,
                "bands": "" 
            })

        print(f"🚀 Processing {file.folder_path} via Dask...")
        
        # Parallel execution using Dask
        futures = client.map(process_tile_worker, tiles)
        results = client.gather(futures)

        for res in results:
            print(f"Tile {res['tile_id']}: {res['status']} (Start: {res.get('start')}, Stop: {res.get('stop')})")

    client.close()
    cluster.close()

if __name__ == "__main__":
    class FileInfo:
        def __init__(self, path):
            self.folder_path = path

    JOB_ID = str(int(time.time()))
    FILES = [FileInfo("S1C_IW_GRDH_1SDV_20250905T041254_20250905T041319_003984_007ED4_10D5.SAFE")]

    # Container Environment Setup
    os.environ['BASE_PATH'] = '/opt/spark/data' 
    os.environ['TEMPLATE_PATH'] = '/opt/spark-jobs/templates'
    os.environ['GRAPH_FILE_NAME'] = 'preprocess_graphs.xml'
    os.environ['INPUT_FOLDER_NAME'] = 'INPUT'
    
    preprocess_sar_dask(JOB_ID, FILES)
