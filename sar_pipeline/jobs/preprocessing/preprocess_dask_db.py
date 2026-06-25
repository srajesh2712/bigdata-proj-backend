import traceback
import numcodecs
import zarr
import os
import time
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from dask.distributed import Client, Semaphore  # Imported Semaphore
import fsspec
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import rioxarray
import xarray as xr

# --- Configuration ---
DB_CONFIG = {
    "host": "postgres",
    "database": "eo",
    "user": "rajesh",
    "password": "rajesh",
    "port": 5432
}

HADOOP_USER = "btcchl0040"
os.environ["HADOOP_USER_NAME"] = HADOOP_USER

BASE_PATH = "/opt/spark/data"
TEMPLATE_XML = "/opt/spark-jobs/templates/preprocess_graphs.xml"
HDFS_BASE = "/user/btcchl0040/dask_preprocessed"

# --- Helper Functions ---
def get_hdfs_dir_size(hdfs_path):
    """Calculates total size of a Zarr directory on HDFS"""
    try:
        time.sleep(1)
        fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
        files = fs.find(hdfs_path, detail=True)
        return sum(f['size'] for f in files.values())
    except Exception:
        return 0
        
        
def write_to_hdfs(local_file_path, hdfs_target_path):
    fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
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
    if (b := find_node_param("Subset", "sourceBands")) is not None: b.text = new_band_names
    if (w := find_node_param("Write", "file")) is not None: w.text = new_output_tiff_path
    if (g := find_node_param("Subset", "geoRegion")) is not None: g.text = new_geo_region

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    return output_path

def process_tile_worker(payload):
    """Executes inside the dask-worker container"""
    
    # Global cluster limit to process exactly 2 tiles at a time total
    with Semaphore(max_leases=4, name="global_gpt_limit"):
        input_file = payload['local_input_path']
        output_tiff = payload['local_output_path']
        hdfs_output_path = payload['hdfs_output_path']
        hdfs_zarr_path = hdfs_output_path.replace(".tif", ".b_storage")
        worker_tmp = f"/tmp/tile_{payload['task_id']}.xml"
        gpt_bin = "/opt/snap/bin/gpt"
        
        try:
            os.makedirs(os.path.dirname(output_tiff), exist_ok=True)

            update_snap_graph(
                xml_path=TEMPLATE_XML,
                new_input_safe_path=input_file,
                new_geo_region=payload['region_wkt'],
                new_band_names="",
                new_output_tiff_path=output_tiff,
                output_path=worker_tmp
            )

            # Fixed syntax typo: "Djava.awt.headless=true" -> "-Djava.awt.headless=true"
            cmd = [
                gpt_bin, worker_tmp,
                "-e",
                "-c", "2G",
                "-J-Xmx6G",
                "-q", "2",
                "-J-Duser.home=/tmp",
                "-Dsnap.jai.defaultTileSize=512",
                "-Dsnap.dataio.reader.tileWidth=512",
                "-Dsnap.dataio.reader.tileHeight=512",
                "-Djava.awt.headless=true",
                "-Dsnap.productlibrary.disable=true",
                "-PexternalOrbitFile=none"
            ]
            start_time_raw = datetime.now()
            start_time_str = start_time_raw.strftime("%Y%m%d_%H%M%S")
            try:
                subprocess.run(
                    cmd, 
                    stdout=subprocess.DEVNULL, 
                    stderr=subprocess.STDOUT, 
                    check=True
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"GPT failed with code {e.returncode}")
            
            stop_time_raw = datetime.now()
            file_size = os.path.getsize(output_tiff)
            hdfs_written_path = write_to_hdfs(output_tiff, hdfs_output_path)
            
            fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
            if not fs.exists(os.path.dirname(hdfs_zarr_path)):
                fs.makedirs(os.path.dirname(hdfs_zarr_path))
                
            tiff_size = os.path.getsize(output_tiff)
            return {
                "task_id": payload['task_id'],
                "job_id": payload['job_id'],
                "scene_id": payload['scene_id'],
                "status": "FINISHED",
                "hdfs_path": hdfs_written_path,
                "file_size": file_size,
                "start_time": start_time_str,
                "stop_time": stop_time_raw.strftime("%Y%m%d_%H%M%S"),
                "duration": int((stop_time_raw - start_time_raw).total_seconds()),
                "region_wkt": payload['region_wkt'],
                "tiff_size": tiff_size,
            }
        except Exception as e:
            return {
                "task_id": payload['task_id'], 
                "status": "ERROR", 
                "msg": str(e),
                "trace": traceback.format_exc()
            }

def fetch_tasks_from_db(job_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    query = f"""
        SELECT t.task_id, t.region_wkt, j.job_id, s.scene_id,
               '{BASE_PATH}/INPUT/' || s.local_path AS local_input_path,
               '{BASE_PATH}/' || j.job_id || '/PREPROCESSING/' || t.task_id || '.tif' AS local_output_path,
               '{HDFS_BASE}/' || j.job_id || '/' || t.task_id || '_tile.tif' AS hdfs_output_path
        FROM sar.job_tasks t
        JOIN sar.processing_job j ON t.job_id = j.job_id
        JOIN sar.sar_scene_master s ON j.scene_id = s.scene_id
        WHERE t.job_id = ANY(%s) AND t.task_status IN ('CREATED','QUEUED');
    """
    cur.execute(query, (job_id,))
    tasks = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(t) for t in tasks]

# --- Main Entry Point ---

if __name__ == "__main__":
    JOB_ID = [8, 7, 6, 5]
    
    tiles_to_process = fetch_tasks_from_db(JOB_ID)
    if not tiles_to_process:
        print(f"No pending tasks found for Job {JOB_ID}. Exiting.")
        exit()

    client = Client('dask-scheduler:8786')
    print(f"Connected to Dask. Processing {len(tiles_to_process)} tiles.")

    # Removed resources tag so it processes on any available normal worker threads
    futures = client.map(process_tile_worker, tiles_to_process)
    results = client.gather(futures)

    # 4. Process Results and Prepare for DB
    success_data = []
    finished_task_ids = []

    for r in results:
        if r['status'] == "FINISHED":
            print(f"✅ Task {r['task_id']} finished in {r['duration']}s. {r}")
            finished_task_ids.append(r['task_id'])
            success_data.append((
                r['job_id'],
                r['task_id'],
                r['scene_id'],
                "PREPROCESSED_TILE",
                "TIFF",
                r['hdfs_path'],
                "DASK",
                r['file_size'],
                datetime.strptime(r['start_time'], "%Y%m%d_%H%M%S"),
                datetime.strptime(r['stop_time'], "%Y%m%d_%H%M%S"),
                r['duration'],
                r['region_wkt']
            ))
        else:
            print(f"❌ Task {r['task_id']} failed: {r.get('msg')}")

    # 5. DB Logging
    if success_data:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Insert artifacts
            insert_query = """
                INSERT INTO sar.processing_artifacts (
                    job_id, task_id, scene_id, artifact_type, file_format, 
                    hdfs_path, local_path, file_size_bytes, start_time, stop_time, duration_seconds, region_wkt
                ) VALUES %s
            """
            execute_values(cur, insert_query, success_data)
            
            # Update task status so they aren't picked up again
            if len(finished_task_ids) == 1:
                cur.execute("UPDATE sar.job_tasks SET task_status = 'FINISHED' WHERE task_id = %s", (finished_task_ids[0],))
            else:
                cur.execute("UPDATE sar.job_tasks SET task_status = 'FINISHED' WHERE task_id IN %s", (tuple(finished_task_ids),))
            
            conn.commit()
            print(f"✅ Logged {len(success_data)} artifacts and updated task statuses.")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ DB Write Failed: {str(e)}")
    else:
        print("⚠️ No successful tasks to log.")

    client.close()
