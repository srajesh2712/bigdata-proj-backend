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

import tempfile
import shutil
import dask


dask.config.set(scheduler="synchronous")


def convert_hdfs_tiff_to_zarr(hdfs_tiff_path):

    fs = fsspec.filesystem(
        "hdfs",
        host="namenode",
        port=8020,
        user=HADOOP_USER
    )

    hdfs_zarr_path = hdfs_tiff_path.replace(".tif", ".zarr")

    local_zarr_path = tempfile.mkdtemp(prefix="zarr_")

    try:

        with fs.open(hdfs_tiff_path, "rb") as f:

            da = rioxarray.open_rasterio(f)

            # Force loading before closing HDFS stream
            da.load()

        # Preserve CRS and transform
        da = da.rio.write_crs(da.rio.crs)
        da = da.rio.write_transform()

        ds = da.to_dataset(
            name="band_data"
        )

        ds.to_zarr(
            local_zarr_path,
            mode="w",
            consolidated=False
        )

        # Upload Zarr directory to HDFS
        fs.put(
            local_zarr_path,
            hdfs_zarr_path,
            recursive=True
        )

        return hdfs_zarr_path

    finally:
        shutil.rmtree(local_zarr_path, ignore_errors=True)

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
        input_file = payload['local_path']
        output_tiff = payload['output_tiff']
        hdfs_output_path = payload['hdfs_output_path']
        hdfs_zarr_path = hdfs_output_path.replace(".tif", ".b_storage")
        worker_tmp = f"/tmp/tile_{payload['job_id']}.xml"
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
            pipeline_start = datetime.now()

            # -----------------------------
            # Stage 1: SNAP preprocessing
            # -----------------------------
            preprocess_start = datetime.now()
            try:
                subprocess.run(
                    cmd, 
                    stdout=subprocess.DEVNULL, 
                    stderr=subprocess.STDOUT, 
                    check=True
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"GPT failed with code {e.returncode}")

            preprocess_end = datetime.now()

            preprocessing_seconds = (
                    preprocess_end - preprocess_start
            ).total_seconds()

            file_size = os.path.getsize(output_tiff)
            hdfs_upload_start = datetime.now()
            hdfs_written_path = write_to_hdfs(output_tiff, hdfs_output_path)
            hdfs_upload_end = datetime.now()

            hdfs_upload_seconds = (
                    hdfs_upload_end - hdfs_upload_start
            ).total_seconds()

            # -----------------------------
            # Stage 3: Zarr conversion
            # -----------------------------
            zarr_start = datetime.now()

            hdfs_zarr_path = convert_hdfs_tiff_to_zarr(
                hdfs_written_path
            )

            zarr_end = datetime.now()

            zarr_conversion_seconds = (
                    zarr_end - zarr_start
            ).total_seconds()

            # -----------------------------
            # Total pipeline time
            # -----------------------------
            pipeline_end = datetime.now()

            pipeline_seconds = (
                    pipeline_end - pipeline_start
            ).total_seconds()

            fs = fsspec.filesystem("hdfs", host="namenode", port=8020, user=HADOOP_USER)
            if not fs.exists(os.path.dirname(hdfs_zarr_path)):
                fs.makedirs(os.path.dirname(hdfs_zarr_path))
                
            tiff_size = os.path.getsize(output_tiff)
            return {
             

                "job_id": payload['job_id'],
                "scene_id": payload['scene_id'],
                "status": "FINISHED",
                "hdfs_path": hdfs_written_path,
                "file_size": file_size,
                "preprocessing_seconds": preprocessing_seconds,
                "hdfs_upload_seconds": hdfs_upload_seconds,
                "zarr_conversion_seconds": zarr_conversion_seconds,
                "pipeline_seconds": pipeline_seconds,
                "region_wkt": payload['region_wkt'],
                "tiff_size": tiff_size,
            }
        except Exception as e:
            return {
              
                "status": "ERROR", 
                "msg": str(e),
                "trace": traceback.format_exc()
            }
# j.job_id = ANY(%s)
def fetch_tasks_from_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    query = f"""
        SELECT j.job_name,
            j.region_wkt,
            j.job_id,
            s.scene_id,
            '{BASE_PATH}/INPUT/' || s.scene_name  AS local_path,
            '{BASE_PATH}/INPUT/' || s.scene_name || '_task_' || j.job_id || '_output.tif' AS output_tiff,
            '{HDFS_BASE}/' || j.job_id || '/' || j.job_id || '_tile.tif' AS hdfs_output_path
     FROM sar.processing_job j
     JOIN sar.sar_scene_master s ON j.scene_id = s.scene_id
     WHERE j.job_status IN ('CREATED','QUEUED') AND j.engine = 'DASK'
    """
    cur.execute(query)
    tasks = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(t) for t in tasks]

# --- Main Entry Point ---

if __name__ == "__main__":
    #JOB_ID = [1,2,3,4]
    
    tiles_to_process = fetch_tasks_from_db()
    if not tiles_to_process:
        print(f"No pending tasks found for Job . Exiting.")
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
            print(f"✅ Task {r['job_id']} finished in {r['preprocessing_seconds']}s. {r}")
            finished_task_ids.append(r['job_id'])
            success_data.append((
               
                r['job_id'],
                r['scene_id'],
                "PREPROCESSED_TILE",
                "TIFF",
                r['hdfs_path'],
                "DASK",
                r['file_size'],
                r['preprocessing_seconds'],
                r['hdfs_upload_seconds'],
                r['zarr_conversion_seconds'],
                r['pipeline_seconds'],
                r['region_wkt']
            ))
        else:
            print(f"❌ Task failed: {r.get('msg')}")
            print(r.get('trace'))

    # 5. DB Logging
    if success_data:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Insert artifacts
            insert_query = """
                INSERT INTO sar.processing_artifacts (
                    job_id,  scene_id, artifact_type, file_format, 
                    hdfs_path, local_path, file_size_bytes,preprocessing_seconds, hdfs_upload_seconds, zarr_conversion_seconds, pipeline_seconds, region_wkt ) VALUES %s
            """
            execute_values(cur, insert_query, success_data)
            
            # Update task status so they aren't picked up again
            if len(finished_task_ids) == 1:
                cur.execute("UPDATE sar.processing_job SET job_status = 'FINISHED' WHERE job_id = %s", (finished_task_ids[0],))
            else:
                cur.execute("UPDATE sar.processing_job SET job_status = 'FINISHED' WHERE job_id IN %s", (tuple(finished_task_ids),))
            
            conn.commit()
            print(f"✅ Logged {len(success_data)} artifacts and updated task statuses.")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"❌ DB Write Failed: {str(e)}")
    else:
        print("⚠️ No successful tasks to log.")

    client.close()
