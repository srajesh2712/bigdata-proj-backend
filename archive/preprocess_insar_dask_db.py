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
TEMPLATE_XML = "/opt/spark-jobs/templates/preprocess_graphs_interferometry.xml"
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

def update_snap_graph(
        xml_path,
        master_path,
        slave_path,
        pixel_region,
        output_path,
        output_file
):

    tree = ET.parse(xml_path)
    root = tree.getroot()

    def find_param(node_id, param_name):
        for node in root.findall(".//node"):
            if node.attrib.get("id") == node_id:
                params = node.find("parameters")
                if params is not None:
                    return params.find(param_name)
        return None


    # Master SLC
    p = find_param("Read", "file")
    if p is not None:
        p.text = master_path


    # Slave SLC
    p = find_param("Read(2)", "file")
    if p is not None:
        p.text = slave_path


    # Apply same pixel region to both SLCs
    p = find_param("Read", "pixelRegion")
    if p is not None:
        p.text = pixel_region


    p = find_param("Read(2)", "pixelRegion")
    if p is not None:
        p.text = pixel_region


    # Output
    p = find_param("Write", "file")
    if p is not None:
        p.text = output_file


    tree.write(
        output_path,
        encoding="UTF-8",
        xml_declaration=True
    )

    return output_path
def process_tile_worker(payload):

    with Semaphore(
        max_leases=2,
        name="global_gpt_limit"
    ):

        try:

            worker_xml = (
                f"/tmp/"
                f"insar_{payload['job_id']}.xml"
            )


            #pixel_region = (
            #    f"{payload['x']},"
            #    f"{payload['y']},"
            #    f"{payload['width']},"
            #    f"{payload['height']}"
            #)
            pixel_region = "0,0,68909,15019"

            output_dim = (
                f"/tmp/"
                f"{payload['job_id']}.dim"
            )

            MASTER_SLC = "/opt/spark/data/INPUT/S1C_IW_SLC__1SDV_20250905T041253_20250905T041321_003984_007ED4_7D8C.SAFE/manifest.safe"

            SLAVE_SLC = "/opt/spark/data/INPUT/S1C_IW_SLC__1SDV_20250824T041253_20250824T041321_003809_00799C_256E.SAFE/manifest.safe"

            pixel_region = "0,0,68909,15019"

            update_snap_graph(
                xml_path=TEMPLATE_XML,
                master_path=MASTER_SLC,
                slave_path=SLAVE_SLC,
                pixel_region=pixel_region,
                output_path=worker_xml,
                output_file=output_dim
            )
            #update_snap_graph(
            #    TEMPLATE_XML,
            #    payload["master_path"],
            #    payload["slave_path"],
            #    pixel_region,
            #    worker_xml,
            #    output_dim
            #)


            cmd = [

                "/opt/snap/bin/gpt",

                worker_xml,

                "-c",
                "6G",

                "-q",
                "2",

                "-J-Djava.awt.headless=true",

                "-J-Dsnap.jai.defaultTileSize=512"

            ]


            start=time.time()

            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            print("========== SNAP GPT OUTPUT ==========")
            print(result.stdout)
            print("=====================================")

            if result.returncode != 0:
                raise RuntimeError(
                    f"SNAP GPT failed with exit code {result.returncode}\n"
                    f"Output:\n{result.stdout}"
                )
            end=time.time()

            return {
                "job_id": payload["job_id"],
                "scene_id": payload["scene_id"],
                "status": "FINISHED",
                "preprocessing_seconds": round(end - start, 2),
                "output": output_dim,
                "hdfs_path": None,
                "file_size": None,
                "hdfs_upload_seconds": 0,
                "zarr_conversion_seconds": 0,
                "pipeline_seconds": round(end - start, 2),
                "region_wkt": payload["region_wkt"]
            }


        except Exception as e:

            return {

                "job_id":
                    payload["job_id"],

                "status":
                    "ERROR",

                "error":
                    str(e),

                "trace":
                    traceback.format_exc()
            }# j.job_id = ANY(%s)
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
