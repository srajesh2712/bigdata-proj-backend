import random
from sar_pipeline.analysis.create_flood_mask import create_flood_mask, create_mask
from sar_pipeline.analysis.merge_flooded_tiles import merge_flooded_tiles
# REMOVED fetch_processing_files since we are syncing with the Job ID workflow
from sar_pipeline.db import update_processing_files_by_jobid, insert_job

from sar_pipeline.a_preprocessing.split_geotiff_files import split_files
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Updated to use search_path mapping
engine = create_engine(
    "postgresql+psycopg2://rajesh:rajesh@localhost/eo",
    connect_args={"options": "-csearch_path=sar"}
)
Session = sessionmaker(bind=engine)
session = Session()


from datetime import datetime
import subprocess
import sys
import xml.etree.ElementTree as ET
import psutil


from dotenv import load_dotenv

load_dotenv()

# Configuration variables synchronized from Spark variables
BASE_PATH = os.getenv('BASE_PATH')
graph_xml_path = os.getenv('TEMPLATE_PATH')
graph_xml = os.path.join(graph_xml_path, os.getenv('GRAPH_FILE_NAME'))


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


def run_snap_graph(graph_path, output_file):
    if not os.path.isfile(graph_path):
        raise FileNotFoundError(f"Graph file not found: {graph_path}")

    gpt_command = "/opt/snap/bin/gpt"
    command = [
        "gpt", graph_path,
        "-c", "2G",
        "-J-Xmx6G",
        "-q", "2",
        "-J-Duser.home=/tmp",
        "-Dsnap.jai.defaultTileSize=512",
        "-Dsnap.dataio.reader.tileWidth=512",
        "-Dsnap.dataio.reader.tileHeight=512",
        "-Djava.awt.headless=true",
        "-PexternalOrbitFile=none",
        "-J-Djava.util.concurrent.ForkJoinPool.common.parallelism=2"
    ]

    print(f"\n Running SNAP GPT with command:\n{' '.join(command)}\n")

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        proc_monitor = psutil.Process(process.pid)
        peak_cpu = 0
        peak_mem = 0

        while process.poll() is None:
            try:
                cpu = proc_monitor.cpu_percent(interval=0.1)
                mem = proc_monitor.memory_info().rss / (1024 * 1024)
                peak_cpu = max(peak_cpu, cpu)
                peak_mem = max(peak_mem, mem)
            except psutil.NoSuchProcess:
                break

            line = process.stdout.readline()
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()

        print(f"Metrics: Peak CPU: {peak_cpu}%, Peak RAM: {peak_mem:.2f} MB")

        remaining_output = process.communicate()[0]
        if remaining_output:
            sys.stdout.write(remaining_output)
            sys.stdout.flush()

        if process.returncode != 0:
            raise RuntimeError(f"SNAP GPT exited with failure code {process.returncode}")
        print(f"\n✅ Processing completed. Output saved at: {output_file}")

    except Exception as e:
        print(f"\n❌ Error during SNAP processing: {str(e)}")
        raise


def preprocess_sar_files(target_job_ids):
    """
    Fetches tasks exactly like the Spark code based on a target list of Job IDs,
    processes them, and logs entries directly into sar.processing_artifacts.
    """
    #log_id, start = insert_start_time('SAR_PREPROCESSING')
    start =  datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Started execution loop at {start}")

    # Step 1: Query tasks exactly matching the logic within your Spark pipeline
    job_ids_str = ",".join(map(str, target_job_ids))
    fetch_query = f"""
        SELECT t.task_id,
                t.task_name,
                t.region_wkt,
                j.job_id,
                s.scene_id,
                '{BASE_PATH}/INPUT/' || s.local_path  AS local_path,
                '{BASE_PATH}/INPUT/' || s.scene_name || '_task_' || t.task_id || '_output.tif' AS output_tiff,
                '{BASE_PATH}/' || j.job_id || '/' || t.task_id || '_tile.tif' AS hdfs_output_path
        FROM job_tasks t
        JOIN processing_job j ON t.job_id = j.job_id
        JOIN sar_scene_master s ON j.scene_id = s.scene_id
        WHERE t.job_id IN ({job_ids_str}) AND t.task_status IN ('CREATED','QUEUED')
    """

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(fetch_query)
            columns = [desc[0] for desc in cur.description]
            records = [dict(zip(columns, row)) for row in cur.fetchall()]

        print(f"DEBUG: Preparing to execute standalone loop for {len(records)} matching payloads.")

        # Step 2: Loop payloads sequentially
        for payload in records:
            start_dt = datetime.now()
            temp_graph = os.path.join("/tmp", f"graph_{payload['task_id']}.xml")

            os.makedirs(os.path.dirname(payload['output_tiff']), exist_ok=True)

            try:
                update_snap_graph(
                    xml_path=graph_xml,
                    new_input_safe_path=payload['local_path'],
                    new_geo_region=payload['region_wkt'],
                    new_band_names="",
                    new_output_tiff_path=payload['output_tiff'],
                    output_path=temp_graph
                )

                run_snap_graph(temp_graph, payload['output_tiff'])

                file_size = os.path.getsize(payload['output_tiff'])
                stop_dt = datetime.now()
                duration = int((stop_dt - start_dt).total_seconds())

                # Step 3: Write artifacts matching Spark's structure
                artifact_query = """
                                 INSERT INTO processing_artifacts (job_id, task_id, scene_id, artifact_type, \
                                                                   file_format, \
                                                                   hdfs_path, local_path, file_size_bytes, start_time, \
                                                                   stop_time, \
                                                                   duration_seconds, region_wkt) \
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                                 """
                with conn.cursor() as cur:
                    cur.execute(artifact_query, (
                        payload['job_id'],
                        payload['task_id'],
                        payload['scene_id'],
                        "PREPROCESSED_TILE",
                        "TIFF",
                        payload['hdfs_output_path'],
                        "STANDALONE",
                        file_size,
                        start_dt,
                        stop_dt,
                        duration,
                        payload['region_wkt']
                    ))
                conn.commit()
                print(f"✅ Task {payload['task_id']} (Job {payload['job_id']}): FINISHED & logged to artifacts.")

            except Exception as task_err:
                conn.rollback()
                print(f"❌ Task {payload['task_id']} (Job {payload['job_id']}): FAILED")
                print(f"   Reason: {str(task_err)}")

    finally:
        conn.close()
        end =  datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Ended pipeline loop execution sequence at {end}")


if __name__ == '__main__':
    # Define the target job IDs you want to sync from your tracking metrics
    TARGET_JOBS = [8, 7, 6, 5]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Pass TARGET_JOBS instead of the broken pending_files list
    preprocess_sar_files(TARGET_JOBS)

    stoptime = datetime.now().strftime("%Y%m%d_%H%M%S")


    print('Standalone starting', starttime)
    print('standalone stopping', stoptime)