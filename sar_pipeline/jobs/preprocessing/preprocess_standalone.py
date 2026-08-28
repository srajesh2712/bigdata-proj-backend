from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
import subprocess
import sys
import xml.etree.ElementTree as ET
import psutil
from dotenv import load_dotenv
import rioxarray
import xarray as xr

engine = create_engine(
    "postgresql+psycopg2://rajesh:rajesh@localhost/eo",
    connect_args={"options": "-csearch_path=sar"}
)
Session = sessionmaker(bind=engine)
session = Session()


load_dotenv() # load env file

BASE_PATH = os.getenv('BASE_PATH')
graph_xml_path = os.getenv('TEMPLATE_PATH')
graph_xml = os.path.join(graph_xml_path, os.getenv('GRAPH_FILE_NAME'))


def update_snap_graph(xml_path, new_input_safe_path, new_geo_region, new_band_names, new_output_tiff_path, output_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    ''' 
    The below function will go over the
     nodes -> parameters and set the value with the new value that comes in 
    '''


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
        "-J-Djava.util.concurrent.ForkJoinPool.common.parallelism=2"
    ]

    print(f"\n Running SNAP GPT with command:\n{' '.join(command)}\n")
    # below code will open separate operating system process
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE, # capture the output so that python program can read this
            stderr=subprocess.STDOUT, # capture the error for the program to read this
            text=True, # decode the output from byte into plain text
            bufsize=1 # buffer the output so that it can be read
        )

        proc_monitor = psutil.Process(process.pid) # get the process for monitoring
        peak_cpu = 0
        peak_mem = 0

        while process.poll() is None: # Continue monitoring while the SNAP process is running and get the status , 0 if exited
            try:
                cpu = proc_monitor.cpu_percent(interval=0.1)
                mem = proc_monitor.memory_info().rss / (1024 * 1024)
                peak_cpu = max(peak_cpu, cpu)
                peak_mem = max(peak_mem, mem)
            except psutil.NoSuchProcess: # handle if SNAP suddenly terminates or is killed before psutil can read
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
    Fetches tasks  based on a target list of Job IDs,
    processes them, and logs entries directly into sar.processing_artifacts.
    """
    start =  datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Started execution loop at {start}")

    # Step 1: Query tasks exactly matching the logic within your Spark pipeline
    job_ids_str = ",".join(map(str, target_job_ids))
    fetch_query = f"""
        SELECT j.job_name,
            j.region_wkt,
            j.job_id,
            s.scene_id,
            '{BASE_PATH}/INPUT/' || s.scene_name  AS local_path,
            '{BASE_PATH}/INPUT/' || s.scene_name || '_task_' || j.job_id || '_output.tif' AS output_tiff,
            '{BASE_PATH}/' || j.job_id || '/' || j.job_id || '_tile.tif' AS hdfs_output_path
                
        FROM sar.processing_job j
     JOIN sar.sar_scene_master s ON j.scene_id = s.scene_id
     WHERE j.job_id IN ({job_ids_str}) AND j.job_status IN ('CREATED','QUEUED')
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
            temp_graph = os.path.join("/tmp", f"graph_{payload['job_id']}.xml")

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

                zarr_output_path = convert_geotiff_to_zarr(
                    payload['output_tiff']
                )
                file_size = os.path.getsize(payload['output_tiff'])
                stop_dt = datetime.now()
                duration = int((stop_dt - start_dt).total_seconds())

                # Step 3: Write artifacts matching Spark's structure
                artifact_query = """
                                 INSERT INTO processing_artifacts (task_id, scene_id, artifact_type, \
                                                                   file_format, \
                                                                   hdfs_path, local_path, file_size_bytes, start_time, \
                                                                   stop_time, \
                                                                   duration_seconds, region_wkt) \
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                                 """
                with conn.cursor() as cur:
                    cur.execute(artifact_query, (

                        payload['job_id'],
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
                print(f"✅ Task  (Job {payload['job_id']}): FINISHED & logged to artifacts.")

            except Exception as task_err:
                conn.rollback()

                print(
                    f"❌ Task (Job {payload['job_id']}): FAILED"
                )
                print(f"   Reason: {str(task_err)}")

    finally:
        conn.close()
        end =  datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Ended pipeline loop execution sequence at {end}")

def convert_geotiff_to_zarr(geotiff_path):
    """
    Convert GeoTIFF to chunked Zarr format.

    Example:
        input:
            /data/scene_output.tif

        output:
            /data/scene_output.zarr/
    """

    if not os.path.isfile(geotiff_path):
        raise FileNotFoundError(
            f"GeoTIFF file not found: {geotiff_path}"
        )

    zarr_path = os.path.splitext(geotiff_path)[0] + ".zarr"

    print(f"\n🔄 Converting GeoTIFF to Zarr")
    print(f"Input : {geotiff_path}")
    print(f"Output: {zarr_path}")

    # Open GeoTIFF lazily using Dask chunks
    raster = rioxarray.open_rasterio(
        geotiff_path,
        chunks={
            "x": 512,
            "y": 512
        }
    )

    # Convert DataArray to Dataset
    dataset = raster.to_dataset(name="sar_data")

    # Store CRS information
    dataset = dataset.rio.write_crs(raster.rio.crs)

    # Write chunked Zarr
    dataset.to_zarr(
        zarr_path,
        mode="w"
    )

    # Close resources
    raster.close()

    print(f"✅ Zarr conversion completed successfully")

    return zarr_path

if __name__ == '__main__':
    # Define the target job IDs you want to sync from your tracking metrics
    TARGET_JOBS = [9,10,11,12]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    starttime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Pass TARGET_JOBS instead of the broken pending_files list
    preprocess_sar_files(TARGET_JOBS)

    stoptime = datetime.now().strftime("%Y%m%d_%H%M%S")


    print('Standalone starting', starttime)
    print('standalone stopping', stoptime)
    fmt = "%Y%m%d_%H%M%S"

    duration = (
            datetime.strptime(stoptime, fmt) -
            datetime.strptime(starttime, fmt)
    ).total_seconds()

    print(duration)
