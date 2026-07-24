import subprocess
import os
import time
import sys
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import psutil
from sqlalchemy.orm import sessionmaker

from archive.db import insert_start_time, update_end_time
from archive.schema.schema import Base
from dotenv import load_dotenv

load_dotenv()

# Updated engine with explicit search path routing
engine = create_engine(
    "postgresql+psycopg2://rajesh:rajesh@localhost/eo",
    connect_args={"options": "-csearch_path=sar"}
)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()


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

    read_file = find_node_param("Read", "file")
    pixel_region = find_node_param("Subset", "geoRegion")
    if (subset_bands := find_node_param("Subset", "sourceBands")) is not None:
        subset_bands.text = new_band_names
    if read_file is not None:
        read_file.text = new_input_safe_path
    if pixel_region is not None:
        pixel_region.text = new_geo_region

    write_file = find_node_param("Write", "file")
    if write_file is not None:
        write_file.text = new_output_tiff_path

    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    print(f"✅ Graph updated and saved to: {output_path}")
    return output_path


def run_snap_graph(graph_path, input_file, output_file):
    if not os.path.isfile(graph_path):
        raise FileNotFoundError(f"Graph file not found: {graph_path}")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    command = [
        "gpt", graph_path,
        "-e",
        "-c", "2G",
        "-J-Xmx6G",
        "-q", "2",
        "-J-Duser.home=/tmp",
        "-Dsnap.jai.defaultTileSize=512",
        "-Dsnap.dataio.reader.tileWidth=512",
        "-Dsnap.dataio.reader.tileHeight=512",
        "-Djava.awt.headless=true",
        f"-Pinput={input_file}",
        f"-Poutput={output_file}"
    ]

    print(f"\n Running SNAP GPT with command:\n{' '.join(command)}\n")

    try:
        # Fixed: Removed check=True (unsupported in Popen) and changed DEVNULL to PIPE for logging output
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

        # Non-blocking stream processing while process executes
        while process.poll() is None:
            try:
                cpu = proc_monitor.cpu_percent(interval=0.1)
                mem = proc_monitor.memory_info().rss / (1024 * 1024)
                peak_cpu = max(peak_cpu, cpu)
                peak_mem = max(peak_mem, mem)
            except psutil.NoSuchProcess:
                break

            # Stream the stdout lines sequentially
            line = process.stdout.readline()
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()

        print(f"Metrics: Peak CPU: {peak_cpu}%, Peak RAM: {peak_mem:.2f} MB")

        # Read any remaining output lines after process ends
        remaining_output = process.communicate()[0]
        if remaining_output:
            sys.stdout.write(remaining_output)
            sys.stdout.flush()

        if process.returncode == 0:
            print(f"\n✅ Processing completed. Output saved at: {output_file}")
        else:
            print(f"\n❌ SNAP GPT exited with code {process.returncode}")

    except Exception as e:
        print(f"\n❌ Error during SNAP processing: {str(e)}")


def preprocess_sar_files(job_id, pending_files):
    log_id, start = insert_start_time('SAR_PREPROCESSING')
    print(f"Started at {start}, log ID: {log_id}")

    try:
        for file in pending_files:
            print(file.folder_path)
            current_time = str(time.time())
            base_path = os.getenv('BASE_PATH')
            graph_xml_path = os.getenv('TEMPLATE_PATH')
            safe_folder_path = file.folder_path

            graph_xml = os.path.join(graph_xml_path, os.getenv('GRAPH_FILE_NAME'))
            input_safe = os.path.join(base_path, os.getenv('INPUT_FOLDER_NAME'), safe_folder_path)
            output_dir = os.path.join(base_path, str(job_id), os.getenv('PREPROCESSING_FOLDER_NAME'), file.folder_path)
            os.makedirs(output_dir, exist_ok=True)

            output_file = os.path.join(output_dir, f"{file.folder_path}_{job_id}.tif")
            target_test_band = ""

            graph_xml = update_snap_graph(
                xml_path=graph_xml,
                new_input_safe_path=input_safe,
                new_geo_region="POLYGON ((-3.7 56.2, -3.1 56.2, -3.1 56.7, -3.7 56.7, -3.7 56.2))",
                new_band_names=target_test_band,
                new_output_tiff_path=output_file,
                output_path=os.path.join(os.getenv('BASE_PATH'), "modified_graph.xml")
            )
            run_snap_graph(graph_xml, input_safe, output_file)

    finally:
        end = update_end_time(log_id)
        print(f"Ended at {end}")