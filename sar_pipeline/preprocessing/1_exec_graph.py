from datetime import datetime
import subprocess
import os
import time
import sys
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
from sar_pipeline.db import connect_db, insert_start_time, update_end_time
from sqlalchemy.orm import sessionmaker

from sar_pipeline.schema.schema import SafeFile, Base
from dotenv import load_dotenv
load_dotenv()
engine = create_engine("postgresql+psycopg2://rajesh:rajesh@localhost/eo")
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()

def update_snap_graph(
    xml_path,
    new_input_safe_path,
    new_pixel_region,        # e.g., "9,0,25846,16734"
    new_subset_region,       # e.g., "17226,5220,25855,16735"
    new_band_names,          # e.g., "Amplitude_VH,Intensity_VH"
    new_output_tiff_path,
    output_path
):
    # Load the XML
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Helper function to find a node by its id
    def find_node_param(node_id, param_name):
        for node in root.findall(".//node"):
            if node.attrib.get("id") == node_id:
                params = node.find("parameters")
                if params is not None:
                    return params.find(param_name)
        return None

    # Update Read → file + pixelRegion
    read_file = find_node_param("Read", "file")
    pixel_region = find_node_param("Read", "pixelRegion")
    if read_file is not None:
        read_file.text = new_input_safe_path
    if pixel_region is not None:
        pixel_region.text = new_pixel_region

    # Update Subset → region + bands
    subset_region = find_node_param("Subset", "region")
    subset_bands = find_node_param("Subset", "sourceBands")
    if subset_region is not None:
        subset_region.text = new_subset_region
    if subset_bands is not None:
        subset_bands.text = new_band_names

    # Update Write → file
    write_file = find_node_param("Write", "file")
    if write_file is not None:
        write_file.text = new_output_tiff_path

    # Save the updated file
    tree.write(output_path, encoding="UTF-8", xml_declaration=True)
    print(f"✅ Graph updated and saved to: {output_path}")
    return output_path

def run_snap_graph(graph_path, input_file, output_file):
    """
    Run a SNAP Graph XML using the gpt command-line tool.

    Parameters:
    - graph_path: Path to the .xml graph
    - input_file: Path to the input .SAFE file (manifest.safe)
    - output_file: Path to the output GeoTIFF or DIMAP file
    """
    if not os.path.isfile(graph_path):
        raise FileNotFoundError(f"Graph file not found: {graph_path}")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    command = [
        "gpt", graph_path,
        f"-Pinput={input_file}",
        f"-Poutput={output_file}"
    ]

    print(f"\n▶️ Running SNAP GPT with command:\n{' '.join(command)}\n")

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        while True:
            char = process.stdout.read(1)
            if not char:
                break
            sys.stdout.write(char)
            sys.stdout.flush()
        process.wait()
        if process.returncode == 0:
            print(f"\n✅ Processing completed. Output saved at: {output_file}")
        else:
            print(f"\n❌ SNAP GPT exited with code {process.returncode}")
    except subprocess.CalledProcessError as e:
        print("\n❌ Error during SNAP processing:")
        print(e)
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)


if __name__ == "__main__":
    conn = connect_db()
    log_id, start = insert_start_time(conn)
    conn.close()
    print(f"Started at {start}, log ID: {log_id}")
    try:

        pending_files = session.query(SafeFile).filter_by(active=True, status='pending').all()
        for file in pending_files:
            print(file.folder_path)
            current_time = str(time.time())
            base_path =os.getenv('BASE_PATH')# "/home/btcchl0040/Documents/SAR_Data/"
            graph_xml_path = os.getenv('TEMPLATE_PATH')#"/home/btcchl0040/Documents/SAR_Data"
            safe_folder_path =file.folder_path# "S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE"

            graph_xml =os.path.join(graph_xml_path,os.getenv('GRAPH_FILE_NAME') )#"preprocessinggraph.xml" )
            input_safe = os.path.join(base_path,os.getenv('INPUT_FOLDER_NAME'),safe_folder_path)
            output_dir = os.path.join(base_path, os.getenv('OUTPUT_FOLDER_NAME'), file.folder_path)
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"{file.folder_path}_{timestamp}.tif")

            graph_xml = update_snap_graph(
                xml_path=graph_xml,
                new_input_safe_path=input_safe,
                new_pixel_region="9,0,25846,16734",
                new_subset_region="17226,5220,25855,16735",
                new_band_names="Amplitude_VH,Intensity_VH,Amplitude_VV,Intensity_VV",
                new_output_tiff_path=output_file,
                output_path=os.path.join(os.getenv('BASE_PATH'),"modified_graph.xml")
            )
            run_snap_graph(graph_xml, input_safe, output_file)
    finally:
        conn = connect_db()
        end = update_end_time(conn, log_id)
        print(f"Ended at {end}")
        conn.close()



