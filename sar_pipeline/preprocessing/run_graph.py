import subprocess
import os
import time


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
        subprocess.run(command, check=True)
        print(f"\n✅ Processing completed. Output saved at: {output_file}")
    except subprocess.CalledProcessError as e:
        print("\n❌ Error during SNAP processing:")
        print(e)


if __name__ == "__main__":
    base_path = "E:\Big Data\Summer Project\Assam-June5-2025\Flood-June5-2025\\20240605"
    graph_xml_path = "E:\Big Data\Summer Project\AssamFlood2023"
    safe_folder_path = "S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB.SAFE"
    current_time = str(time.time())
    output_folder = "output"
    graph_xml =os.path.join(graph_xml_path,"preprocessinggraph.xml" )
    input_safe = os.path.join(base_path,safe_folder_path)
    output_dir = os.path.join(base_path, output_folder, current_time)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_file.tif")

    run_snap_graph(graph_xml, input_safe, output_file)
