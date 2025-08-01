import subprocess
import os
import time

import xml.etree.ElementTree as ET
def update_snap_graph(
    xml_path,
    new_input_safe_path,
    new_pixel_region,        # e.g., "9,0,25846,16734"
    new_subset_region,       # e.g., "17226,5220,25855,16735"
    new_band_names,          # e.g., "Amplitude_VH,Intensity_VH"
    new_output_tiff_path,
    output_path="modified_graph.xml"
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
        subprocess.run(command, check=True)
        print(f"\n✅ Processing completed. Output saved at: {output_file}")
    except subprocess.CalledProcessError as e:
        print("\n❌ Error during SNAP processing:")
        print(e)


if __name__ == "__main__":
    base_path = "E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Flood-June5-2024\\20240605"
    graph_xml_path = "E:\Big Data\Summer Project"
    safe_folder_path = "S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB.SAFE"
    current_time = str(time.time())
    output_folder = "output"
    graph_xml =os.path.join(graph_xml_path,"myGraph.xml" )
    input_safe = os.path.join(base_path,safe_folder_path)
    output_dir = os.path.join(base_path, output_folder, current_time)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_file.tif")

    graph_xml = update_snap_graph(
        xml_path=graph_xml,
        new_input_safe_path="E:/Big Data/Summer Project/Assam-Flood-June5-2024/Flood-June5-2024/20240605/S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB.SAFE",
        new_pixel_region="9,0,25846,16734",
        new_subset_region="17226,5220,25855,16735",
        new_band_names="Amplitude_VH,Intensity_VH,Amplitude_VV,Intensity_VV",
        new_output_tiff_path="E:/Big Data/Summer Project/Assam-Flood-June5-2024/Flood-June5-2024/20240605/scene_TC.tif",
        output_path="updated_graph.xml"
    )


    run_snap_graph(graph_xml, input_safe, output_file)
