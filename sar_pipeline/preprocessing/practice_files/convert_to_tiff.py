import subprocess
import os


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

    graph_xml = "E:\\Big Data\\Summer Project\\AssamFlood2023\\preprocessinggraph1.xml"  # Your saved XML
    input_safe = "E:\\Big Data\\Summer Project\\AssamFlood2023\\S1A_IW_GRDH_1SDV_20230805T114911_20230805T114936_049740_05FB26_334C_Cal_Spk_TC.dim"  # Full path to manifest.safe
    output_file = "E:\\Big Data\\Summer Project\\AssamFlood2023\\processed\\processed_output_from_python.tif"  # Output path

    run_snap_graph(graph_xml, input_safe, output_file)


