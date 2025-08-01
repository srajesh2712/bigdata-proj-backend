import subprocess
import os

def convert_dim_to_bigtiff(input_dim_path, output_tif_path):
    """
    Converts a BEAM-DIMAP .dim file to a compressed BigTIFF using GDAL.
    """
    if not os.path.exists(input_dim_path):
        raise FileNotFoundError(f"Input file not found: {input_dim_path}")

    command = [
        "gdal_translate",
        "-of", "GTiff",
        "-co", "BIGTIFF=YES",
        "-co", "COMPRESS=LZW",
        input_dim_path,
        output_tif_path
    ]

    try:
        subprocess.run(command, check=True)
        print(f"✅ BigTIFF created at: {output_tif_path}")
    except subprocess.CalledProcessError as e:
        print("❌ Error during GDAL conversion:")
        print(e)

# Example usage
if __name__ == "__main__":
    input_file = r"E:\Big Data\Summer Project\AssamFlood2023\S1A_IW_GRDH_1SDV_20230805T114911_20230805T114936_049740_05FB26_334C_Cal_Spk_TC.dim"
    output_file = r"E:\Big Data\Summer Project\AssamFlood2023\processed\S1A_BIGTIFF_output.tif"
    convert_dim_to_bigtiff(input_file, output_file)
