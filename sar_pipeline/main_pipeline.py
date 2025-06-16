from download.downloader import download_sentinel1_data
from sar_pipeline.preprocessing.unused.apply_orbit import apply_orbit_correction
from sar_pipeline.preprocessing.unused.calibrate import calibrate_image
from sar_pipeline.preprocessing.unused.speckle_filter import apply_speckle_filter
from sar_pipeline.preprocessing.unused.terrain_correction import apply_terrain_correction
from utils.helper import ensure_directory

def run_pipeline(product_id, base_output_dir):
    ensure_directory(base_output_dir)

    zip_path = download_sentinel1_data(product_id, base_output_dir)
    orbit_corrected = apply_orbit_correction(zip_path, f"{base_output_dir}/orbit_corrected.dim")
    calibrated = calibrate_image(orbit_corrected, f"{base_output_dir}/calibrated.dim")
    speckle_filtered = apply_speckle_filter(calibrated, f"{base_output_dir}/speckle_filtered.dim")
    terrain_corrected = apply_terrain_correction(speckle_filtered, f"{base_output_dir}/terrain_corrected.tif")

    print("preprocessing complete.")
    return terrain_corrected

if __name__ == "__main__":
    final_output = run_pipeline("S1_SAMPLE_PRODUCT", "output")
    print("Final output file:", final_output)
