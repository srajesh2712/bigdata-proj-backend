import os
import sys
import uuid
import tempfile
from dask.distributed import Client
import dask.array as da
import rioxarray as rxr
import pyarrow.fs as pa_fs
from rasterio.io import MemoryFile
import numpy as np

# ---------- CONFIGURATION AND SETUP ----------

# Dask Scheduler address
DASK_SCHEDULER = "dask-scheduler:8786"

# HDFS configuration
HDFS_HOST = "namenode"
HDFS_PORT = 8020

# Input paths for the two distinct SAR images
HDFS_INPUT_PATH_PRE = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
HDFS_INPUT_PATH_POST = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif"

HDFS_OUTPUT_PATH = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/output_flood_mask.tif"

# Shared path (must be mounted into ALL dask containers for staging the output file)
SHARED_DIR = "/opt/shared/data"
os.makedirs(SHARED_DIR, exist_ok=True)

# Local staging file name for the final flood mask output
LOCAL_OUTPUT_NAME = f"output_flood_mask_{uuid.uuid4().hex}.tif"
LOCAL_OUTPUT_PATH = os.path.join(SHARED_DIR, LOCAL_OUTPUT_NAME)

# Dask chunking for parallel processing
CHUNKS = (1, 512, 512)  # Specify chunks as a tuple for da.from_array (Band, Y, X)


def load_hdfs_to_dask(hdfs, hdfs_path):
    """Reads a single SAR GeoTIFF from HDFS into memory and converts it to a Dask array."""
    print(f"Reading HDFS file into client memory: {hdfs_path}")

    # 1. Read entire HDFS file into memory
    with hdfs.open_input_file(hdfs_path) as in_stream:
        data_bytes = in_stream.readall()

    # 2. Open bytes via MemoryFile
    memfile = MemoryFile(data_bytes)
    data_xr = rxr.open_rasterio(memfile)

    # Use the first band if the file has multiple, assuming a single intensity band per file
    if data_xr.data.shape[0] > 1:
        # Select the first band and preserve coordinates/metadata
        data_xr = data_xr.sel(band=data_xr["band"].values[0])
        # Re-add band dimension for consistency (1, Y, X)
        data_xr = data_xr.expand_dims(dim="band", axis=0)

    # 3. Convert NumPy array to Dask array
    dask_data = da.from_array(data_xr.data, chunks=CHUNKS)
    data_array = data_xr.copy(data=dask_data)

    print(f"Loaded shape: {data_array.shape}, chunks: {getattr(data_array, 'chunks', 'N/A')}")

    # Return the Dask array and the MemoryFile reference for later cleanup
    return data_array, memfile


def run():
    client = None
    memfiles_to_close = []  # Track all memory files for cleanup
    try:
        client = Client(DASK_SCHEDULER)
        print("Connected to Dask:", client.dashboard_link)

        hdfs = pa_fs.HadoopFileSystem(host=HDFS_HOST, port=HDFS_PORT)

        # 1. LOAD PRE-EVENT DATA
        pre_array, memfile_pre = load_hdfs_to_dask(hdfs, HDFS_INPUT_PATH_PRE)
        memfiles_to_close.append(memfile_pre)

        # 2. LOAD POST-EVENT DATA
        post_array, memfile_post = load_hdfs_to_dask(hdfs, HDFS_INPUT_PATH_POST)
        memfiles_to_close.append(memfile_post)

        # 3. ALIGN ARRAYS (The Fix)
        # The logs show different shapes, preventing direct subtraction.
        # We reproject the PRE array to perfectly match the POST array's spatial geometry.
        print("Re-projecting PRE array to match POST array geometry to ensure alignment.")

        # This operation is lazy (Dask) and performs the necessary resampling/clipping to match
        # the dimensions and coordinates of the post_array.
        pre_array_aligned = pre_array.rio.reproject_match(post_array)

        # The strict check is no longer needed since reproject_match ensures identical geometry.
        print(f"Aligned PRE array shape: {pre_array_aligned.shape}")

        # 4. Build flood mask Dask graph (lazy computation - DIFFERENTIAL LOGIC)
        FLOOD_THRESHOLD_DB = -5.5
        print(f"Building differential flood mask graph with threshold: < {FLOOD_THRESHOLD_DB} dB (Post - Pre).")

        # --- Differential Analysis Logic ---

        # Convert to dB scale (10 * log10(Intensity)). Use da.clip for Dask compatibility.
        # Squeeze removes the band dimension (1,) leaving only (Y, X) for simpler math
        # NOTE: We use the ALIGNED pre array here.
        pre_dB = 10 * da.log10(da.clip(pre_array_aligned.data.squeeze(), 1e-6, None))
        post_dB = 10 * da.log10(da.clip(post_array.data.squeeze(), 1e-6, None))

        # Calculate differential backscatter (Post - Pre)
        diff_data = post_dB - pre_dB

        # Apply the flood logic: mask where backscatter dropped below the threshold
        flood_mask_data = (diff_data < FLOOD_THRESHOLD_DB)

        # Create the final flood mask DataArray, using the post-event metadata/coords
        # The result is 2D (Y, X)
        flood_mask = post_array.squeeze().copy(data=flood_mask_data.astype('uint8')).rename("flood_mask")

        # --- End Differential Analysis Logic ---

        # 5. Metadata and Structure Fix
        flood_mask.attrs.pop('_FillValue', None)
        flood_mask.attrs.pop('scale_factor', None)
        flood_mask.attrs.pop('add_offset', None)

        # Re-add the 'band' dimension in the first axis (0) to make it 3D again (1, y, x)
        flood_mask = flood_mask.expand_dims(dim="band", axis=0)

        # 6. Trigger distributed computation and write output to local/shared staging path
        print("Starting Dask computation and writing output to shared path:", LOCAL_OUTPUT_PATH)

        flood_mask.rio.to_raster(
            LOCAL_OUTPUT_PATH,
            driver="GTiff",
            dtype='uint8',
            tiled=True,
            compress='LZW',
            NUM_THREADS=4
        )
        print("Local write complete:", LOCAL_OUTPUT_PATH)

        # 7. Upload written output back to HDFS
        print("Uploading final flood mask back to HDFS:", HDFS_OUTPUT_PATH)

        # Clean up any existing file before writing
        try:
            hdfs.delete_file(HDFS_OUTPUT_PATH)
        except Exception:
            pass

        # Stream upload of the final file
        with open(LOCAL_OUTPUT_PATH, "rb") as in_f:
            with hdfs.open_output_stream(HDFS_OUTPUT_PATH) as out_stream:
                while True:
                    chunk = in_f.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    out_stream.write(chunk)
        print("HDFS upload finished.")

    except Exception as e:
        print("An unexpected error occurred during processing:", e, file=sys.stderr)
        raise
    finally:
        if client:
            client.close()
        print("Dask client closed.")

        # Cleanup all MemoryFiles
        for memfile in memfiles_to_close:
            try:
                memfile.close()
            except Exception:
                pass

        # Cleanup the temporary local file
        if os.path.exists(LOCAL_OUTPUT_PATH):
            os.remove(LOCAL_OUTPUT_PATH)
            print(f"Cleaned up temporary file: {LOCAL_OUTPUT_PATH}")


if __name__ == "__main__":
    run()
