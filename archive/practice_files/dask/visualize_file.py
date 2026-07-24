import os
import sys
import tempfile # Added for temporary local file storage

# --- FIX: Set matplotlib backend for display in graphical environments ---
try:
    import matplotlib
    # Try using TkAgg, which is usually installed by default and works well
    matplotlib.use('TkAgg') 
except ImportError:
    # If TkAgg fails (often due to missing tkinter library on Linux),
    # try Qt5Agg or warn the user about missing dependencies.
    try:
        matplotlib.use('Qt5Agg')
    except ImportError:
        print("Warning: Neither TkAgg nor Qt5Agg plotting backends are available.", file=sys.stderr)
        print("To display the plot on Ubuntu, you may need to install the system package 'python3-tk' and/or 'python3-pyqt5'.", file=sys.stderr)
        print("Run: sudo apt update && sudo apt install python3-tk python3-pyqt5", file=sys.stderr)
# ------------------------------------------------------------------------

# Re-introduced pyarrow.fs as we are now using it to download the file contents
import pyarrow.fs as pa_fs 
import rioxarray as rxr
from rasterio.io import MemoryFile # Kept, but not used in this version
import matplotlib.pyplot as plt
import numpy as np
import glob # Needed to find all .jar files for CLASSPATH

# ---------- HDFS CONFIGURATION (copied from dask_flood_mask_final.py) ----------
HDFS_HOST = "namenode"
HDFS_PORT = 8020
HDFS_OUTPUT_PATH = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/output_flood_mask.tif"

# ---------- HDFS ENVIRONMENT CONFIGURATION (MUST BE EDITED BY USER) ----------
# PyArrow needs these environment variables set locally.
HADOOP_HOME_DIR = "/home/btcchl0040/Documents/summer-project/hadoop-3.4.1-bin"
JAVA_HOME_DIR = "/usr/lib/jvm/java-11-openjdk-amd64"     # <-- CORRECTED PATH

# --- Apply environment variables if paths are provided ---
if HADOOP_HOME_DIR != "/path/to/your/hadoop/install":
    print("Setting HADOOP_HOME and CLASSPATH for PyArrow...")
    os.environ.setdefault('HADOOP_HOME', HADOOP_HOME_DIR)
    # This variable tells PyArrow where to look for libhdfs.so
    os.environ.setdefault('ARROW_LIBHDFS_DIR', os.path.join(HADOOP_HOME_DIR, 'lib', 'native'))
    
    # 1. Set CLASSPATH for JNI connectivity
    classpath_dirs = [
        os.path.join(HADOOP_HOME_DIR, 'etc', 'hadoop'), # Config files
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'common', '*.jar'),
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'common', 'lib', '*.jar'),
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'hdfs', '*.jar'),
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'hdfs', 'lib', '*.jar'),
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'mapreduce', '*.jar'),
        os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', 'mapreduce', 'lib', '*.jar'),
    ]

    # Use glob to find all actual JAR files and join them with the system path separator (:)
    jars = []
    for pattern in classpath_dirs:
        # glob.glob returns an empty list if path doesn't exist, which is fine
        jars.extend(glob.glob(pattern))
        
    classpath_str = os.pathsep.join(jars)
    os.environ.setdefault('CLASSPATH', classpath_str)
    print(f"CLASSPATH calculated with {len(jars)} JAR files.")

    
if JAVA_HOME_DIR != "/path/to/your/java/install":
    print("Setting JAVA_HOME...")
    os.environ.setdefault('JAVA_HOME', JAVA_HOME_DIR)

# Ensure ARROW_LIBHDFS_DIR is checked one more time if HADOOP_HOME is set externally
if 'HADOOP_HOME' in os.environ and 'ARROW_LIBHDFS_DIR' not in os.environ:
     os.environ.setdefault('ARROW_LIBHDFS_DIR', os.path.join(os.environ['HADOOP_HOME'], 'lib', 'native'))


def visualize_mask():
    """
    Reads the final flood mask GeoTIFF from HDFS using PyArrow, saves it to a temporary
    local file to avoid memory crashes, and displays the result using rioxarray/matplotlib.
    """
    local_path = None # Initialize local_path outside try block for cleanup
    
    try:
        # 0. Connect using PyArrow (since we know this works for auth/connection)
        print(f"Attempting to connect to HDFS at {HDFS_HOST}:{HDFS_PORT} using PyArrow...")
        hdfs = pa_fs.HadoopFileSystem(host=HDFS_HOST, port=HDFS_PORT)

        # 1. DOWNLOAD HDFS FILE TO A TEMPORARY LOCAL FILE
        print(f"Downloading HDFS file ({HDFS_OUTPUT_PATH}) to temporary local file...")
        
        # Use tempfile to ensure the file is handled safely
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            local_path = tmp_file.name
            
            try:
                # Read from HDFS input stream
                with hdfs.open_input_file(HDFS_OUTPUT_PATH) as in_stream:
                    # Read all contents and write directly to the temporary local file
                    # This still involves reading the 128MB byte string into memory momentarily,
                    # but should be tolerable in a desktop environment.
                    tmp_file.write(in_stream.readall()) 
                
            except pa_fs.FileNotFound:
                print("ERROR: Output file not found on HDFS. Please check the path.")
                return

        # 2. OPEN LOCAL FILE VIA RIOXARRAY
        print(f"Opening temporary local file via rioxarray: {local_path}")
        # rioxarray opens the local file path efficiently
        flood_mask_xr = rxr.open_rasterio(local_path, masked=True)
        
        print("Data loaded successfully.")
        
        # 3. VERIFY AND PLOT THE MASK
        
        # Assuming the mask is a single band (1, Y, X) and dtype uint8 (0s and 1s)
        mask_data = flood_mask_xr.squeeze() # Remove the band dimension if present (shape Y, X)
        
        print(f"Mask shape: {mask_data.shape}, Data Type: {mask_data.dtype}")
        # Use .values to force computation and read all data from the array
        print(f"Unique values found: {np.unique(mask_data.values)}") 
        
        if np.max(mask_data.values) > 0:
            print("Flood pixels detected (values > 0).")
        else:
            print("No flood pixels detected (mask is all zeros).")
        
        # Use a colormap suitable for binary data, often just showing 0 and 1
        plt.figure(figsize=(10, 10))
        
        # Plotting the mask.
        from matplotlib.colors import ListedColormap
        # Gray for background (0), Blue for flood (1)
        cmap = ListedColormap(['#DDDDDD', '#1f77b4']) 

        mask_data.plot.imshow(
            cmap=cmap, 
            interpolation='nearest',
            cbar_kwargs={'ticks': [0, 1], 'label': 'Flood Mask (0: No Flood, 1: Flood)'}
        )
        
        plt.title("Flood Mask Visualization (Downloaded to Local Temp File)")
        plt.xlabel("X coordinate")
        plt.ylabel("Y coordinate")
        plt.show()

    except Exception as e:
        # Generic error handling
        print(f"An unexpected error occurred during visualization: {e}", file=sys.stderr)
        
    finally:
        # Clean up the temporary local file
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            print(f"Cleaned up temporary file: {local_path}")

if __name__ == "__main__":
    visualize_mask()

