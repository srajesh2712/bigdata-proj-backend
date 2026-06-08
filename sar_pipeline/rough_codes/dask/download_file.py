import os
import pyarrow.fs as pa_fs 
import glob
import sys
import pyarrow.lib as pa_lib # Imported pyarrow.lib to access FileNotFoundError

# ---------- HDFS CONFIGURATION (Adjust these if needed) ----------
HDFS_HOST = "namenode"
HDFS_PORT = 8020
HDFS_OUTPUT_PATH = "/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/output_flood_mask.tif"
LOCAL_OUTPUT_PATH = "downloaded_flood_mask.tif"

# ---------- HDFS ENVIRONMENT CONFIGURATION (Crucial for PyArrow) ----------
HADOOP_HOME_DIR = "/home/btcchl0040/Documents/summer-project/hadoop-3.4.1-bin"
JAVA_HOME_DIR = "/usr/lib/jvm/java-11-openjdk-amd64" 

# --- Apply environment variables ---
print("Setting HADOOP_HOME, JAVA_HOME, and CLASSPATH...")

os.environ.setdefault('HADOOP_HOME', HADOOP_HOME_DIR)
os.environ.setdefault('JAVA_HOME', JAVA_HOME_DIR)
os.environ.setdefault('ARROW_LIBHDFS_DIR', os.path.join(HADOOP_HOME_DIR, 'lib', 'native'))

# Calculate CLASSPATH
classpath_dirs = [
    os.path.join(HADOOP_HOME_DIR, 'etc', 'hadoop'),
    os.path.join(HADOOP_HOME_DIR, 'share', 'hadoop', '**', '*.jar'), # Use ** for deep search
]
jars = []
for pattern in classpath_dirs:
    # Recursively find all JAR files
    jars.extend(glob.glob(pattern, recursive=True)) 
    
classpath_str = os.pathsep.join(jars)
os.environ.setdefault('CLASSPATH', classpath_str)
print(f"CLASSPATH calculated with {len(jars)} JAR files.")


def download_hdfs_file_to_local():
    """
    Connects to HDFS using PyArrow and downloads the specified file to the local path.
    """
    try:
        print(f"Attempting to connect to HDFS at {HDFS_HOST}:{HDFS_PORT}...")
        hdfs = pa_fs.HadoopFileSystem(host=HDFS_HOST, port=HDFS_PORT)

        print(f"Downloading HDFS file ({HDFS_OUTPUT_PATH}) to local file: {LOCAL_OUTPUT_PATH}")
        
        # Read from HDFS input stream
        with hdfs.open_input_file(HDFS_OUTPUT_PATH) as in_stream:
            # Write contents to local file
            with open(LOCAL_OUTPUT_PATH, 'wb') as out_file:
                out_file.write(in_stream.readall()) 
            
        print(f"\n✅ Download successful! File saved to: {os.path.abspath(LOCAL_OUTPUT_PATH)}")

    except pa_lib.FileNotFoundError: # Catching the specific PyArrow exception
        print(f"\nERROR: Output file not found on HDFS at path: {HDFS_OUTPUT_PATH}", file=sys.stderr)
        
    except Exception as e:
        print(f"\nAn error occurred during download: {e}", file=sys.stderr)

if __name__ == "__main__":
    download_hdfs_file_to_local()

