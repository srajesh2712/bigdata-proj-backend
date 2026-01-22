import os
import rasterio

# 1. Setup the environment
os.environ['GDAL_HADOOP_VFS'] = 'YES'
# Replace with your actual path
path = "hdfs://namenode:8020/user/btcchl0040/sar/processed/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif"
vsi_path = path.replace("hdfs://namenode:8020", "/vsihdfs")

print(f"Testing connection to: {vsi_path}")

try:
    with rasterio.open(vsi_path) as src:
        print("Success! File metadata:")
        print(src.profile)
except Exception as e:
    print(f"Failed! Error: {e}")
    print("\nPossible fix: Run 'export CLASSPATH=$(hadoop classpath)' in your terminal first.")