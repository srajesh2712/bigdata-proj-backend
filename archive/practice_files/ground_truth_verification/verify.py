import geopandas as gpd
import fiona
import os

# Path to the UNZIPPED .gdb FOLDER
gdb_path = "/home/btcchl0040/Documents/SAR_Data/Remotely_Sensed_Flood_Estimates.gdb"

try:
    if not os.path.exists(gdb_path):
        print(f"❌ GDB folder not found. Check the unzip output.")
    else:
        # 1. List the layers in the GDB to find the right one
        layers = fiona.listlayers(gdb_path)
        print(f"✅ Layers found in GDB: {layers}")
        
        # We'll pick the first layer (usually the main one)
        target_layer = layers[0]
        print(f"Reading layer: {target_layer}...")
        
        # 2. Read the specific layer
        gdf = gpd.read_file(gdb_path, layer=target_layer)
        
        print(f"--- Success! Loaded {len(gdf)} rows ---")

        # 3. Search for the 2025 Manchester Flood
        gdf['date_time'] = gdf['date_time'].astype(str)
        jan_2025 = gdf[gdf['date_time'].str.contains('2025-01', na=False)]
        
        if not jan_2025.empty:
            print("🎉 FOUND 2025 DATA!")
            print("Unique Dates:", jan_2025['date_time'].unique())
            
            # Save for your React App validation
            # Manchester BNG: 380000, 390000
            mcr_gt = jan_2025.cx[380000:395000, 385000:395000]
            mcr_gt.to_file("manchester_ground_truth_2025.geojson", driver='GeoJSON')
            print("Saved 'manchester_ground_truth_2025.geojson'")
        else:
            print("⚠️ Still only seeing older data. Latest date is:")
            print(gdf['date_time'].sort_values().tail(1).values)

except Exception as e:
    print(f"❌ Error: {e}")
