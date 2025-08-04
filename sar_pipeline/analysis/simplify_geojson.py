import geopandas as gpd

output_path = r"/home/btcchl0040/Documents/SAR_Data/flood_mask.geojson"
# Load original large GeoJSON
gdf = gpd.read_file(output_path)

# Simplify geometries (tolerance in degrees, e.g., 0.0001)
gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.001, preserve_topology=True)

# Save to a new GeoJSON or GeoPackage
gdf.to_file("/home/btcchl0040/Documents/SAR_Data/flood_mask_simplified.geojson", driver="GeoJSON")
# or smaller binary format:
gdf.to_file("/home/btcchl0040/Documents/SAR_Data/flood_mask_simplified.gpkg", driver="GPKG")


