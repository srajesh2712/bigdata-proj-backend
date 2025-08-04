import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon

# Load GeoJSON
gdf = gpd.read_file("/home/btcchl0040/Documents/SAR_Data/20250803_163826/FLOOD_MASK/tile_4096_2048_mask.geojson")

# Filter flooded areas
flooded = gdf[gdf['value'] == 1]

# Project to UTM (meters) for buffering
flooded_proj = flooded.to_crs(epsg=32646)

# Apply buffer (e.g., 10 meters to merge nearby polygons)
flooded_buffered = flooded_proj.buffer(10)
      
# Merge all geometries
merged_geom = flooded_buffered.union_all()

# Handle different geometry types
if isinstance(merged_geom, (MultiPolygon, Polygon)):
    merged_gdf = gpd.GeoDataFrame(geometry=[merged_geom], crs=flooded_proj.crs)
else:
    raise ValueError("Unexpected geometry type after union.")

# Simplify the geometry (5 meter tolerance)
merged_gdf['geometry'] = merged_gdf['geometry'].simplify(tolerance=5, preserve_topology=True)

# Reproject back to WGS84 (lat/lon)
final_gdf = merged_gdf.to_crs(epsg=4326)

# Save to GeoJSON
final_gdf.to_file("/home/btcchl0040/Documents/SAR_Data/20250803_163826/FLOOD_MASK/tile_4096_2048_merged.geojson", driver="GeoJSON")
