import folium
import geopandas as gpd
import json

# STEP 1: Load only flooded areas
gdf = gpd.read_file("/home/btcchl0040/Documents/SAR_Data/OUTPUT/predicted_flood_assam_june_2025.geojson")
gdf = gdf[gdf["value"] == 1]

# STEP 2: Buffer fix (geometry repair)
gdf["geometry"] = gdf["geometry"].buffer(0)

# STEP 3: Explode multipolygons to smaller pieces (optional, improves dissolve)
gdf = gdf.explode(index_parts=False)

# STEP 4: Simplify geometry
gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.1, preserve_topology=True)

# STEP 5: Dissolve all into one shape to reduce file size
gdf = gdf[["value", "geometry"]]
gdf_merged = gdf.dissolve(by="value")

# STEP 6: Save to simplified GeoJSON
merged_geojson_path = "merged_simplified_flood.geojson"
gdf_merged.to_file(merged_geojson_path, driver="GeoJSON")

# STEP 7: Load simplified geojson as dict
with open(merged_geojson_path) as f:
    geojson_data = json.load(f)

# STEP 8: Create map and focus to bounding box
bounds = gdf_merged.total_bounds  # [minx, miny, maxx, maxy]
m = folium.Map(tiles="OpenStreetMap")
m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

# STEP 9: Add GeoJson overlay with styling
folium.GeoJson(
    geojson_data,
    style_function=lambda feature: {
        'fillColor': 'blue',
        'color': 'black',
        'weight': 0.5,
        'fillOpacity': 0.4
    },
    name="Flooded Area"
).add_to(m)

folium.LayerControl().add_to(m)

# STEP 10: Save final map
m.save("flood_map_on_basemap.html")
print("✅ Map saved as flood_map_on_basemap.html")
