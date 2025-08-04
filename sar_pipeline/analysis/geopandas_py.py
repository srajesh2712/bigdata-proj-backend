import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
import geopandas as gpd

flood_polygons = []

with rasterio.open("../assets/predicted_flood_June2024.tif") as src:
    mask = src.read(1)
    transform = src.transform
    crs = src.crs

    for geom, value in shapes(mask, mask=(mask == 1), transform=transform):
        geom_shape = shape(geom)
        flood_polygons.append({"geometry": geom_shape, "value": 1})

# Convert to GeoDataFrame
flood_gdf = gpd.GeoDataFrame(flood_polygons, crs=crs)

# Optional: Save to file
flood_gdf.to_file("flood_polygons.geojson", driver="GeoJSON")
