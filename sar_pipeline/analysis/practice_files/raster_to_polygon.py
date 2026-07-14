import rasterio
from rasterio.features import shapes
import geopandas as gpd
import json
import numpy as np

# Path to your flood mask GeoTIFF
tif_path = r"/home/btcchl0040/Documents/SAR_Data/20250803_163826/FLOOD_MASK/tile_4096_2048_mask.tif"

with rasterio.open(tif_path) as src:
    print(f' crs is {src.crs}')
    band = src.read(1)
    mask = band > 0.5  # Apply threshold to extract flooded areas
    results = (
        {"properties": {"value": v}, "geometry": s}
        for s, v in shapes(band, mask=mask, transform=src.transform)
    )

    geoms = list(results)

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame.from_features(geoms)
gdf = gdf[gdf['value'] == 1]  # Only flooded areas

# Save as GeoJSON (place inside /data folder mapped to Docker)
output_path = r"/home/btcchl0040/Documents/SAR_Data/20250803_163826/FLOOD_MASK/tile_4096_2048_mask.geojson"
gdf.set_crs("EPSG:4326", inplace=True)
gdf.to_file(output_path, driver="GeoJSON")




