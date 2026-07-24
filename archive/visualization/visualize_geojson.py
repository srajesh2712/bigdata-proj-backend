import geopandas as gpd
import matplotlib.pyplot as plt

gdf = gpd.read_file("/home/btcchl0040/Documents/SAR_Data/OUTPUT/predicted_flood_assam_june_2025.geojson")
gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0005, preserve_topology=True)

gdf.plot(column="value", cmap="Blues", legend=True, edgecolor='black')
plt.title("Predicted Flood Map - June 2025")
plt.show()
