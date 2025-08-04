import streamlit as st
import leafmap.foliumap as leafmap

st.set_page_config(layout="wide")
st.title("Flooded Areas Map")

# Create a map centered on Assam
m = leafmap.Map(center=[26.2, 91.7], zoom=8)

# Add GeoJSON layer (update the path if needed)
geojson_path = "/home/btcchl0040/Documents/SAR_Data/20250803_163826/FLOOD_MASK/tile_4096_2048_merged.geojson"

try:
    m.add_geojson(geojson_path, layer_name="Flooded Areas")
except Exception as e:
    st.error(f"Failed to load GeoJSON: {e}")

# Show the map in Streamlit
m.to_streamlit(height=700)
