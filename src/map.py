import streamlit as st
import geopandas as gpd
import pyproj
from shapely.geometry import box, shape, Point
import folium
from streamlit_folium import st_folium
from folium.plugins import Draw
import json
import requests
from geodata_retrieval import get_capabilities_layers, fetch_geodata
import sys
import csv

# ensure max table size for windows is not reached
if sys.platform != "win32":
    csv.field_size_limit(sys.maxsize)
else:
    import ctypes
    csv.field_size_limit(ctypes.c_ulong(-1).value // 2)

# json file containing the dataset links
with open(r"D:\PROJECTS\geodata_tool\data\datasets.json") as f:
    datasets = json.load(f)["datasets"]


# ============ Streamlit folium website parts ==========
st.title('Geodata Downloader')

crs_code = st.text_input('Enter output CRS (EPSG code):', 'EPSG:28992')
m = folium.Map(location=[52.3676, 4.9041], zoom_start=5)

draw = Draw(export=True)
draw.add_to(m)

map_data = st_folium(m, width=800, height=800)

# Dataset Selection
selected_datasets = st.multiselect(
    "Select Datasets",
    options=[ds["name"] for ds in datasets]
)

@st.cache_data
def get_all_dataset_layers(datasets):
    return {
        dataset["name"]: get_capabilities_layers(dataset["url"], dataset["type"])
        for dataset in datasets
    }

dataset_layers = get_all_dataset_layers(datasets)

bbox = None

radius = st.number_input('Enter radius (in meters) for the point:', min_value=0, value=1500)

if map_data and 'all_drawings' in map_data and map_data['all_drawings']:
    # Get the last drawn object (NOTE: ONLY THE LAST DRAWN BBOX WILL THEREFORE BE USED)
    drawn_geometry = map_data['all_drawings'][-1]['geometry']

    # Check if the geometry is a polygon for bounding box
    if drawn_geometry['type'] == 'Polygon':
        drawn_geo = shape(drawn_geometry)

        minx, miny, maxx, maxy = drawn_geo.bounds
        st.write(f"Original Coordinates Bounding Box Coordinates: {minx}, {miny}, {maxx}, {maxy}")

        bbox = [minx, miny, maxx, maxy]

    # Else if geometry is a point use the radius
    elif drawn_geometry['type'] == 'Point':

        drawn_geo = shape(drawn_geometry)
        if isinstance(drawn_geo, Point):  # Check if it's actually a Point
            st.write(f"Point Coordinates: {drawn_geo.x}, {drawn_geo.y}")

            # Convert point to a bounding box using the radius
            # Convert the radius to degrees (approximate, lat/lon)
            radius_in_degrees = radius / 111320
            minx = drawn_geo.x - radius_in_degrees
            miny = drawn_geo.y - radius_in_degrees
            maxx = drawn_geo.x + radius_in_degrees
            maxy = drawn_geo.y + radius_in_degrees

            st.write(f"Generated Bounding Box Coordinates: {minx}, {miny}, {maxx}, {maxy}")
            bbox = [minx, miny, maxx, maxy]
        else:
            st.error("The drawn geometry is not a Point.")

    try:
        crs = pyproj.CRS.from_string(crs_code)
        transformer = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)

        minx_t, miny_t = transformer.transform(minx, miny)
        maxx_t, maxy_t = transformer.transform(maxx, maxy)

        st.write(f"Transformed Bounding Box ({crs_code}): {minx_t}, {miny_t}, {maxx_t}, {maxy_t}")

    except Exception as e:
        st.error(f"Error converting CRS: {e}")

c1, c2 = st.columns(2)

with c1:
    st.write("")

with c2:
    if bbox and selected_datasets:
        if st.button("Fetch Data"):
            results = fetch_geodata(selected_datasets, dataset_layers, datasets, bbox)

            # Download the selected WFS data
            for layer, data in results.items():
                if data["type"] == "WFS":
                    st.download_button(
                        label=f"Download {layer} as GeoJSON",
                        data=data["geojson"],
                        file_name=data["filename"],
                        mime="application/geo+json"
                    )