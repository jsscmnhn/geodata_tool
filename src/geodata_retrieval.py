import requests
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO
import geopandas as gpd
import pandas as pd
import pyproj
from shapely.geometry import box
from shapely.ops import transform
from functools import partial

def transform_bbox(bbox, from_epsg, to_epsg):
    """Transform a bbox from one CRS to another."""
    minx, miny, maxx, maxy = bbox
    project = partial(
        pyproj.transform,
        pyproj.Proj(init=f'epsg:{from_epsg}'),
        pyproj.Proj(init=f'epsg:{to_epsg}')
    )
    transformed = transform(project, box(minx, miny, maxx, maxy))
    return transformed.bounds

def get_capabilities_layers(service_url, service_type):
    """
     Fetches and parses the available layers from a WMS or WFS service by querying
     its GetCapabilities endpoint.

     Parameters:
         service_url (str): The URL of the WMS or WFS service.
         service_type (str): The type of service ("WMS" or "WFS").

     Returns:
         list: A list of layer names (for WMS) or feature type names (for WFS)
               available in the service, or an empty list if the request fails or no layers are found.

     Raises:
         requests.exceptions.RequestException: If the request to the service fails.
         xml.etree.ElementTree.ParseError: If the response is not valid XML.
     """
    capabilities_url = f"{service_url}?request=GetCapabilities&service={service_type}"

    print(f"Fetching: {capabilities_url}")

    try:
        response = requests.get(capabilities_url, timeout=10)
        response.raise_for_status()

        if not response.text.strip().startswith("<"):
            print("Error: Response is not XML. Received:", response.text[:200])
            return []

        root = ET.fromstring(response.text)
        layers = []

        if service_type.upper() == "WMS":
            for layer in root.findall(".//Layer/Name"):
                layers.append(layer.text)

            if not layers:
                namespace = {"wms": "http://www.opengis.net/wms/1.3.0"}
                for layer in root.findall(".//wms:Layer/wms:Name", namespaces=namespace):
                    layers.append(layer.text)

        elif service_type.upper() == "WFS":
            for feature in root.findall(".//FeatureType/Name"):
                layers.append(feature.text)

            if not layers:
                namespace = {"wfs": "http://www.opengis.net/wfs/2.0"}
                for feature in root.findall(".//wfs:FeatureType/wfs:Name", namespaces=namespace):
                    layers.append(feature.text)

        print(layers)
        return layers

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []
    except ET.ParseError:
        print("Error: Invalid XML response from server.")
        return []


def get_supported_crs(wfs_url):
    """
    Retrieves the set of supported CRS (Coordinate Reference Systems) from a WFS service
    by parsing its GetCapabilities response.

    Parameters:
        wfs_url (str): The base URL of the WFS service.

    Returns:
        set: A set of CRS URNs (e.g., "urn:ogc:def:crs:EPSG::4326") supported by the service,
             or an empty set if the request fails or no CRS entries are found.

    Raises:
        Exception: If there is a network or parsing error (caught and logged).
    """
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetCapabilities"
    }
    try:
        response = requests.get(wfs_url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"Failed to get capabilities: {response.status_code}")
            return set()

        root = ET.fromstring(response.content)
        ns = {'wfs': 'http://www.opengis.net/wfs/2.0'}
        crs_set = set()

        for elem in root.iter():
            if elem.tag.endswith("DefaultCRS") or elem.tag.endswith("OtherCRS"):
                crs_set.add(elem.text.strip())

        return crs_set
    except Exception as e:
        print(f"CRS check failed: {e}")
        return set()


def fetch_geodata(selected_datasets, dataset_layers, datasets, bbox):
    """
     Fetches geospatial data for selected datasets and layers from WFS or WMS services
     within a given bounding box.

     For WFS layers, it downloads features in GeoJSON format (handling pagination),
     reprojects to EPSG:4326 if necessary, and saves the result to disk.
     For WMS layers, it constructs a GetMap URL for the bounding box.

     Parameters:
         selected_datasets (list of str): Names of datasets selected by the user.
         dataset_layers (dict): Mapping from dataset name to a list of layer names.
         datasets (list of dict): Each dict should contain keys 'name', 'url', and 'type' ('WFS' or 'WMS').
         bbox (tuple): Bounding box as (minx, miny, maxx, maxy) in EPSG:4326.

     Returns:
         dict: A dictionary where keys are layer names and values contain:
               - type: 'WFS' or 'WMS'
               - geojson and filename for WFS layers
               - URL for WMS layers

     Raises:
         requests.exceptions.RequestException: If any service request fails (caught and logged).
     """

    minx, miny, maxx, maxy = bbox
    results = {}

    for dataset in datasets:
        if dataset["name"] in selected_datasets:
            layers = dataset_layers.get(dataset["name"], [])
            supported_crs = get_supported_crs(dataset["url"])

            from_epsg = 4326
            to_epsg = 28992 if "urn:ogc:def:crs:EPSG::28992" in supported_crs else 4326
            preferred_crs = f"urn:ogc:def:crs:EPSG::{to_epsg}"

            if to_epsg != from_epsg:
                minx, miny, maxx, maxy = transform_bbox(bbox, from_epsg, to_epsg)

            for layer in layers:
                if dataset["type"] == "WFS":
                    all_features = []
                    start_index = 0
                    count = 1000

                    while True:
                        params = {
                            "SERVICE": "WFS",
                            "REQUEST": "GetFeature",
                            "VERSION": "2.0.0",
                            "TYPENAMES": layer,
                            "SRSNAME": preferred_crs,
                            "BBOX": f"{minx},{miny},{maxx},{maxy},{preferred_crs}",
                            "COUNT": count,
                            "STARTINDEX": start_index,
                            "OUTPUTFORMAT": "application/json"
                        }

                        headers = {"User-Agent": "Mozilla/5.0 QGIS/33411/Windows 11 Version 2009"}
                        response = requests.get(dataset["url"], params=params, headers=headers)

                        if response.status_code == 200:
                            if response.headers.get('Content-Encoding', '').lower() == 'gzip' and response.content[:2] == b'\x1f\x8b':
                                data = gzip.decompress(response.content)
                            else:
                                data = response.content

                            with BytesIO(data) as f:
                                gdf = gpd.read_file(f)
                                print(f"Layer {layer} returned {len(gdf)} features")

                            all_features.append(gdf)

                            if len(gdf) < count:
                                break

                            start_index += count
                        else:
                            print(f"Failed to download WFS data. Status code: {response.status_code}")
                            print(f"Error message: {response.text}")
                            break

                    if all_features:
                        full_gdf = gpd.GeoDataFrame(pd.concat(all_features, ignore_index=True))

                        if full_gdf.crs != "EPSG:4326":
                            full_gdf = full_gdf.to_crs("EPSG:4326")

                        full_gdf = full_gdf[full_gdf.geometry.notnull()]
                        full_gdf = full_gdf[full_gdf.is_valid]

                        filename = f"{layer.replace(':', '_')}_4326.geojson"
                        full_gdf.to_file(filename, driver='GeoJSON')

                        geojson = full_gdf.to_json()
                        results[layer] = {
                            "type": "WFS",
                            "geojson": geojson,
                            "filename": filename
                        }

                        print(f"Saved {layer} to {filename}")

                elif dataset["type"] == "WMS":
                    wms_url = (
                        f"{dataset['url']}?service=WMS&request=GetMap"
                        f"&layers={layer}&bbox={minx},{miny},{maxx},{maxy}"
                        f"&width=500&height=500&srs=EPSG:4326&format=image/png"
                    )
                    results[layer] = {
                        "type": "WMS",
                        "url": wms_url
                    }

    return results