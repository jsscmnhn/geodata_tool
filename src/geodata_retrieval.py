import requests
import gzip
from io import BytesIO
import geopandas as gpd
import pandas as pd
from pyproj import Transformer
from shapely.geometry import box
from shapely.ops import transform
import os
import math
import numpy as np
from PIL import Image

import rasterio
from rasterio.io import MemoryFile
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import xml.etree.ElementTree as ET


# CHOOSE PLACE TO DOWNLOAD TO
output_dir = r"D:\PROJECTS\geodata_tool\downloads"
os.makedirs(output_dir, exist_ok=True)


def transform_bbox(bbox, from_epsg, to_epsg):
    """Transform a bbox from one CRS to another."""
    minx, miny, maxx, maxy = bbox
    transformer = Transformer.from_crs(f"EPSG:{from_epsg}", f"EPSG:{to_epsg}", always_xy=True)
    transformed = transform(transformer.transform, box(minx, miny, maxx, maxy))
    return transformed.bounds

def split_bbox_to_tiles(bbox, max_width_px, max_height_px, resolution, from_epsg=4326, to_epsg=28992):
    """
    Split a bbox into smaller bboxes so that each tile does not exceed max pixels.

    Parameters:
        bbox (tuple): (minx, miny, maxx, maxy) in from_epsg CRS
        max_width_px (int): max width in pixels per tile
        max_height_px (int): max height in pixels per tile
        resolution (float): pixel resolution in projected units per pixel
        from_epsg (int): EPSG code of input bbox CRS
        to_epsg (int): EPSG code of projected CRS

    Returns:
        list of bboxes in from_epsg CRS (tiles)
    """

    # Transform bbox to projected CRS (meters)
    if from_epsg != to_epsg:
        minx, miny, maxx, maxy = transform_bbox(bbox, from_epsg, to_epsg)
    else:
        minx, miny, maxx, maxy = bbox

    bbox_width = maxx - minx
    bbox_height = maxy - miny

    # Max tile width/height in projected units
    tile_width = max_width_px * resolution
    tile_height = max_height_px * resolution

    # Number of tiles in each direction (round up)
    nx = int(np.ceil(bbox_width / tile_width))
    ny = int(np.ceil(bbox_height / tile_height))

    tiles_proj = []
    for i in range(nx):
        for j in range(ny):
            t_minx = minx + i * tile_width
            t_maxx = min(minx + (i + 1) * tile_width, maxx)
            t_miny = miny + j * tile_height
            t_maxy = min(miny + (j + 1) * tile_height, maxy)
            tiles_proj.append((t_minx, t_miny, t_maxx, t_maxy))

    # Transform tiles back to original CRS
    tiles = [transform_bbox(tile, to_epsg, from_epsg) for tile in tiles_proj]

    return tiles


def get_wcs_coverages(wcs_url):
    capabilities_url = f"{wcs_url}?service=WCS&request=GetCapabilities&version=1.0.0"
    try:
        response = requests.get(capabilities_url, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        # WCS namespace (usually)
        ns = {'wcs': 'http://www.opengis.net/wcs'}

        # Coverage IDs can be under different tags depending on version
        coverages = []

        # For WCS 1.0.0, coverage IDs under <ContentMetadata>/<CoverageOfferingBrief>
        for coverage in root.findall('.//wcs:CoverageOfferingBrief', ns):
            cov_id = coverage.find('wcs:name', ns)
            if cov_id is not None and cov_id.text:
                coverages.append(cov_id.text)

        return coverages

    except Exception as e:
        print(f"Failed to get WCS coverages: {e}")
        return []

def get_capabilities_layers(service_url, service_type):
    def extract_layer_names(layer_element, namespace=None):
        names = []
        name_elem = layer_element.find("wms:Name", namespaces=namespace)
        if name_elem is not None and name_elem.text:
            names.append(name_elem.text)

        for child_layer in layer_element.findall("wms:Layer", namespaces=namespace):
            names.extend(extract_layer_names(child_layer, namespace))
        return names

    capabilities_url = f"{service_url}?request=GetCapabilities&service={service_type}"

    print(f"Fetching: {capabilities_url}")

    try:
        response = requests.get(capabilities_url, timeout=10)
        response.raise_for_status()

        if not response.text.strip().startswith("<"):
            print("Error: Response is not XML. Received:", response.text[:200])
            return []

        root = ET.fromstring(response.text)

        if service_type.upper() == "WMS":
            namespace = {"wms": "http://www.opengis.net/wms"}

            capability = root.find("wms:Capability", namespaces=namespace)
            if capability is None:
                print("No Capability element found in WMS XML.")
                return []

            root_layer = capability.find("wms:Layer", namespaces=namespace)
            if root_layer is None:
                print("No root Layer element found in Capability.")
                return []

            layers = extract_layer_names(root_layer, namespace)
            layers = list(set(layers))  # deduplicate if needed
            print("WMS Layers found:", layers)
            return layers

        elif service_type.upper() == "WFS":
            layers = []
            for feature in root.findall(".//FeatureType/Name"):
                layers.append(feature.text)

            if not layers:
                namespace = {"wfs": "http://www.opengis.net/wfs/2.0"}
                for feature in root.findall(".//wfs:FeatureType/wfs:Name", namespaces=namespace):
                    layers.append(feature.text)

            print("WFS layers found:", layers)
            return layers

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []
    except ET.ParseError:
        print("Error: Invalid XML response from server.")
        return []


# Function to get supported wfs but very specific for how to request CRS from WFS service so commented out for now
# def get_supported_crs(wfs_url):
#     """
#     Retrieves the set of supported CRS (Coordinate Reference Systems) from a WFS service
#     by parsing its GetCapabilities response.
#
#     Parameters:
#         wfs_url (str): The base URL of the WFS service.
#
#     Returns:
#         set: A set of CRS URNs (e.g., "urn:ogc:def:crs:EPSG::4326") supported by the service,
#              or an empty set if the request fails or no CRS entries are found.
#
#     Raises:
#         Exception: If there is a network or parsing error (caught and logged).
#     """
#     params = {
#         "service": "WFS",
#         "version": "2.0.0",
#         "request": "GetCapabilities"
#     }
#     try:
#         response = requests.get(wfs_url, params=params, timeout=10)
#         if response.status_code != 200:
#             print(f"Failed to get capabilities: {response.status_code}")
#             return set()
#
#         root = ET.fromstring(response.content)
#         ns = {'wfs': 'http://www.opengis.net/wfs/2.0'}
#         crs_set = set()
#
#         for elem in root.iter():
#             if elem.tag.endswith("DefaultCRS") or elem.tag.endswith("OtherCRS"):
#                 crs_set.add(elem.text.strip())
#
#         return crs_set
#     except Exception as e:
#         print(f"CRS check failed: {e}")
#         return set()


def sample_wcs_raster_to_points(bbox, width=100, height=100, crs='EPSG:28992', resolution=0.5,
                               sample_values=False,  # default False
                               save_geotiff=False,
                               geotiff_path='coverage.tif', geojson_path='coverage.geojson',
                               coverage_id='dsm_05m'):
    '''
    Samples a WCS raster coverage and extracts point data with corresponding values if sample_values=True.
    '''

    minx, miny, maxx, maxy = bbox
    bbox_width = maxx - minx
    bbox_height = maxy - miny

    width = int(round(bbox_width / resolution))
    height = int(round(bbox_height / resolution))


    base_url = "https://service.pdok.nl/rws/ahn/wcs/v1_0"
    params = {
        "service": "WCS",
        "version": "1.0.0",
        "request": "GetCoverage",
        "coverage": coverage_id,
        "bbox": ",".join(map(str, bbox)),
        "CRS": crs,
        "width": str(width),
        "height": str(height),
        "format": "image/tiff"
    }
    print(f"Requesting WCS coverage: {params}")
    response = requests.get(base_url, params=params)

    if response.status_code != 200:
        print(f"Failed to download WCS coverage: {response.status_code}")
        print(response.text[:1000])
        return None

    try:
        with MemoryFile(response.content) as memfile:
            with memfile.open() as src:
                if sample_values:
                    transform = src.transform
                    raster_data = src.read(1)

                    rows, cols = src.height, src.width
                    points = []
                    values = []

                    for row in range(rows):
                        for col in range(cols):
                            x, y = transform * (col + 0.5, row + 0.5)
                            value = raster_data[row, col]
                            points.append(Point(x, y))
                            values.append(value)

                    gdf = gpd.GeoDataFrame({'value': values}, geometry=points, crs=crs)
                    gdf = gdf.to_crs('EPSG:4326')
                    gdf.to_file(geojson_path, driver="GeoJSON")
                    print(f"GeoJSON saved to {geojson_path}")

                if save_geotiff:
                    with open(geotiff_path, "wb") as f:
                        f.write(response.content)
                    print(f"GeoTIFF saved to {geotiff_path}")

                if sample_values:
                    return gdf
                else:
                    return None

    except rasterio.errors.RasterioIOError:
        print("The response was not a valid GeoTIFF. Saving for inspection...")
        with open("debug_response.tiff", "wb") as f:
            f.write(response.content)
        return None


def get_map(params, base_url):
    # Try GeoTIFF first
    params["FORMAT"] = "image/geotiff"
    response = requests.get(base_url, params=params)
    if response.status_code == 200 and not b"ServiceException" in response.content:
        return response.content, "geotiff"
    else:
        # Fall back to PNG
        params["FORMAT"] = "image/png"
        response = requests.get(base_url, params=params)
        if response.status_code == 200 and not b"ServiceException" in response.content:
            return response.content, "png"
        else:
            raise Exception("Failed to get map in GeoTIFF or PNG formats")


def sort_tiles_top_left(tiles):
    '''Sort tiles top-to-bottom (min y to max y), then left-to-right (min x to max x).'''
    # Sort rows by increasing y, columns by increasing x
    return sorted(tiles, key=lambda b: (b[1], b[0]))

def calculate_grid_dims(tiles):
    '''Calculate number of columns (nx) and rows (ny) based on unique X and Y coords.'''
    x_coords = sorted(set(round(b[0], 8) for b in tiles))
    y_coords = sorted(set(round(b[1], 8) for b in tiles))  # ascending y now
    return len(x_coords), len(y_coords)

def stitch_tiles_to_bytes(tile_files, nx, ny, output_format="PNG"):
    '''Stitch tiles arranged as nx columns and ny rows into a single mosaic image.

    Assumes:
    - Tiles are sorted top-to-bottom (lowest Y at top), left-to-right
    - Only the last tile in each row (rightmost) may be narrower
    - Only the last row (topmost) may be shorter
    - Tiles are left- and top-aligned
    '''
    tiles = [Image.open(fp) for fp in tile_files]

    # Width of full columns (excluding last)
    col_widths = [max(tiles[c * ny + r].width for r in range(ny)) for c in range(nx - 1)]

    # Width of each tile in the last column
    last_col_widths = [tiles[(nx - 1) * ny + r].width for r in range(ny)]

    # Height of each row
    row_heights = [max(tiles[c * ny + r].height for c in range(nx)) for r in range(ny)]

    # Compute total width and height
    max_last_col_width = max(last_col_widths)
    total_width = sum(col_widths) + max_last_col_width
    total_height = sum(row_heights)

    mode = "RGBA" if any(tile.mode == "RGBA" for tile in tiles) else "RGB"
    mosaic = Image.new(mode, (total_width, total_height))

    y_offset = 0
    for r in range(ny):
        tile_row = ny - 1 - r  # Flip so last row is placed on top
        x_offset = 0
        for c in range(nx):
            idx = c * ny + tile_row
            tile = tiles[idx]
            mosaic.paste(tile, (x_offset, y_offset))
            if c < nx - 1:
                x_offset += col_widths[c]
            else:
                x_offset += tile.width  # last column is probably smaller
        y_offset += row_heights[tile_row]

    buffer = BytesIO()
    mosaic.save(buffer, format=output_format)
    return buffer.getvalue()



def fetch_and_stitch(tiles, tile_files):
    '''
    Given tiles bounding boxes and tile file paths:
    - Sort tiles and tile_files top-to-bottom (lowest y first), left-to-right
    - Calculate grid size nx, ny
    - Stitch tiles
    - Return combined bytes or raise error
    '''

    tiles_sorted = sort_tiles_top_left(tiles)
    tile_file_map = dict(zip(tiles, tile_files))
    tile_files_sorted = [tile_file_map[bbox] for bbox in tiles_sorted]

    nx, ny = calculate_grid_dims(tiles_sorted)

    if len(tile_files_sorted) != nx * ny:
        raise ValueError(f"Mismatch between tiles ({len(tile_files_sorted)}) and grid size ({nx}x{ny}={nx*ny})")

    combined_bytes = stitch_tiles_to_bytes(tile_files_sorted, nx, ny, output_format="PNG")
    return combined_bytes

def fetch_geodata(selected_datasets, dataset_layers, datasets, bbox, sample_values=False, save_geotiff=False, resolution=1):
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
    print(selected_datasets)
    for dataset in datasets:
        if dataset["name"] in selected_datasets:
            layers = dataset_layers.get(dataset["name"], [])
            # supported_crs = get_supported_crs(dataset["url"])

            from_epsg = 4326
            to_epsg = 28992 # if "urn:ogc:def:crs:EPSG::28992" in supported_crs else 4326
            preferred_crs = f"EPSG:{to_epsg}"

            if to_epsg != from_epsg:
                minx, miny, maxx, maxy = transform_bbox(bbox, from_epsg, to_epsg)

            if dataset["type"] == "WFS":
                for layer in layers:
                    all_features = []
                    start_index = 0
                    count = 1000  # limit of dutch requests

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

                        filename = os.path.join(output_dir, f"{layer.replace(':', '_')}_4326.geojson")
                        full_gdf.to_file(filename, driver='GeoJSON')

                        for col in full_gdf.columns:
                            if pd.api.types.is_datetime64_any_dtype(full_gdf[col]):
                                full_gdf[col] = full_gdf[col].astype(str)

                        geojson = full_gdf.to_json()
                        results[layer] = {
                            "type": "WFS",
                            "geojson": geojson,
                            "filename": filename
                        }

                        print(f"Saved {layer} to {filename}")



            elif dataset["type"] == "WMS":
                max_pixels = 4000

                # IMPORTANT: ONLY USE THE MOST RECENT LUCHTFOTO OTHERWISE VERY SLOW IN FETCHING
                if dataset.get("name") == "Luchtfoto RGB (WMS)":
                    layers = ['Actueel_ortho25']
                    max_pixels = 2500
                    resolution=0.25

                for layer in layers:
                    try:
                        width = int(round((maxx - minx) / resolution))
                        height = int(round((maxy - miny) / resolution))

                        if width > max_pixels or height > max_pixels:
                            print(f"Requested size {width}x{height} exceeds max {max_pixels}, splitting into tiles")

                            # Split bbox into tiles that fit within max_pixels
                            tiles = split_bbox_to_tiles(
                                bbox=(minx, miny, maxx, maxy),
                                max_width_px=max_pixels,
                                max_height_px=max_pixels,
                                resolution=resolution,
                                from_epsg=to_epsg,
                                to_epsg=to_epsg
                            )

                            tile_files = []
                            for idx, tile_bbox in enumerate(tiles):
                                tile_minx, tile_miny, tile_maxx, tile_maxy = tile_bbox

                                # Validate tile bbox
                                coords = [tile_minx, tile_miny, tile_maxx, tile_maxy]
                                if any([math.isnan(c) or math.isinf(c) for c in coords]):
                                    print(f"Skipping tile {idx} due to invalid bbox coordinates: {coords}")
                                    continue

                                # Ensure bbox min < max
                                if tile_maxx <= tile_minx or tile_maxy <= tile_miny:
                                    print(f"Skipping tile {idx} because bbox min >= max: {coords}")
                                    continue

                                # Calculate tile width/height in pixels
                                tile_width_f = (tile_maxx - tile_minx) / resolution
                                tile_height_f = (tile_maxy - tile_miny) / resolution

                                # Check for valid sizes
                                if tile_width_f <= 0 or tile_height_f <= 0 or np.isnan(tile_width_f) or np.isnan(
                                        tile_height_f):
                                    print(
                                        f"Skipping tile {idx} due to invalid size: width={tile_width_f}, height={tile_height_f}")
                                    continue

                                tile_width = int(round(tile_width_f))
                                tile_height = int(round(tile_height_f))

                                print(
                                    f"Requesting tile {idx}: BBOX={tile_bbox} WIDTH={tile_width} HEIGHT={tile_height}")

                                # Most WMS either have png or jpeg
                                for img_format in ["image/png", "image/jpeg"]:
                                    params = {
                                        "SERVICE": "WMS",
                                        "REQUEST": "GetMap",
                                        "VERSION": "1.3.0",
                                        "LAYERS": layer,
                                        "CRS": preferred_crs,
                                        "BBOX": f"{tile_minx},{tile_miny},{tile_maxx},{tile_maxy}",
                                        "WIDTH": tile_width,
                                        "HEIGHT": tile_height,
                                        "FORMAT": img_format,
                                        "TRANSPARENT": True,
                                        "STYLES": ""
                                    }

                                    response = requests.get(dataset["url"], params=params)
                                    if response.status_code == 200 and response.content:
                                        tile_filename = os.path.join(output_dir,
                                                                     f"{layer}_tile_{idx}.png")
                                        with open(tile_filename, "wb") as f:
                                            f.write(response.content)
                                        tile_files.append(tile_filename)
                                        print(f"Saved tile {idx} to {tile_filename}")
                                        break
                                else:
                                    print(f"Failed to fetch tile {idx} for layer {layer}: HTTP {response.status_code}")

                            # Stitching tiles into one image in memory & store bytes in results
                            try:
                                # Calculate number of tiles horizontally (nx) and vertically (ny)
                                # Tiles are ordered left->right, top->bottom
                                x_coords = sorted(set(round(tb[0], 8) for tb in tiles))
                                y_coords = sorted(set(round(tb[1], 8) for tb in tiles))
                                nx = len(x_coords)
                                ny = len(y_coords)

                                combined_bytes = stitch_tiles_to_bytes(tile_files, nx, ny, output_format="PNG")

                                results[layer] = {
                                    "type": "WMS",
                                    "data_bytes": combined_bytes,
                                    "format": "image/png"
                                }

                                # Deleting tiles after download
                                for f in tile_files:
                                    os.remove(f)

                            except Exception as e:
                                print(f"Failed stitching tiles for layer {layer}: {e}")
                                results[layer] = {
                                    "type": "WMS",
                                    "tiles": tile_files
                                }

                        else:
                            for img_format in ["image/png", "image/jpeg"]:
                                params = {
                                    "SERVICE": "WMS",
                                    "REQUEST": "GetMap",
                                    "VERSION": "1.3.0",
                                    "LAYERS": layer,
                                    "CRS": preferred_crs,
                                    "BBOX": f"{minx},{miny},{maxx},{maxy}",
                                    "WIDTH": width,
                                    "HEIGHT": height,
                                    "FORMAT": img_format,
                                    "TRANSPARENT": True,
                                    "STYLES": ""
                                }

                                response = requests.get(dataset["url"], params=params,
                                                        headers={"User-Agent": "Mozilla/5.0"})

                                if response.status_code == 200:
                                    image_bytes = response.content
                                    results[layer] = {
                                        "type": "WMS",
                                        "data_bytes": image_bytes,
                                        "url": response.url,
                                        "format": img_format
                                    }
                                    break

                            if response.status_code == 200:
                                image_bytes = response.content
                                results[layer] = {
                                    "type": "WMS",
                                    "data_bytes": image_bytes,
                                    "url": response.url

                                }

                            else:
                                print(f"Failed to download WMS layer {layer}: HTTP {response.status_code}")


                    except Exception as e:
                        print(f"Exception fetching WMS layer {layer}: {e}")


            elif dataset["type"] == "WCS":
                geojson_path = os.path.join(output_dir, f"{layer}_points.geojson")
                geotiff_path = os.path.join(output_dir, f"{layer}.tif")

                try:
                    gdf = sample_wcs_raster_to_points(
                        bbox=(minx, miny, maxx, maxy),
                        crs=f"EPSG:{to_epsg}",
                        save_geotiff=save_geotiff,
                        sample_values=sample_values,
                        geotiff_path=geotiff_path,
                        geojson_path=geojson_path
                    )

                    if gdf is not None:
                        results[layer] = {
                            "type": "WCS",
                            "geojson": gdf.to_json(),
                            "filename": geojson_path,
                            "geotiff": geotiff_path
                        }


                except Exception as e:
                    print(f"Failed to fetch WCS layer {layer}: {e}")
    return results