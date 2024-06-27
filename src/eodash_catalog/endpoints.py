import json
import os
import uuid
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter

import requests
from dateutil import parser
from pystac import Catalog, Collection, Item, Link, SpatialExtent, Summaries
from pystac_client import Client

from eodash_catalog.sh_endpoint import get_SH_token
from eodash_catalog.stac_handling import (
    add_collection_information,
    add_example_info,
    get_or_create_collection_and_times,
)
from eodash_catalog.thumbnails import generate_thumbnail
from eodash_catalog.utils import (
    Options,
    create_geojson_point,
    generate_veda_cog_link,
    retrieveExtentFromWMSWMTS,
)


def process_STAC_Datacube_Endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog
) -> Collection:
    collection, _ = get_or_create_collection_and_times(
        catalog, data["Name"], data, config, endpoint
    )
    add_visualization_info(collection, data, endpoint)

    stac_endpoint_url = endpoint["EndPoint"]
    if endpoint.get("Name") == "xcube":
        stac_endpoint_url = stac_endpoint_url + endpoint.get("StacEndpoint", "")
    # assuming /search not implemented
    api = Client.open(stac_endpoint_url)
    collection_id = endpoint.get("CollectionId", "datacubes")
    coll = api.get_collection(collection_id)
    if not coll:
        raise ValueError(f"Collection {collection_id} not found in endpoint {endpoint}")
    item_id = endpoint.get("DatacubeId", "")
    item = coll.get_item(item_id)
    if not item:
        raise ValueError(f"Item  {item_id} not found in collection {coll}")
    # slice a datacube along temporal axis to individual items, selectively adding properties
    dimensions = item.properties.get("cube:dimensions", {})
    variables = item.properties.get("cube:variables", {})
    if endpoint.get("Variable") not in variables:
        raise Exception(f'Variable {endpoint.get("Variable")} not found in datacube {variables}')
    time_dimension = "time"
    for k, v in dimensions.items():
        if v.get("type") == "temporal":
            time_dimension = k
            break
    time_entries = dimensions.get(time_dimension).get("values")
    for t in time_entries:
        item = Item(
            id=t,
            bbox=item.bbox,
            properties={},
            geometry=item.geometry,
            datetime=parser.isoparse(t),
        )
        link = collection.add_item(item)
        link.extra_fields["datetime"] = t
        # bubble up information we want to the link
        item_datetime = item.get_datetime()
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            link.extra_fields["datetime"] = item_datetime.isoformat()[:-6] + "Z"
        else:
            link.extra_fields["start_datetime"] = item.properties["start_datetime"]
            link.extra_fields["end_datetime"] = item.properties["end_datetime"]
    unit = variables.get(endpoint.get("Variable")).get("unit")
    if unit and "yAxis" not in data:
        data["yAxis"] = unit
    collection.update_extent_from_items()

    add_collection_information(config, collection, data)

    return collection


def handle_STAC_based_endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog, options: Options, headers=None
) -> Collection:
    if "Locations" in data:
        root_collection, _ = get_or_create_collection_and_times(
            catalog, data["Name"], data, config, endpoint
        )
        for location in data["Locations"]:
            if "FilterDates" in location:
                collection = process_STACAPI_Endpoint(
                    config=config,
                    endpoint=endpoint,
                    data=data,
                    catalog=catalog,
                    options=options,
                    headers=headers,
                    bbox=",".join(map(str, location["Bbox"])),
                    filter_dates=location["FilterDates"],
                    root_collection=root_collection,
                )
            else:
                collection = process_STACAPI_Endpoint(
                    config=config,
                    endpoint=endpoint,
                    data=data,
                    catalog=catalog,
                    options=options,
                    headers=headers,
                    bbox=",".join(map(str, location["Bbox"])),
                    root_collection=root_collection,
                )
            # Update identifier to use location as well as title
            # TODO: should we use the name as id? it provides much more
            # information in the clients
            collection.id = location.get("Identifier", uuid.uuid4())
            collection.title = location.get("Name")
            # See if description should be overwritten
            if "Description" in location:
                collection.description = location["Description"]
            else:
                collection.description = location["Name"]
            # TODO: should we remove all assets from sub collections?
            link = root_collection.add_child(collection)
            latlng = f"{location["Point"][1]},{location["Point"][0]}"
            # Add extra properties we need
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["name"] = location["Name"]
            add_example_info(collection, data, endpoint, config)
            if "OverwriteBBox" in location:
                collection.extent.spatial = SpatialExtent(
                    [
                        location["OverwriteBBox"],
                    ]
                )
        root_collection.update_extent_from_items()
        # Add bbox extents from children
        for c_child in root_collection.get_children():
            if isinstance(c_child, Collection):
                root_collection.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
    else:
        if "Bbox" in endpoint:
            root_collection = process_STACAPI_Endpoint(
                config=config,
                endpoint=endpoint,
                data=data,
                catalog=catalog,
                options=options,
                headers=headers,
                bbox=",".join(map(str, endpoint["Bbox"])),
            )
        else:
            root_collection = process_STACAPI_Endpoint(
                config=config,
                endpoint=endpoint,
                data=data,
                catalog=catalog,
                options=options,
                headers=headers,
            )

    add_example_info(root_collection, data, endpoint, config)
    return root_collection


def process_STACAPI_Endpoint(
    config: dict,
    endpoint: dict,
    data: dict,
    catalog: Catalog,
    options: Options,
    headers: dict[str, str] | None = None,
    bbox=None,
    root_collection: Collection | None = None,
    filter_dates: list[str] | None = None,
) -> Collection:
    if headers is None:
        headers = {}
    collection, _ = get_or_create_collection_and_times(
        catalog, endpoint["CollectionId"], data, config, endpoint
    )

    api = Client.open(endpoint["EndPoint"], headers=headers)
    if bbox is None:
        bbox = [-180, -90, 180, 90]
    results = api.search(
        collections=[endpoint["CollectionId"]],
        bbox=bbox,
        datetime=["1900-01-01T00:00:00Z", "3000-01-01T00:00:00Z"],
    )
    # We keep track of potential duplicate times in this list
    added_times = {}
    for item in results.items():
        item_datetime = item.get_datetime()
        if item_datetime is not None:
            iso_date = item_datetime.isoformat()[:10]
            # if filterdates has been specified skip dates not listed in config
            if filter_dates and iso_date not in filter_dates:
                continue
            if iso_date in added_times:
                continue
            added_times[iso_date] = True
        link = collection.add_item(item)
        if options.tn:
            if "cog_default" in item.assets:
                generate_thumbnail(item, data, endpoint, item.assets["cog_default"].href)
            else:
                generate_thumbnail(item, data, endpoint)
        # Check if we can create visualization link
        if "Assets" in endpoint:
            add_visualization_info(item, data, endpoint, item.id)
            link.extra_fields["item"] = item.id
        elif "cog_default" in item.assets:
            add_visualization_info(item, data, endpoint, item.assets["cog_default"].href)
            link.extra_fields["cog_href"] = item.assets["cog_default"].href
        elif item_datetime:
            time_string = item_datetime.isoformat()[:-6] + "Z"
            add_visualization_info(item, data, endpoint, time=time_string)
        elif "start_datetime" in item.properties and "end_datetime" in item.properties:
            add_visualization_info(
                item,
                data,
                endpoint,
                time="{}/{}".format(
                    item.properties["start_datetime"], item.properties["end_datetime"]
                ),
            )
        # If a root collection exists we point back to it from the item
        if root_collection:
            item.set_collection(root_collection)

        # bubble up information we want to the link
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            iso_time = item_datetime.isoformat()[:-6] + "Z"
            if endpoint["Name"] == "Sentinel Hub":
                # for SH WMS we only save the date (no time)
                link.extra_fields["datetime"] = iso_date
            else:
                link.extra_fields["datetime"] = iso_time
        else:
            link.extra_fields["start_datetime"] = item.properties["start_datetime"]
            link.extra_fields["end_datetime"] = item.properties["end_datetime"]

    collection.update_extent_from_items()

    # replace SH identifier with catalog identifier
    collection.id = data["Name"]
    add_collection_information(config, collection, data)

    # Check if we need to overwrite the bbox after update from items
    if "OverwriteBBox" in endpoint:
        collection.extent.spatial = SpatialExtent(
            [
                endpoint["OverwriteBBox"],
            ]
        )

    return collection


def handle_VEDA_endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog, options: Options
) -> Collection:
    collection = handle_STAC_based_endpoint(config, endpoint, data, catalog, options)
    return collection


def handle_collection_only(
    config: dict, endpoint: dict, data: dict, catalog: Catalog
) -> Collection:
    collection, times = get_or_create_collection_and_times(
        catalog, data["Name"], data, config, endpoint
    )
    if len(times) > 0 and not endpoint.get("Disable_Items"):
        for t in times:
            item = Item(
                id=t,
                bbox=endpoint.get("OverwriteBBox"),
                properties={},
                geometry=None,
                datetime=parser.isoparse(t),
            )
            link = collection.add_item(item)
            link.extra_fields["datetime"] = t
    add_collection_information(config, collection, data)
    return collection


def handle_SH_WMS_endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog
) -> Collection:
    # create collection and subcollections (based on locations)
    if "Locations" in data:
        root_collection, _ = get_or_create_collection_and_times(
            catalog, data["Name"], data, config, endpoint
        )
        for location in data["Locations"]:
            # create  and populate location collections based on times
            # TODO: Should we add some new description per location?
            location_config = {
                "Title": location["Name"],
                "Description": "",
            }
            collection, _ = get_or_create_collection_and_times(
                catalog, location["Identifier"], location_config, config, endpoint
            )
            collection.extra_fields["endpointtype"] = endpoint["Name"]
            for time in location["Times"]:
                item = Item(
                    id=time,
                    bbox=location["Bbox"],
                    properties={},
                    geometry=None,
                    datetime=parser.isoparse(time),
                    stac_extensions=[
                        "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                    ],
                )
                add_visualization_info(item, data, endpoint, time=time)
                item_link = collection.add_item(item)
                item_link.extra_fields["datetime"] = time

            link = root_collection.add_child(collection)
            # bubble up information we want to the link
            latlng = "{},{}".format(location["Point"][1], location["Point"][0])
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["country"] = location["Country"]
            link.extra_fields["city"] = location["Name"]
            collection.update_extent_from_items()
            add_visualization_info(collection, data, endpoint)

        root_collection.update_extent_from_items()
        # Add bbox extents from children
        for c_child in root_collection.get_children():
            if isinstance(c_child, Collection):
                root_collection.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
    return root_collection


def handle_xcube_endpoint(config: dict, endpoint: dict, data: dict, catalog: Catalog) -> Collection:
    collection = process_STAC_Datacube_Endpoint(
        config=config,
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )

    add_example_info(collection, data, endpoint, config)
    return collection


def handle_GeoDB_endpoint(config: dict, endpoint: dict, data: dict, catalog: Catalog) -> Collection:
    collection, _ = get_or_create_collection_and_times(
        catalog, endpoint["CollectionId"], data, config, endpoint
    )
    select = "?select=aoi,aoi_id,country,city,time"
    url = (
        endpoint["EndPoint"]
        + endpoint["Database"]
        + "_{}".format(endpoint["CollectionId"])
        + select
    )
    if additional_query_parameters := endpoint.get("AdditionalQueryString"):
        url += f"&{additional_query_parameters}"
    response = json.loads(requests.get(url).text)

    # Sort locations by key
    sorted_locations = sorted(response, key=itemgetter("aoi_id"))
    cities = []
    countries = []
    for key, value in groupby(sorted_locations, key=itemgetter("aoi_id")):
        # Finding min and max values for date
        values = list(value)
        times = [datetime.fromisoformat(t["time"]) for t in values]
        unique_values = next(iter({v["aoi_id"]: v for v in values}.values()))
        country = unique_values["country"]
        city = unique_values["city"]
        IdKey = endpoint.get("IdKey", "city")
        IdValue = unique_values[IdKey]
        if country not in countries:
            countries.append(country)
        # sanitize unique key identifier to be sure it is saveable as a filename
        if IdValue is not None:
            IdValue = "".join(
                [c for c in IdValue if c.isalpha() or c.isdigit() or c == " "]
            ).rstrip()
        # Additional check to see if unique key name is empty afterwards
        if IdValue == "" or IdValue is None:
            # use aoi_id as a fallback unique id instead of configured key
            IdValue = key
        if city not in cities:
            cities.append(city)
        min_date = min(times)
        max_date = max(times)
        latlon = unique_values["aoi"]
        [lat, lon] = [float(x) for x in latlon.split(",")]
        # create item for unique locations
        buff = 0.01
        bbox = [lon - buff, lat - buff, lon + buff, lat + buff]
        item = Item(
            id=IdValue,
            bbox=bbox,
            properties={},
            geometry=create_geojson_point(lon, lat),
            datetime=None,
            start_datetime=min_date,
            end_datetime=max_date,
        )
        link = collection.add_item(item)
        # bubble up information we want to the link
        link.extra_fields["id"] = key
        link.extra_fields["latlng"] = latlon
        link.extra_fields["country"] = country
        link.extra_fields["city"] = city

    if "yAxis" not in data:
        # fetch yAxis and store it to data, preventing need to save it per dataset in yml
        select = "?select=y_axis&limit=1"
        url = (
            endpoint["EndPoint"]
            + endpoint["Database"]
            + "_{}".format(endpoint["CollectionId"])
            + select
        )
        response = json.loads(requests.get(url).text)
        yAxis = response[0]["y_axis"]
        data["yAxis"] = yAxis
    add_collection_information(config, collection, data)
    add_example_info(collection, data, endpoint, config)

    collection.update_extent_from_items()
    collection.summaries = Summaries(
        {
            "cities": cities,
            "countries": countries,
        }
    )
    return collection


def handle_SH_endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog, options: Options
) -> Collection:
    token = get_SH_token()
    headers = {"Authorization": f"Bearer {token}"}
    endpoint["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    # Overwrite collection id with type, such as ZARR or BYOC
    if "Type" in endpoint:
        endpoint["CollectionId"] = endpoint["Type"] + "-" + endpoint["CollectionId"]
    collection = handle_STAC_based_endpoint(config, endpoint, data, catalog, options, headers)
    return collection


def handle_WMS_endpoint(
    config: dict, endpoint: dict, data: dict, catalog: Catalog, wmts: bool = False
) -> Collection:
    collection, times = get_or_create_collection_and_times(
        catalog, data["Name"], data, config, endpoint
    )
    spatial_extent = collection.extent.spatial.to_dict().get("bbox", [-180, -90, 180, 90])[0]
    if endpoint.get("Type") != "OverwriteTimes" or not endpoint.get("OverwriteBBox"):
        # some endpoints allow "narrowed-down" capabilities per-layer, which we utilize to not
        # have to process full service capabilities XML
        capabilities_url = endpoint["EndPoint"]
        spatial_extent, times = retrieveExtentFromWMSWMTS(
            capabilities_url,
            endpoint["LayerId"],
            version=endpoint.get("Version", "1.1.1"),
            wmts=wmts,
        )
    # Create an item per time to allow visualization in stac clients
    if len(times) > 0 and not endpoint.get("Disable_Items"):
        for t in times:
            item = Item(
                id=t,
                bbox=spatial_extent,
                properties={},
                geometry=None,
                datetime=parser.isoparse(t),
                stac_extensions=[
                    "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                ],
            )
            add_visualization_info(item, data, endpoint, time=t)
            link = collection.add_item(item)
            link.extra_fields["datetime"] = t
        collection.update_extent_from_items()

    # Check if we should overwrite bbox
    if "OverwriteBBox" in endpoint:
        collection.extent.spatial = SpatialExtent(
            [
                endpoint["OverwriteBBox"],
            ]
        )
    add_collection_information(config, collection, data)
    return collection


def generate_veda_tiles_link(endpoint: dict, item: str | None) -> str:
    collection = "collection={}".format(endpoint["CollectionId"])
    assets = ""
    for asset in endpoint["Assets"]:
        assets += f"&assets={asset}"
    color_formula = ""
    if "ColorFormula" in endpoint:
        color_formula = "&color_formula={}".format(endpoint["ColorFormula"])
    no_data = ""
    if "NoData" in endpoint:
        no_data = "&no_data={}".format(endpoint["NoData"])
    item = f"&item={item}" if item else ""
    target_url = f"https://staging-raster.delta-backend.com/stac/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?{collection}{item}{assets}{color_formula}{no_data}"
    return target_url


def add_visualization_info(
    stac_object: Collection | Item,
    data: dict,
    endpoint: dict,
    file_url: str | None = None,
    time: str | None = None,
) -> None:
    # add extension reference
    if endpoint["Name"] == "Sentinel Hub" or endpoint["Name"] == "Sentinel Hub WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        extra_fields: dict[str, list[str] | dict[str, str]] = {
            "wms:layers": [endpoint["LayerId"]],
            "role": ["data"],
        }
        if time is not None:
            if endpoint["Name"] == "Sentinel Hub WMS":
                # SH WMS for public collections needs time interval, we use full day here
                datetime_object = datetime.strptime(time, "%Y-%m-%d")
                start = datetime_object.isoformat()
                end = (datetime_object + timedelta(days=1) - timedelta(milliseconds=1)).isoformat()
                time_interval = f"{start}/{end}"
                extra_fields["wms:dimensions"] = {"TIME": time_interval}
            if endpoint["Name"] == "Sentinel Hub":
                extra_fields["wms:dimensions"] = {"TIME": time}
        stac_object.add_link(
            Link(
                rel="wms",
                target=f"https://services.sentinel-hub.com/ogc/wms/{instanceId}",
                media_type=(endpoint.get("MimeType", "image/png")),
                title=data["Name"],
                extra_fields=extra_fields,
            )
        )
    elif endpoint["Name"] == "WMS":
        extra_fields = {
            "wms:layers": [endpoint["LayerId"]],
            "role": ["data"],
        }
        if time is not None:
            extra_fields["wms:dimensions"] = {
                "TIME": time,
            }
        if "Styles" in endpoint:
            extra_fields["wms:styles"] = endpoint["Styles"]
        media_type = "image/jpeg"
        if "MediaType" in endpoint:
            media_type = endpoint["MediaType"]
        stac_object.add_link(
            Link(
                rel="wms",
                target=endpoint["EndPoint"],
                media_type=media_type,
                title=data["Name"],
                extra_fields=extra_fields,
            )
        )
    elif endpoint["Name"] == "JAXA_WMTS_PALSAR":
        target_url = "{}".format(endpoint.get("EndPoint"))
        # custom time just for this special case as a default for collection wmts
        extra_fields = {"wmts:layer": endpoint.get("LayerId", "").replace("{time}", time or "2017")}
        stac_object.add_link(
            Link(
                rel="wmts",
                target=target_url,
                media_type="image/png",
                title="wmts capabilities",
                extra_fields=extra_fields,
            )
        )
    elif endpoint["Name"] == "xcube":
        if endpoint["Type"] == "zarr":
            # either preset ColormapName of left as a template
            cbar = endpoint.get("ColormapName", "{cbar}")
            # either preset Rescale of left as a template
            vmin = "{vmin}"
            vmax = "{vmax}"
            if "Rescale" in endpoint:
                vmin = endpoint["Rescale"][0]
                vmax = endpoint["Rescale"][1]
            crs = endpoint.get("Crs", "EPSG:3857")
            target_url = (
                "{}/tiles/{}/{}/{{z}}/{{y}}/{{x}}" "?crs={}&time={{time}}&vmin={}&vmax={}&cbar={}"
            ).format(
                endpoint["EndPoint"],
                endpoint["DatacubeId"],
                endpoint["Variable"],
                crs,
                vmin,
                vmax,
                cbar,
            )
            stac_object.add_link(
                Link(
                    rel="xyz",
                    target=target_url,
                    media_type="image/png",
                    title="xcube tiles",
                )
            )
    elif endpoint["Type"] == "WMTSCapabilities":
        target_url = "{}".format(endpoint.get("EndPoint"))
        extra_fields = {
            "wmts:layer": endpoint.get("LayerId", ""),
            "role": ["data"],
        }
        dimensions = {}
        if time is not None:
            dimensions["time"] = time
        if dimensions_config := endpoint.get("Dimensions", {}):
            for key, value in dimensions_config.items():
                dimensions[key] = value
        if dimensions != {}:
            extra_fields["wmts:dimensions"] = dimensions
        stac_object.add_link(
            Link(
                rel="wmts",
                target=target_url,
                media_type="image/png",
                title="wmts capabilities",
                extra_fields=extra_fields,
            )
        )
    elif endpoint["Name"] == "VEDA":
        if endpoint["Type"] == "cog":
            target_url = generate_veda_cog_link(endpoint, file_url)
        elif endpoint["Type"] == "tiles":
            target_url = generate_veda_tiles_link(endpoint, file_url)
        if target_url:
            stac_object.add_link(
                Link(
                    rel="xyz",
                    target=target_url,
                    media_type="image/png",
                    title=data["Name"],
                )
            )
    elif endpoint["Name"] == "GeoDB Vector Tiles":
        # `${geoserverUrl}${config.layerName}@EPSG%3A${projString}@pbf/{z}/{x}/{-y}.pbf`,
        # 'geodb_debd884d-92f9-4979-87b6-eadef1139394:GTIF_AT_Gemeinden_3857'
        target_url = "{}{}:{}_{}@EPSG:3857@pbf/{{z}}/{{x}}/{{-y}}.pbf".format(
            endpoint["EndPoint"],
            endpoint["Instance"],
            endpoint["Database"],
            endpoint["CollectionId"],
        )
        stac_object.add_link(
            Link(
                rel="xyz",
                target=target_url,
                media_type="application/pbf",
                title=data["Name"],
                extra_fields={
                    "description": data["Title"],
                    "parameters": endpoint["Parameters"],
                    "matchKey": endpoint["MatchKey"],
                    "timeKey": endpoint["TimeKey"],
                    "source": endpoint["Source"],
                    "role": ["data"],
                },
            )
        )
    else:
        print("Visualization endpoint not supported")
