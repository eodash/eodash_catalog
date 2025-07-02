import importlib
import json
import os
import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter

import requests
from pystac import Asset, Catalog, Collection, Item, Link, SpatialExtent, Summaries, TemporalExtent
from pystac_client import Client
from shapely import wkt
from shapely.geometry import mapping
from structlog import get_logger

from eodash_catalog.sh_endpoint import get_SH_token
from eodash_catalog.stac_handling import (
    add_collection_information,
    add_example_info,
    add_process_info_child_collection,
    add_projection_info,
    get_collection_datetimes_from_config,
    get_or_create_collection,
)
from eodash_catalog.thumbnails import generate_thumbnail
from eodash_catalog.utils import (
    Options,
    create_geojson_from_bbox,
    filter_time_entries,
    format_datetime_to_isostring_zulu,
    generate_veda_cog_link,
    merge_bboxes,
    parse_datestring_to_tz_aware_datetime,
    replace_with_env_variables,
    retrieveExtentFromWCS,
    retrieveExtentFromWMSWMTS,
)

LOGGER = get_logger(__name__)


def process_WCS_rasdaman_Endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    bbox, datetimes = retrieveExtentFromWCS(
        endpoint_config["EndPoint"],
        endpoint_config["CoverageId"],
        version=endpoint_config.get("Version", "2.0.1"),
    )
    for dt in datetimes:
        item = Item(
            id=format_datetime_to_isostring_zulu(dt),
            bbox=bbox,
            properties={},
            geometry=None,
            datetime=dt,
        )
        add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
        link = collection.add_item(item)
        # bubble up information we want to the link
        link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)

    if datetimes:
        collection.update_extent_from_items()
    else:
        LOGGER.warn(f"NO datetimes returned for collection: {endpoint_config['CoverageId']}!")

    add_collection_information(catalog_config, collection, collection_config)
    return collection


def process_STAC_Datacube_Endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    add_visualization_info(collection, collection_config, endpoint_config)

    stac_endpoint_url = endpoint_config["EndPoint"]
    if endpoint_config.get("Name") == "xcube":
        stac_endpoint_url = stac_endpoint_url + endpoint_config.get("StacEndpoint", "")
    # assuming /search not implemented
    api = Client.open(stac_endpoint_url)
    collection_id = endpoint_config.get("DatacubeId", "")
    coll = api.get_collection(collection_id)
    if not coll:
        raise ValueError(f"Collection {collection_id} not found in endpoint {endpoint_config}")
    item_id = endpoint_config.get("CollectionId", "datacube")
    item = coll.get_item(item_id)
    if not item:
        raise ValueError(f"Item  {item_id} not found in collection {coll}")
    # slice a datacube along temporal axis to individual items, selectively adding properties
    dimensions = item.properties.get("cube:dimensions", {})
    variables = item.properties.get("cube:variables", {})
    if endpoint_config.get("Variable") not in variables:
        raise Exception(
            f'Variable {endpoint_config.get("Variable")} not found in datacube {variables}'
        )
    time_dimension = "time"
    for k, v in dimensions.items():
        if v.get("type") == "temporal":
            time_dimension = k
            break
    datetimes = [
        parse_datestring_to_tz_aware_datetime(time_string)
        for time_string in dimensions.get(time_dimension).get("values")
    ]
    # optionally subset time results based on config
    if query := endpoint_config.get("Query"):
        datetimes = filter_time_entries(datetimes, query)

    for dt in datetimes:
        new_item = Item(
            id=format_datetime_to_isostring_zulu(dt),
            bbox=item.bbox,
            properties={},
            geometry=item.geometry,
            datetime=dt,
        )
        add_visualization_info(new_item, collection_config, endpoint_config)
        link = collection.add_item(new_item)
        # bubble up information we want to the link
        link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)

    unit = variables.get(endpoint_config.get("Variable")).get("unit")
    if unit and "yAxis" not in collection_config:
        collection_config["yAxis"] = unit
    if datetimes:
        collection.update_extent_from_items()
    else:
        LOGGER.warn(f"NO datetimes returned for collection: {collection_id}!")

    add_collection_information(catalog_config, collection, collection_config)

    return collection


def handle_STAC_based_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
    options: Options,
    headers=None,
) -> Collection:
    if collection_config.get("Locations"):
        root_collection = get_or_create_collection(
            catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
        )
        for location in collection_config["Locations"]:
            collection = process_STACAPI_Endpoint(
                catalog_config=catalog_config,
                endpoint_config=endpoint_config,
                collection_config=collection_config,
                catalog=catalog,
                options=options,
                headers=headers,
                filter_dates=location.get("FilterDates"),
                bbox=",".join(map(str, location["Bbox"])),
                root_collection=root_collection,
            )
            # Update identifier to use location as well as title
            # TODO: should we use the name as id? it provides much more
            # information in the clients
            collection.id = location.get("Identifier", uuid.uuid4())
            collection.title = location.get("Name")
            # See if description should be overwritten
            if location.get("Description"):
                collection.description = location["Description"]
            else:
                collection.description = location["Name"]
            # TODO: should we remove all assets from sub collections?
            link = root_collection.add_child(collection)
            latlng = f'{location["Point"][1]},{location["Point"][0]}'.strip()
            # Add extra properties we need
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["name"] = location["Name"]
            add_example_info(collection, collection_config, endpoint_config, catalog_config)
            # eodash v4 compatibility
            add_visualization_info(collection, collection_config, endpoint_config)
            add_process_info_child_collection(collection, catalog_config, collection_config)
            if location.get("OverwriteBBox"):
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
        bbox = None
        if endpoint_config.get("Bbox"):
            bbox = ",".join(map(str, endpoint_config["Bbox"]))
        root_collection = process_STACAPI_Endpoint(
            catalog_config=catalog_config,
            endpoint_config=endpoint_config,
            collection_config=collection_config,
            catalog=catalog,
            options=options,
            headers=headers,
            bbox=bbox,
        )
    # eodash v4 compatibility
    add_visualization_info(root_collection, collection_config, endpoint_config)
    add_collection_information(catalog_config, root_collection, collection_config, True)
    add_example_info(root_collection, collection_config, endpoint_config, catalog_config)
    return root_collection


def process_STACAPI_Endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
    options: Options,
    headers: dict[str, str] | None = None,
    bbox=None,
    root_collection: Collection | None = None,
    filter_dates: list[str] | None = None,
) -> Collection:
    if headers is None:
        headers = {}
    collection_id = endpoint_config["CollectionId"]
    collection = get_or_create_collection(
        catalog, collection_id, collection_config, catalog_config, endpoint_config
    )
    datetime_query = ["1900-01-01T00:00:00Z", "3000-01-01T00:00:00Z"]
    if query := endpoint_config.get("Query"):
        if start := query.get("Start"):
            datetime_query[0] = start
        if end := query.get("End"):
            datetime_query[1] = end

    api = Client.open(endpoint_config["EndPoint"], headers=headers)
    if bbox is None:
        bbox = [-180, -90, 180, 90]
    results = api.search(
        collections=[collection_id],
        bbox=bbox,
        datetime=datetime_query,  # type: ignore
    )
    # We keep track of potential duplicate times in this list
    added_times = {}
    any_item_added = False
    for item in results.items():
        any_item_added = True
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
            if item.assets.get("cog_default"):
                generate_thumbnail(
                    item, collection_config, endpoint_config, item.assets["cog_default"].href
                )
            else:
                generate_thumbnail(item, collection_config, endpoint_config)
        # Check if we can create visualization link
        if endpoint_config.get("Assets"):
            add_visualization_info(item, collection_config, endpoint_config, item.id)
            link.extra_fields["item"] = item.id
        elif item.assets.get("cog_default"):
            add_visualization_info(
                item, collection_config, endpoint_config, item.assets["cog_default"].href
            )
            link.extra_fields["cog_href"] = item.assets["cog_default"].href
        elif item_datetime:
            add_visualization_info(
                item, collection_config, endpoint_config, datetimes=[item_datetime]
            )
        elif item.properties.get("start_datetime") and item.properties.get("end_datetime"):
            add_visualization_info(
                item,
                collection_config,
                endpoint_config,
                datetimes=[
                    parse_datestring_to_tz_aware_datetime(item.properties["start_datetime"]),
                    parse_datestring_to_tz_aware_datetime(item.properties["end_datetime"]),
                ],
            )
        # If a root collection exists we point back to it from the item
        if root_collection:
            item.set_collection(root_collection)

        # bubble up information we want to the link
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(item_datetime)
        else:
            link.extra_fields["start_datetime"] = format_datetime_to_isostring_zulu(
                parse_datestring_to_tz_aware_datetime(item.properties["start_datetime"])
            )
            link.extra_fields["end_datetime"] = format_datetime_to_isostring_zulu(
                parse_datestring_to_tz_aware_datetime(item.properties["end_datetime"])
            )
        add_projection_info(
            endpoint_config,
            item,
        )
    if any_item_added:
        collection.update_extent_from_items()
    else:
        LOGGER.warn(
            f"""NO items returned for
            bbox: {bbox}, datetime: {datetime_query}, collection: {collection_id}!"""
        )
    # replace SH identifier with catalog identifier
    collection.id = collection_config["Name"]
    add_collection_information(catalog_config, collection, collection_config)

    # Check if we need to overwrite the bbox after update from items
    if endpoint_config.get("OverwriteBBox"):
        collection.extent.spatial = SpatialExtent(
            [
                endpoint_config["OverwriteBBox"],
            ]
        )

    return collection


def handle_VEDA_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = handle_STAC_based_endpoint(
        catalog_config, endpoint_config, collection_config, catalog, options
    )
    return collection


def handle_collection_only(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    datetimes = get_collection_datetimes_from_config(endpoint_config)
    if len(datetimes) > 0:
        for dt in datetimes:
            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=endpoint_config.get("OverwriteBBox"),
                properties={},
                geometry=None,
                datetime=dt,
            )
            link = collection.add_item(item)
            link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)
    add_collection_information(catalog_config, collection, collection_config)
    # eodash v4 compatibility
    add_visualization_info(collection, collection_config, endpoint_config)
    return collection


def handle_SH_WMS_endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    # create collection and subcollections (based on locations)
    root_collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    if collection_config.get("Locations"):
        for location in collection_config["Locations"]:
            # create  and populate location collections based on times
            # TODO: Should we add some new description per location?
            location_config = {
                "Title": location["Name"],
                "Description": "",
            }
            collection = get_or_create_collection(
                catalog, location["Identifier"], location_config, catalog_config, endpoint_config
            )
            collection.extra_fields["endpointtype"] = endpoint_config["Name"]
            for time_string in location["Times"]:
                dt = parse_datestring_to_tz_aware_datetime(time_string)
                item = Item(
                    id=format_datetime_to_isostring_zulu(dt),
                    bbox=location["Bbox"],
                    properties={},
                    geometry=None,
                    datetime=dt,
                    stac_extensions=[
                        "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                    ],
                )
                add_projection_info(endpoint_config, item)
                add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
                item_link = collection.add_item(item)
                item_link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)

            link = root_collection.add_child(collection)
            # bubble up information we want to the link
            latlng = "{},{}".format(location["Point"][1], location["Point"][0]).strip()
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["country"] = location["Country"]
            link.extra_fields["city"] = location["Name"]
            if location["Times"]:
                collection.update_extent_from_items()
            else:
                LOGGER.warn(f"NO datetimes configured for collection: {collection_config['Name']}!")
            add_visualization_info(collection, collection_config, endpoint_config)
            add_process_info_child_collection(collection, catalog_config, collection_config)

        root_collection.update_extent_from_items()
        # Add bbox extents from children
        for c_child in root_collection.get_children():
            if isinstance(c_child, Collection):
                root_collection.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
    else:
        # if locations are not provided, treat the collection as a
        # general proxy to the sentinel hub layer
        datetimes = get_collection_datetimes_from_config(endpoint_config)
        bbox = endpoint_config.get("Bbox", [-180, -85, 180, 85])
        for dt in datetimes:
            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=bbox,
                properties={},
                geometry=None,
                datetime=dt,
                stac_extensions=[
                    "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                ],
            )
            add_projection_info(endpoint_config, item)
            add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
            item_link = root_collection.add_item(item)
            item_link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)
    # eodash v4 compatibility
    add_collection_information(catalog_config, root_collection, collection_config, True)
    add_visualization_info(root_collection, collection_config, endpoint_config)
    return root_collection


def handle_xcube_endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    collection = process_STAC_Datacube_Endpoint(
        catalog_config=catalog_config,
        endpoint_config=endpoint_config,
        collection_config=collection_config,
        catalog=catalog,
    )

    add_example_info(collection, collection_config, endpoint_config, catalog_config)
    return collection


def handle_rasdaman_endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    collection = process_WCS_rasdaman_Endpoint(
        catalog_config, endpoint_config, collection_config, catalog
    )
    # add_example_info(collection, collection_config, endpoint_config, catalog_config)
    return collection


def handle_GeoDB_endpoint(
    catalog_config: dict, endpoint_config: dict, collection_config: dict, catalog: Catalog
) -> Collection:
    # ID of collection is data["Name"] instead of CollectionId to be able to
    # create more STAC collections from one geoDB table
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    select = "?select=aoi,aoi_id,country,city,time,input_data,sub_aoi"
    url = (
        endpoint_config["EndPoint"]
        + endpoint_config["Database"]
        + "_{}".format(endpoint_config["CollectionId"])
        + select
    )
    if additional_query_parameters := endpoint_config.get("AdditionalQueryString"):
        url += f"&{additional_query_parameters}"
    response = json.loads(requests.get(url).text)

    # Sort locations by key
    sorted_locations = sorted(response, key=itemgetter("aoi_id"))
    cities = []
    countries = []
    input_data = endpoint_config.get("InputData")
    for key, value in groupby(sorted_locations, key=itemgetter("aoi_id")):
        # Finding min and max values for date
        values = list(value)
        unique_values = next(iter({v["aoi_id"]: v for v in values}.values()))
        country = unique_values["country"]
        city = unique_values["city"]
        IdKey = endpoint_config.get("IdKey", "city")
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
        latlon = unique_values["aoi"]
        [lat, lon] = [float(x) for x in latlon.split(",")]
        # create item for unique locations
        buff = 0.01
        bbox = [lon - buff, lat - buff, lon + buff, lat + buff]

        # create collection per available inputdata information
        sc_config = {
            "Title": city,
            "Description": f"{city} - {country}",
        }
        locations_collection = get_or_create_collection(
            collection, key, sc_config, catalog_config, endpoint_config
        )
        if input_data:
            for v in values:
                # add items based on inputData fields for each time step available in values
                first_match = next(
                    (item for item in input_data if item.get("Identifier") == v["input_data"]), None
                )
                time_object = datetime.fromisoformat(v["time"])
                # extract wkt geometry from sub_aoi
                if "sub_aoi" in v and v["sub_aoi"] != "/":
                    # create geometry from wkt
                    geometry = mapping(wkt.loads(v["sub_aoi"]))
                else:
                    geometry = create_geojson_from_bbox(bbox)
                item = Item(
                    id=v["time"],
                    bbox=bbox,
                    properties={},
                    geometry=geometry,
                    datetime=time_object,
                )
                if first_match:
                    match first_match["Type"]:
                        case "WMS":
                            url = first_match["Url"]
                            extra_fields = {
                                "wms:layers": [first_match["Layers"]],
                                "role": ["data"],
                            }
                            if url.startswith("https://services.sentinel-hub.com/ogc/wms/"):
                                instanceId = os.getenv("SH_INSTANCE_ID")
                                if "InstanceId" in endpoint_config:
                                    instanceId = endpoint_config["InstanceId"]
                                start_date = format_datetime_to_isostring_zulu(time_object)
                                used_delta = timedelta(days=1)
                                if "TimeDelta" in first_match:
                                    used_delta = timedelta(minutes=first_match["TimeDelta"])
                                end_date = format_datetime_to_isostring_zulu(
                                    time_object + used_delta - timedelta(milliseconds=1)
                                )
                                extra_fields.update(
                                    {"wms:dimensions": {"TIME": f"{start_date}/{end_date}"}}
                                )
                                # we add the instance id to the url
                                url = f"https://services.sentinel-hub.com/ogc/wms/{instanceId}"
                            else:
                                extra_fields.update({"wms:dimensions": {"TIME": v["time"]}})
                            link = Link(
                                rel="wms",
                                target=url,
                                media_type=(endpoint_config.get("MimeType", "image/png")),
                                title=collection_config["Name"],
                                extra_fields=extra_fields,
                            )
                            item.add_link(link)
                            itemlink = locations_collection.add_item(item)
                            itemlink.extra_fields["datetime"] = (
                                f"{format_datetime_to_isostring_zulu(time_object)}Z"
                            )

            # add_visualization_info(
            #     item, collection_config, endpoint_config, file_url=first_match.get("FileUrl")
            # )
            locations_collection.update_extent_from_items()
        else:
            # set spatial extent from geodb
            locations_collection.extent.spatial.bboxes = [bbox]
            # set time extent from geodb
            individual_datetimes = [datetime.fromisoformat(v["time"]) for v in values]
            time_extent = [min(individual_datetimes), max(individual_datetimes)]
            locations_collection.extent.temporal = TemporalExtent([time_extent])
        locations_collection.extra_fields["subcode"] = key
        link = collection.add_child(locations_collection)
        # collection.update_extent_from_items()
        # bubble up information we want to the link
        link.extra_fields["id"] = key
        link.extra_fields["latlng"] = latlon
        link.extra_fields["country"] = country
        link.extra_fields["name"] = city

    if "yAxis" not in collection_config:
        # fetch yAxis and store it to data, preventing need to save it per dataset in yml
        select = "?select=y_axis&limit=1"
        url = (
            endpoint_config["EndPoint"]
            + endpoint_config["Database"]
            + "_{}".format(endpoint_config["CollectionId"])
            + select
        )
        response = json.loads(requests.get(url).text)
        yAxis = response[0]["y_axis"]
        collection_config["yAxis"] = yAxis
    add_collection_information(catalog_config, collection, collection_config)
    add_example_info(collection, collection_config, endpoint_config, catalog_config)
    collection.extra_fields["locations"] = True
    if not input_data:
        # we have no items, extents of collection need to be updated manually
        merged_bbox = merge_bboxes(
            [
                c_child.extent.spatial.bboxes[0]
                for c_child in collection.get_children()
                if isinstance(c_child, Collection)
            ]
        )
        collection.extent.spatial.bboxes = [merged_bbox]
        # Add bbox extents from children
        for c_child in collection.get_children():
            if isinstance(c_child, Collection) and merged_bbox != c_child.extent.spatial.bboxes[0]:
                collection.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
        # set time extent of collection
        individual_datetimes = []
        for c_child in collection.get_children():
            if isinstance(c_child, Collection) and isinstance(
                c_child.extent.temporal.intervals[0], list
            ):
                individual_datetimes.extend(c_child.extent.temporal.intervals[0])  # type: ignore
        time_extent = [min(individual_datetimes), max(individual_datetimes)]
        collection.extent.temporal = TemporalExtent([time_extent])
    else:
        # we can update from items
        collection.update_extent_from_items()
    collection.summaries = Summaries(
        {
            "cities": cities,
            "countries": countries,
        }
    )
    return collection


def handle_SH_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
    options: Options,
) -> Collection:
    token = get_SH_token(endpoint_config)
    headers = {"Authorization": f"Bearer {token}"}
    endpoint_config["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    # Overwrite collection id with type, such as ZARR or BYOC
    if endpoint_config.get("Type"):
        endpoint_config["CollectionId"] = (
            endpoint_config["Type"] + "-" + endpoint_config["CollectionId"]
        )
    collection = handle_STAC_based_endpoint(
        catalog_config, endpoint_config, collection_config, catalog, options, headers
    )
    return collection


def handle_WMS_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
    wmts: bool = False,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    datetimes = get_collection_datetimes_from_config(endpoint_config)
    spatial_extent = collection.extent.spatial.to_dict().get("bbox", [-180, -90, 180, 90])[0]
    if endpoint_config.get("Type") != "OverwriteTimes" or not endpoint_config.get("OverwriteBBox"):
        # some endpoints allow "narrowed-down" capabilities per-layer, which we utilize to not
        # have to process full service capabilities XML
        capabilities_url = endpoint_config["EndPoint"]
        spatial_extent, datetimes = retrieveExtentFromWMSWMTS(
            capabilities_url,
            endpoint_config["LayerId"],
            version=endpoint_config.get("Version", "1.1.1"),
            wmts=wmts,
        )
    # optionally filter time results
    if query := endpoint_config.get("Query"):
        datetimes = filter_time_entries(datetimes, query)
    # Create an item per time to allow visualization in stac clients
    if len(datetimes) > 0:
        for dt in datetimes:
            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=spatial_extent,
                properties={},
                geometry=None,
                datetime=dt,
                stac_extensions=[
                    "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                ],
            )
            add_projection_info(endpoint_config, item)
            add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
            link = collection.add_item(item)
            link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)
        collection.update_extent_from_items()
    else:
        LOGGER.warn(f"NO datetimes returned for collection: {collection_config['Name']}!")

    # Check if we should overwrite bbox
    if endpoint_config.get("OverwriteBBox"):
        collection.extent.spatial = SpatialExtent(
            [
                endpoint_config["OverwriteBBox"],
            ]
        )
    # eodash v4 compatibility
    add_visualization_info(collection, collection_config, endpoint_config)
    add_collection_information(catalog_config, collection, collection_config)
    return collection


def generate_veda_tiles_link(endpoint_config: dict, item: str | None) -> str:
    collection = endpoint_config["CollectionId"]
    assets = ""
    for asset in endpoint_config["Assets"]:
        assets += f"&assets={asset}"
    color_formula = ""
    if endpoint_config.get("ColorFormula"):
        color_formula = "&color_formula={}".format(endpoint_config["ColorFormula"])
    no_data = ""
    if endpoint_config.get("NoData"):
        no_data = "&no_data={}".format(endpoint_config["NoData"])
    item = item if item else "{item}"
    target_url = f"https://openveda.cloud/api/raster/collections/{collection}/items/{item}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?{assets}{color_formula}{no_data}"
    return target_url


def add_visualization_info(
    stac_object: Collection | Item,
    collection_config: dict,
    endpoint_config: dict,
    file_url: str | None = None,
    datetimes: list[datetime] | None = None,
) -> None:
    extra_fields: dict[str, list[str] | dict[str, str]] = {}
    if endpoint_config.get("Attribution"):
        stac_object.stac_extensions.append(
            "https://stac-extensions.github.io/attribution/v0.1.0/schema.json"
        )
        extra_fields["attribution"] = endpoint_config["Attribution"]
    # add extension reference
    if endpoint_config["Name"] == "Sentinel Hub" or endpoint_config["Name"] == "Sentinel Hub WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if endpoint_config.get("InstanceId"):
            instanceId = endpoint_config["InstanceId"]
        if env_id := endpoint_config.get("CustomSHEnvId"):
            # special handling for custom environment
            # (will take SH_INSTANCE_ID_{env_id}) as ENV VAR
            instanceId = os.getenv(f"SH_INSTANCE_ID_{env_id}")
        extra_fields.update(
            {
                "wms:layers": [endpoint_config["LayerId"]],
                "role": ["data"],
            }
        )
        dimensions = {}
        if dimensions_config := endpoint_config.get("Dimensions", {}):
            for key, value in dimensions_config.items():
                dimensions[key] = value
        if datetimes is not None:
            dt = datetimes[0]
            start_isostring = format_datetime_to_isostring_zulu(dt)
            # SH WMS for public collections needs time interval, we use full day here
            end = dt + timedelta(days=1) - timedelta(milliseconds=1)
            # we have start_datetime and end_datetime
            if len(datetimes) == 2:
                end = datetimes[1]
            end_isostring = format_datetime_to_isostring_zulu(end)
            time_interval = f"{start_isostring}/{end_isostring}"
            dimensions["TIME"] = time_interval

        if dimensions != {}:
            extra_fields["wms:dimensions"] = dimensions
        link = Link(
            rel="wms",
            target=f"https://services.sentinel-hub.com/ogc/wms/{instanceId}",
            media_type=(endpoint_config.get("MimeType", "image/png")),
            title=collection_config["Name"],
            extra_fields=extra_fields,
        )
        add_projection_info(
            endpoint_config,
            link,
        )
        stac_object.add_link(link)
    elif endpoint_config["Name"] == "WMS":
        extra_fields.update(
            {
                "wms:layers": [endpoint_config["LayerId"]],
                "role": ["data"],
            }
        )
        dimensions = {}
        if dimensions_config := endpoint_config.get("Dimensions", {}):
            for key, value in dimensions_config.items():
                # special replace for world_settlement_footprint
                if collection_config.get("EodashIdentifier") == "WSF":
                    value = value.replace(
                        "{time}", datetimes is not None and str(datetimes[0].year) or "{time}"
                    )
                dimensions[key] = value
        if datetimes is not None:
            dimensions["TIME"] = format_datetime_to_isostring_zulu(datetimes[0])
        if dimensions != {}:
            extra_fields["wms:dimensions"] = dimensions
        if endpoint_config.get("Styles"):
            extra_fields["wms:styles"] = endpoint_config["Styles"]
        if endpoint_config.get("TileSize"):
            extra_fields["wms:tilesize"] = endpoint_config["TileSize"]
        if endpoint_config.get("Version"):
            extra_fields["wms:version"] = endpoint_config["Version"]
        media_type = endpoint_config.get("MediaType", "image/jpeg")
        endpoint_url = endpoint_config["EndPoint"]
        # custom replacing of all ENV VARS present as template in URL as {VAR}
        endpoint_url = replace_with_env_variables(endpoint_url)
        link = Link(
            rel="wms",
            target=endpoint_url,
            media_type=media_type,
            title=collection_config["Name"],
            extra_fields=extra_fields,
        )
        add_projection_info(
            endpoint_config,
            link,
        )
        stac_object.add_link(link)
    elif endpoint_config["Name"] == "rasdaman":
        extra_fields.update(
            {
                "wms:layers": [endpoint_config["CoverageId"]],
                "role": ["data"],
            }
        )
        dimensions = {}
        if dimensions_config := endpoint_config.get("Dimensions", {}):
            for key, value in dimensions_config.items():
                dimensions[key] = value
        if datetimes is not None:
            dimensions["TIME"] = format_datetime_to_isostring_zulu(datetimes[0])
        if dimensions != {}:
            extra_fields["wms:dimensions"] = dimensions
        if endpoint_config.get("Styles"):
            extra_fields["wms:styles"] = endpoint_config["Styles"]
        media_type = endpoint_config.get("MediaType", "image/png")
        endpoint_url = endpoint_config["EndPoint"]
        # custom replacing of all ENV VARS present as template in URL as {VAR}
        link = Link(
            rel="wms",
            target=endpoint_url,
            media_type=media_type,
            title=collection_config["Name"],
            extra_fields=extra_fields,
        )
        add_projection_info(
            endpoint_config,
            link,
        )
        stac_object.add_link(link)
    elif endpoint_config["Name"] == "JAXA_WMTS_PALSAR":
        target_url = "{}".format(endpoint_config.get("EndPoint"))
        # custom time just for this special case as a default for collection wmts
        time = None
        if datetimes is not None:
            time = datetimes[0]
        extra_fields.update(
            {
                "wmts:layer": endpoint_config.get("LayerId", "").replace(
                    "{time}", (time and str(time.year)) or "2017"
                )
            }
        )
        stac_object.add_link(
            Link(
                rel="wmts",
                target=target_url,
                media_type="image/png",
                title="wmts capabilities",
                extra_fields=extra_fields,
            )
        )
    elif endpoint_config["Name"] == "xcube":
        if endpoint_config["Type"] == "zarr":
            # either preset ColormapName of left as a template
            cbar = endpoint_config.get("ColormapName", "{cbar}")
            # either preset Rescale of left as a template
            vmin = "{vmin}"
            vmax = "{vmax}"
            if endpoint_config.get("Rescale"):
                vmin = endpoint_config["Rescale"][0]
                vmax = endpoint_config["Rescale"][1]
            # depending on numerical input only
            data_projection = str(endpoint_config.get("DataProjection", 3857))
            epsg_prefix = "" if "EPSG:" in data_projection else "EPSG:"
            crs = f"{epsg_prefix}{data_projection}"
            time = (
                stac_object.get_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")  # type: ignore
                if isinstance(stac_object, Item)
                else "{time}"
            )
            target_url = (
                "{}/tiles/{}/{}/{{z}}/{{y}}/{{x}}" "?crs={}&time={}&vmin={}&vmax={}&cbar={}"
            ).format(
                endpoint_config["EndPoint"],
                endpoint_config["DatacubeId"],
                endpoint_config["Variable"],
                crs,
                time,
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
                    extra_fields=extra_fields,
                )
            )
    elif endpoint_config.get("Type") == "WMTSCapabilities":
        target_url = "{}".format(endpoint_config.get("EndPoint"))
        extra_fields.update(
            {
                "wmts:layer": endpoint_config.get("LayerId", ""),
                "role": ["data"],
            }
        )
        dimensions = {}
        if datetimes is not None:
            dimensions["time"] = format_datetime_to_isostring_zulu(datetimes[0])
        if dimensions_config := endpoint_config.get("Dimensions", {}):
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
    elif endpoint_config["Name"] == "VEDA":
        if endpoint_config["Type"] == "cog":
            target_url = generate_veda_cog_link(endpoint_config, file_url)
        elif endpoint_config["Type"] == "tiles":
            target_url = generate_veda_tiles_link(endpoint_config, file_url)
        if target_url:
            link = Link(
                rel="xyz",
                target=target_url,
                media_type="image/png",
                title=collection_config["Name"],
                extra_fields=extra_fields,
            )
            add_projection_info(
                endpoint_config,
                link,
            )
            stac_object.add_link(link)
    elif endpoint_config["Name"] == "GeoDB Vector Tiles":
        # `${geoserverUrl}${config.layerName}@EPSG%3A${projString}@pbf/{z}/{x}/{-y}.pbf`,
        # 'geodb_debd884d-92f9-4979-87b6-eadef1139394:GTIF_AT_Gemeinden_3857'
        target_url = "{}{}:{}_{}@EPSG:3857@pbf/{{z}}/{{x}}/{{-y}}.pbf".format(
            endpoint_config["EndPoint"],
            endpoint_config["Instance"],
            endpoint_config["Database"],
            endpoint_config["CollectionId"],
        )
        extra_fields.update(
            {
                "description": collection_config["Title"],
                "parameters": endpoint_config["Parameters"],
                "matchKey": endpoint_config["MatchKey"],
                "timeKey": endpoint_config["TimeKey"],
                "source": endpoint_config["Source"],
                "role": ["data"],
            }
        )
        stac_object.add_link(
            Link(
                rel="xyz",
                target=target_url,
                media_type="application/pbf",
                title=collection_config["Name"],
                extra_fields=extra_fields,
            )
        )
    else:
        LOGGER.info(f"Visualization endpoint not supported {endpoint_config['Name']}")


def handle_custom_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    # invoke 3rd party code and return
    function_path = endpoint_config["Python_Function_Location"]
    module_name, _, func_name = function_path.rpartition(".")
    # add current working directory to sys path
    sys.path.append(os.getcwd())
    try:
        # import configured function
        imported_function: Callable[[Collection, dict, dict, dict], Collection] = getattr(
            importlib.import_module(module_name), func_name
        )
    except ModuleNotFoundError as e:
        LOGGER.warn(
            f"""function {func_name} from module {module_name} can not be imported.
            Check if you are specifying relative path inside the
            catalog repository or catalog generator repository."""
        )
        raise e
    # execture the custom handler
    collection = imported_function(
        collection,
        catalog_config,
        endpoint_config,
        collection_config,
    )
    return collection


def handle_raw_source(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    catalog: Catalog,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    if len(endpoint_config.get("TimeEntries", [])) > 0:
        style_link = None
        for time_entry in endpoint_config["TimeEntries"]:
            assets = {}
            media_type = "application/geo+json"
            style_type = "text/vector-styles"
            if endpoint_config["Name"] == "COG source":
                style_type = "text/cog-styles"
                media_type = "image/tiff"
            if endpoint_config["Name"] == "FlatGeobuf source":
                media_type = "application/vnd.flatgeobuf"
            for a in time_entry["Assets"]:
                asset = Asset(
                    href=a["File"], roles=["data"], media_type=media_type, extra_fields={}
                )
                add_projection_info(endpoint_config, asset)
                assets[a["Identifier"]] = asset
            bbox = endpoint_config.get("Bbox", [-180, -85, 180, 85])
            dt = parse_datestring_to_tz_aware_datetime(time_entry["Time"])
            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=bbox,
                properties={},
                geometry=create_geojson_from_bbox(bbox)["features"][0]["geometry"],
                datetime=dt,
                assets=assets,
                extra_fields={},
            )
            if endpoint_config.get("Attribution"):
                item.stac_extensions.append(
                    "https://stac-extensions.github.io/attribution/v0.1.0/schema.json"
                )
                asset.extra_fields["attribution"] = endpoint_config["Attribution"]
            add_projection_info(
                endpoint_config,
                item,
            )
            if ep_st := endpoint_config.get("Style"):
                style_link = Link(
                    rel="style",
                    target=ep_st
                    if ep_st.startswith("http")
                    else f"{catalog_config['assets_endpoint']}/{ep_st}",
                    media_type=style_type,
                    extra_fields={
                        "asset:keys": list(assets),
                    },
                )
                item.add_link(style_link)
            link = collection.add_item(item)
            link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)
            link.extra_fields["assets"] = [a["File"] for a in time_entry["Assets"]]
        # eodash v4 compatibility, adding last referenced style to collection
        if style_link:
            collection.add_link(style_link)
        collection.update_extent_from_items()
    else:
        LOGGER.warn(f"NO datetimes configured for collection: {collection_config['Name']}!")

    add_collection_information(catalog_config, collection, collection_config)
    return collection
