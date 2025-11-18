import copy
import importlib
import io
import json
import os
import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter
from urllib.parse import urlparse

import pyarrow.parquet as pq
import requests
from pystac import Asset, Catalog, Collection, Item, Link, SpatialExtent, Summaries, TemporalExtent
from pystac_client import Client
from shapely import wkt
from shapely.geometry import mapping
from structlog import get_logger

from eodash_catalog.sh_endpoint import get_SH_token
from eodash_catalog.stac_handling import (
    add_authentication,
    add_base_overlay_info,
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
    create_geometry_from_bbox,
    extract_extent_from_geoparquet,
    filter_time_entries,
    format_datetime_to_isostring_zulu,
    generate_veda_cog_link,
    parse_datestring_to_tz_aware_datetime,
    replace_with_env_variables,
    retrieveExtentFromWCS,
    retrieveExtentFromWMSWMTS,
    save_items,
    update_extents_from_collection_children,
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
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    add_visualization_info(collection, collection_config, endpoint_config)
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
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
    items = []
    for dt in datetimes:
        new_item = Item(
            id=format_datetime_to_isostring_zulu(dt),
            bbox=item.bbox,
            properties={},
            geometry=item.geometry,
            datetime=dt,
            assets={"dummy_asset": Asset(href="")},
        )
        add_visualization_info(new_item, collection_config, endpoint_config)
        items.append(new_item)

    save_items(
        collection,
        items,
        options.outputpath,
        catalog_config["id"],
        coll_path_rel_to_root_catalog,
        options.gp,
    )
    unit = variables.get(endpoint_config.get("Variable")).get("unit")
    if unit and "yAxis" not in collection_config:
        collection_config["yAxis"] = unit
    if datetimes and not options.gp:
        collection.update_extent_from_items()
    elif not datetimes:
        LOGGER.warn(f"NO datetimes returned for collection: {collection_id}!")

    add_collection_information(catalog_config, collection, collection_config)

    return collection


def handle_STAC_based_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
    headers=None,
) -> Collection:
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
    if collection_config.get("Locations"):
        root_collection = get_or_create_collection(
            catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
        )
        for location in collection_config["Locations"]:
            identifier = location.get("Identifier", str(uuid.uuid4()))
            collection = process_STACAPI_Endpoint(
                catalog_config=catalog_config,
                endpoint_config=endpoint_config,
                collection_config=collection_config,
                coll_path_rel_to_root_catalog=f"{coll_path_rel_to_root_catalog}/{identifier}",
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
            collection.id = identifier
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
            add_process_info_child_collection(collection, catalog_config, collection_config, None)
            if location.get("OverwriteBBox"):
                collection.extent.spatial = SpatialExtent(
                    [
                        location["OverwriteBBox"],
                    ]
                )
            add_collection_information(catalog_config, collection, collection_config)
            add_base_overlay_info(collection, catalog_config, collection_config)
        update_extents_from_collection_children(root_collection)
    else:
        bbox = None
        if endpoint_config.get("OverwriteBBox"):
            bbox = ",".join(map(str, endpoint_config["OverwriteBBox"]))
        root_collection = process_STACAPI_Endpoint(
            catalog_config=catalog_config,
            endpoint_config=endpoint_config,
            collection_config=collection_config,
            coll_path_rel_to_root_catalog=coll_path_rel_to_root_catalog,
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
    coll_path_rel_to_root_catalog: str,
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
    items = []
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
        if options.tn:
            if item.assets.get("cog_default"):
                generate_thumbnail(
                    item, collection_config, endpoint_config, item.assets["cog_default"].href
                )
            else:
                generate_thumbnail(item, collection_config, endpoint_config)
        # Check if we can create visualization link
        if endpoint_config.get("Name") == "VEDA" and endpoint_config.get("Type") == "tiles":
            add_visualization_info(item, collection_config, endpoint_config, item.id)
        elif (
            endpoint_config.get("Name") == "VEDA"
            and endpoint_config.get("Type") == "cog"
            and item.assets.get("cog_default")
        ):
            add_visualization_info(
                item, collection_config, endpoint_config, item.assets["cog_default"].href
            )
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

        add_projection_info(
            endpoint_config,
            item,
        )
        # we check if the item has any assets, if not we create a dummy asset
        if not item.assets:
            item.assets["dummy_asset"] = Asset(href="")
        if "cog_default" in item.assets and item.assets["cog_default"].extra_fields.get(
            "raster:bands"
        ):
            # saving via pyarrow does not work well with statistics ranges
            # Integer value -10183824872833024 is outside of the range exactly
            # representable by a IEEE 754 double precision value
            item.assets["cog_default"].extra_fields.pop("raster:bands")
        items.append(item)

    if len(items) > 0:
        save_items(
            collection,
            items,
            options.outputpath,
            catalog_config["id"],
            coll_path_rel_to_root_catalog,
            options.gp,
        )
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
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = handle_STAC_based_endpoint(
        catalog_config,
        endpoint_config,
        collection_config,
        coll_path_rel_to_root_catalog,
        catalog,
        options,
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
                assets={"dummy_asset": Asset(href="")},
            )
            link = collection.add_item(item)
            link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(dt)
    add_collection_information(catalog_config, collection, collection_config)
    # eodash v4 compatibility
    add_visualization_info(collection, collection_config, endpoint_config)
    return collection


def handle_SH_WMS_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    # create collection and subcollections (based on locations)
    root_collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
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
            items = []
            for time_string in location["Times"]:
                dt = parse_datestring_to_tz_aware_datetime(time_string)
                item = Item(
                    id=format_datetime_to_isostring_zulu(dt),
                    bbox=location["Bbox"],
                    properties={},
                    geometry=create_geometry_from_bbox(location["Bbox"]),
                    datetime=dt,
                    stac_extensions=[
                        "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                    ],
                    assets={"dummy_asset": Asset(href="")},
                )
                add_projection_info(endpoint_config, item)
                add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
                items.append(item)
            save_items(
                collection,
                items,
                options.outputpath,
                catalog_config["id"],
                f"{coll_path_rel_to_root_catalog}/{collection.id}",
                options.gp,
            )
            link = root_collection.add_child(collection)
            # bubble up information we want to the link
            latlng = "{},{}".format(location["Point"][1], location["Point"][0]).strip()
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["country"] = location["Country"]
            link.extra_fields["city"] = location["Name"]
            if location["Times"] and not options.gp:
                collection.update_extent_from_items()
            elif not location["Times"]:
                LOGGER.warn(f"NO datetimes configured for collection: {collection_config['Name']}!")
            add_visualization_info(collection, collection_config, endpoint_config)
            add_process_info_child_collection(collection, catalog_config, collection_config, None)
            add_collection_information(catalog_config, collection, collection_config)
            add_base_overlay_info(collection, catalog_config, collection_config)
        update_extents_from_collection_children(root_collection)
    else:
        # if locations are not provided, treat the collection as a
        # general proxy to the sentinel hub layer
        datetimes = get_collection_datetimes_from_config(endpoint_config)
        bbox = endpoint_config.get("OverwriteBBox", [-180, -85, 180, 85])
        items = []
        for dt in datetimes:
            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=bbox,
                properties={},
                geometry=create_geometry_from_bbox(bbox),
                datetime=dt,
                stac_extensions=[
                    "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                ],
                assets={"dummy_asset": Asset(href="")},
            )
            add_projection_info(endpoint_config, item)
            add_visualization_info(item, collection_config, endpoint_config, datetimes=[dt])
            items.append(item)
        save_items(
            root_collection,
            items,
            options.outputpath,
            catalog_config["id"],
            coll_path_rel_to_root_catalog,
            options.gp,
        )
        # set spatial extent from config
        root_collection.extent.spatial.bboxes = [bbox]
        # set time extent from geodb
        time_extent = [min(datetimes), max(datetimes)]
        root_collection.extent.temporal = TemporalExtent([time_extent])
    # eodash v4 compatibility
    add_collection_information(catalog_config, root_collection, collection_config, True)
    add_visualization_info(root_collection, collection_config, endpoint_config)
    return root_collection


def handle_xcube_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = process_STAC_Datacube_Endpoint(
        catalog_config=catalog_config,
        endpoint_config=endpoint_config,
        collection_config=collection_config,
        catalog=catalog,
        options=options,
        coll_path_rel_to_root_catalog=coll_path_rel_to_root_catalog,
    )

    add_example_info(collection, collection_config, endpoint_config, catalog_config)
    return collection


def handle_rasdaman_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
) -> Collection:
    collection = process_WCS_rasdaman_Endpoint(
        catalog_config, endpoint_config, collection_config, coll_path_rel_to_root_catalog, catalog
    )
    # add_example_info(collection, collection_config, endpoint_config, catalog_config)
    return collection


def handle_GeoDB_Features_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    # ID of collection is data["Name"] instead of CollectionId to be able to
    # create more STAC collections from one geoDB table
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
    select = f'?select={endpoint_config["TimeParameter"]}'
    url = (
        endpoint_config["EndPoint"]
        + endpoint_config["Database"]
        + "_{}".format(endpoint_config["CollectionId"])
        + select
    )
    response = json.loads(requests.get(url).text)
    # Use aggregation value to group datetime results
    aggregation = endpoint_config.get("Aggregation", "day")
    unique_datetimes = set()
    for value in response:
        time_object = datetime.fromisoformat(value[endpoint_config["TimeParameter"]])
        match aggregation:
            case "hour":
                unique_datetimes.add(
                    datetime(
                        time_object.year,
                        time_object.month,
                        time_object.day,
                        time_object.hour,
                    )
                )
            case "day":
                unique_datetimes.add(
                    datetime(time_object.year, time_object.month, time_object.day).date()
                )
            case "month":
                unique_datetimes.add(datetime(time_object.year, time_object.month, 1).date())
            case "year":
                unique_datetimes.add(datetime(time_object.year, 1, 1).date())
            case _:
                # default to day
                unique_datetimes.add(
                    datetime(time_object.year, time_object.month, time_object.day).date()
                )
    # go over unique datetimes and create items
    items = []
    for dt in sorted(unique_datetimes):
        item_datetime = dt if isinstance(dt, datetime) else datetime(dt.year, dt.month, dt.day)
        matching_string = ""
        match aggregation:
            case "hour":
                matching_string = item_datetime.strftime("%Y-%m-%dT%H:00:00Z")
            case "day":
                matching_string = item_datetime.strftime("%Y-%m-%d")
            case "month":
                matching_string = item_datetime.strftime("%Y-%m")
            case "year":
                matching_string = item_datetime.strftime("%Y")
        updated_query = endpoint_config["Query"].replace("{{date_time}}", matching_string)
        assets = {
            "geodbfeatures": Asset(
                href=f"{endpoint_config['EndPoint']}{endpoint_config['Database']}_{endpoint_config['CollectionId']}?{updated_query}",
                media_type="application/geodb+json",
                roles=["data"],
            )
        }
        item = Item(
            id=format_datetime_to_isostring_zulu(item_datetime),
            bbox=endpoint_config.get("OverwriteBBox", [-180, -90, 180, 90]),
            properties={},
            geometry=create_geometry_from_bbox(
                endpoint_config.get("OverwriteBBox", [-180, -90, 180, 90])
            ),
            datetime=item_datetime,
            stac_extensions=[],
            assets=assets,
        )
        # add eodash style visualization info if Style has been provided
        if endpoint_config.get("Style"):
            ep_st = endpoint_config.get("Style")
            style_link = Link(
                rel="style",
                target=ep_st
                if ep_st.startswith("http")
                else f"{catalog_config['assets_endpoint']}/{ep_st}",
                media_type="text/vector-styles",
                extra_fields={
                    "asset:keys": list(assets),
                },
            )
            item.add_link(style_link)
        add_projection_info(endpoint_config, item)
        items.append(item)
    save_items(
        collection,
        items,
        options.outputpath,
        catalog_config["id"],
        coll_path_rel_to_root_catalog,
        options.gp,
    )
    add_collection_information(catalog_config, collection, collection_config)
    return collection


def handle_GeoDB_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    # ID of collection is data["Name"] instead of CollectionId to be able to
    # create more STAC collections from one geoDB table
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
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
        buff = 0.2
        bbox = [lon - buff, lat - buff, lon + buff, lat + buff]

        # create collection per available inputdata information
        sc_config = {
            "Title": f"{city} - {collection_config['Name']}",
            "Description": collection_config["Description"],
        }
        locations_collection = get_or_create_collection(
            collection, key, sc_config, catalog_config, endpoint_config
        )
        # check if input data is none
        if input_data is None:
            input_data = []
        if len(input_data) > 0 or endpoint_config.get("FeatureCollection"):
            items = []
            content_for_individual_datetimes = values
            if endpoint_config.get("MapTimesCollection"):
                # extract datetimes from another table if configured so and match it based on aoi_id
                # special for E13d
                select = f"?select=time&aoi_id=eq.{key}"
                url = (
                    endpoint_config["EndPoint"]
                    + endpoint_config["Database"]
                    + "_{}".format(endpoint_config["MapTimesCollection"])
                    + select
                )
                response = json.loads(requests.get(url).text)
                content_for_individual_datetimes = []
                for response_obj in response:
                    time_object = datetime.fromisoformat(response_obj["time"])
                    for searched_row in values:
                        search_datetime = datetime.fromisoformat(searched_row["time"])
                        if (
                            search_datetime.month == time_object.month
                            and search_datetime.year == time_object.year
                        ):
                            break
                    insert_row = copy.deepcopy(searched_row)
                    # overwrite time with one from another collection and save
                    insert_row["time"] = response_obj["time"]
                    content_for_individual_datetimes.append(insert_row)
            for v in content_for_individual_datetimes:
                # add items based on inputData fields for each time step available in values
                first_match: dict = next(
                    (item for item in input_data if item.get("Identifier") == v["input_data"]), None
                )
                time_object = datetime.fromisoformat(v["time"])
                if endpoint_config.get("MapReplaceDates"):
                    # get mapping of AOI_ID to list of dates
                    available_dates_for_aoi_id = endpoint_config.get("MapReplaceDates").get(
                        v["aoi_id"]
                    )
                    if available_dates_for_aoi_id:
                        formatted_datetime = time_object.strftime("%Y-%m-%d")
                        if formatted_datetime not in available_dates_for_aoi_id:
                            # discard this date because not in available map dates
                            continue
                # extract wkt geometry from sub_aoi
                if "sub_aoi" in v and v["sub_aoi"] != "/":
                    # create geometry from wkt
                    shapely_geometry = wkt.loads(v["sub_aoi"])
                    geometry = mapping(shapely_geometry)
                    # converting multipolygon to polygon to avoid shapely throwing an exception
                    # in collection extent from geoparquet table generation
                    # while trying to create a multipolygon extent of all multipolygons
                    if geometry["type"] == "MultiPolygon":
                        geometry = {"type": "Polygon", "coordinates": geometry["coordinates"][0]}
                    bbox = shapely_geometry.bounds
                else:
                    geometry = create_geometry_from_bbox(bbox)

                assets = {"dummy_asset": Asset(href="")}
                if endpoint_config.get("FeatureCollection"):
                    assets["geodbfeatures"] = Asset(
                        href=f"{endpoint_config['EndPoint']}{endpoint_config['Database']}_{endpoint_config['FeatureCollection']}?aoi_id=eq.{v['aoi_id']}&time=eq.{v['time']}",
                        media_type="application/geodb+json",
                        roles=["data"],
                    )
                item = Item(
                    id=v["time"],
                    bbox=bbox,
                    properties={},
                    geometry=geometry,
                    datetime=time_object,
                    assets=assets,
                )
                # make sure to also add Style link if FeatureCollection and Style has been provided
                if endpoint_config.get("FeatureCollection") and endpoint_config.get("Style"):
                    ep_st = endpoint_config.get("Style")
                    style_link = Link(
                        rel="style",
                        target=ep_st
                        if ep_st.startswith("http")
                        else f"{catalog_config['assets_endpoint']}/{ep_st}",
                        media_type="text/vector-styles",
                        extra_fields={
                            "asset:keys": list(assets),
                        },
                    )
                    item.add_link(style_link)
                if first_match:
                    match first_match["Type"]:
                        case "WMS":
                            url = first_match["Url"]
                            extra_fields = {
                                "wms:layers": [first_match["Layers"]],
                                "role": ["data"],
                            }
                            if "sentinel-hub.com" in url:
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
                                url = f"{url}{instanceId}"
                            else:
                                extra_fields.update({"wms:dimensions": {"TIME": v["time"]}})
                            link = Link(
                                rel="wms",
                                target=url,
                                media_type=(endpoint_config.get("MimeType", "image/png")),
                                title=first_match["Identifier"],
                                extra_fields=extra_fields,
                            )
                            item.add_link(link)
                            items.append(item)
                        case "XYZ":
                            # handler for NASA apis
                            url = first_match["Url"]
                            extra_fields = {}
                            # replace time to a formatted version
                            date_formatted = time_object.strftime(
                                first_match.get("DateFormat", "%Y_%m_%d")
                            )
                            target_url = url.replace("{time}", date_formatted)
                            if SiteMapping := first_match.get("SiteMapping"):
                                # match with aoi_id
                                site = SiteMapping.get(v["aoi_id"])
                                # replace in URL
                                if site:
                                    target_url = target_url.replace("{site}", site)
                                else:
                                    LOGGER.info(
                                        f"Warning: no match for SiteMapping in config for {site}"
                                    )
                            link = Link(
                                rel="xyz",
                                target=target_url,
                                media_type="image/png",
                                title=collection_config["Name"],
                                extra_fields=extra_fields,
                            )
                            item.add_link(link)
                            items.append(item)
                elif endpoint_config.get("FeatureCollection"):
                    # no input data match found, just add the item with asset only
                    assets["geodbfeatures"] = Asset(
                        href=f"{endpoint_config['EndPoint']}{endpoint_config['Database']}_{endpoint_config['FeatureCollection']}?aoi_id=eq.{v['aoi_id']}&time=eq.{v['time']}",
                        media_type="application/geodb+json",
                        roles=["data"],
                    )
                    item = Item(
                        id=v["time"],
                        bbox=bbox,
                        properties={},
                        geometry=geometry,
                        datetime=time_object,
                        assets=assets,
                    )
                    items.append(item)
            save_items(
                locations_collection,
                items,
                options.outputpath,
                catalog_config["id"],
                f"{coll_path_rel_to_root_catalog}/{locations_collection.id}",
                options.gp,
            )
        else:
            # set spatial extent from geodb
            locations_collection.extent.spatial.bboxes = [bbox]
            # set time extent from geodb
            individual_datetimes = [datetime.fromisoformat(v["time"]) for v in values]
            time_extent = [min(individual_datetimes), max(individual_datetimes)]
            locations_collection.extent.temporal = TemporalExtent([time_extent])
        add_process_info_child_collection(
            locations_collection, catalog_config, collection_config, key
        )
        locations_collection.extra_fields["subcode"] = key
        link = collection.add_child(locations_collection)
        # collection.update_extent_from_items()
        # bubble up information we want to the link
        link.extra_fields["id"] = key
        link.extra_fields["latlng"] = latlon
        link.extra_fields["country"] = country
        link.extra_fields["name"] = city
        add_collection_information(catalog_config, locations_collection, collection_config)
        add_base_overlay_info(locations_collection, catalog_config, collection_config)

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

    update_extents_from_collection_children(collection)

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
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    token = get_SH_token(endpoint_config)
    headers = {"Authorization": f"Bearer {token}"}
    endpoint_url_parts = urlparse(endpoint_config["EndPoint"])
    endpoint_config["EndPoint"] = f"https://{endpoint_url_parts.netloc}/api/v1/catalog/1.0.0/"
    # Overwrite collection id with type, such as ZARR or BYOC
    if endpoint_config.get("Type"):
        endpoint_config["CollectionId"] = (
            endpoint_config["Type"] + "-" + endpoint_config["CollectionId"]
        )
    collection = handle_STAC_based_endpoint(
        catalog_config,
        endpoint_config,
        collection_config,
        coll_path_rel_to_root_catalog,
        catalog,
        options,
        headers,
    )
    return collection


def handle_WMS_endpoint(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
    wmts: bool = False,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
    datetimes = get_collection_datetimes_from_config(endpoint_config)
    spatial_extent = collection.extent.spatial.to_dict().get("bbox", [-180, -90, 180, 90])[0]
    if endpoint_config.get("Type") != "OverwriteTimes" or not endpoint_config.get("OverwriteBBox"):
        # some endpoints allow "narrowed-down" capabilities per-layer, which we utilize to not
        # have to process full service capabilities XML
        capabilities_url = endpoint_config["EndPoint"]
        spatial_extent, datetimes_retrieved = retrieveExtentFromWMSWMTS(
            capabilities_url,
            endpoint_config["LayerId"],
            version=endpoint_config.get("Version", "1.1.1"),
            wmts=wmts,
        )
        if datetimes_retrieved:
            datetimes = datetimes_retrieved
    # optionally filter time results
    if query := endpoint_config.get("Query"):
        datetimes = filter_time_entries(datetimes, query)

    # we first collect the items and then decide if to save as geoparquet or individual items
    items = []

    # Create an item per time to allow visualization in stac clients
    if len(datetimes) > 0:
        for dt in datetimes:
            # case of wms interval coming from config
            dt_item = dt[0] if isinstance(dt, list) else dt
            item = Item(
                id=format_datetime_to_isostring_zulu(dt_item),
                bbox=spatial_extent,
                properties={},
                geometry=create_geometry_from_bbox(spatial_extent),
                datetime=dt_item,
                stac_extensions=[
                    "https://stac-extensions.github.io/web-map-links/v1.1.0/schema.json",
                ],
                assets={"dummy_asset": Asset(href="")},
            )
            add_projection_info(endpoint_config, item)
            dt_visualization = dt if isinstance(dt, list) else [dt]
            add_visualization_info(
                item, collection_config, endpoint_config, datetimes=dt_visualization
            )
            items.append(item)
    else:
        LOGGER.warn(f"NO datetimes returned for collection: {collection_config['Name']}!")

    # Save items either into collection as individual items or as geoparquet
    save_items(
        collection,
        items,
        options.outputpath,
        catalog_config["id"],
        coll_path_rel_to_root_catalog,
        options.gp,
    )

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

    colormap_name = ""
    if endpoint_config.get("ColormapName"):
        colormap_name = "&colormap_name={}".format(endpoint_config["ColormapName"])

    color_formula = ""
    if endpoint_config.get("ColorFormula"):
        color_formula = "&color_formula={}".format(endpoint_config["ColorFormula"])
    rescale = ""
    if rescale_configs := endpoint_config.get("Rescale", ""):
        if isinstance(rescale_configs[0], list):
            # one rescale definition for each band
            for rescale_config in rescale_configs:
                rescale += f"&rescale={rescale_config[0]},{rescale_config[1]}"
        else:
            # shared rescale definition for all bands
            rescale = "&rescale={},{}".format(
                endpoint_config["Rescale"][0], endpoint_config["Rescale"][1]
            )
    no_data = ""
    if endpoint_config.get("NoData"):
        no_data = "&no_data={}".format(endpoint_config["NoData"])
    item = item if item else "{item}"
    target_url_base = endpoint_config["EndPoint"].replace("/stac/", "")
    target_url = (
        f"{target_url_base}/raster/collections/{collection}/items/{item}"
        f"/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?{assets}{colormap_name}{color_formula}{no_data}{rescale}"
    )
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
            if len(datetimes) == 2:
                end = datetimes[1]
            end_isostring = format_datetime_to_isostring_zulu(end)
            time_interval = f"{start_isostring}/{end_isostring}"
            dimensions["TIME"] = time_interval

        if dimensions != {}:
            extra_fields["wms:dimensions"] = dimensions
        endpoint_url_parts = urlparse(endpoint_config["EndPoint"])
        link = Link(
            rel="wms",
            target=f"https://{endpoint_url_parts.netloc}/ogc/wms/{instanceId}",
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
        if collection_config.get("EodashIdentifier") == "FNF":
            extra_fields.update(
                {
                    "wms:layers": endpoint_config.get("LayerId", "").replace(
                        "{time}", (datetimes is not None and str(datetimes[0].year)) or "2020"
                    ),
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
            if len(datetimes) > 1:
                start = format_datetime_to_isostring_zulu(datetimes[0])
                end = format_datetime_to_isostring_zulu(datetimes[1])
                interval = f"{start}/{end}"
                dimensions["TIME"] = interval
            else:
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
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
    if len(endpoint_config.get("TimeEntries", [])) > 0:
        items = []
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
                geometry=create_geometry_from_bbox(bbox),
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
            items.append(item)

        save_items(
            collection,
            items,
            options.outputpath,
            catalog_config["id"],
            coll_path_rel_to_root_catalog,
            options.gp,
        )
        # eodash v4 compatibility, adding last referenced style to collection
        if style_link:
            collection.add_link(style_link)
    elif endpoint_config.get("ParquetSource"):
        # if parquet source is provided, download it and create items from it
        parquet_source = endpoint_config["ParquetSource"]
        if parquet_source.startswith("http"):
            # download parquet file
            parquet_file = requests.get(parquet_source)
            if parquet_file.status_code != 200:
                LOGGER.error(f"Failed to download parquet file from {parquet_source}")
                return collection
            try:
                table = pq.read_table(io.BytesIO(parquet_file.content))
            except Exception as e:
                LOGGER.error(f"Failed to read parquet file: {e}")
                return collection
            extents = extract_extent_from_geoparquet(table)
            collection.extent.temporal = extents[0]
            collection.extent.spatial = extents[1]
            collection.add_asset(
                "geoparquet",
                Asset(
                    href=parquet_source,
                    media_type="application/vnd.apache.parquet",
                    title="GeoParquet Items",
                    roles=["collection-mirror"],
                ),
            )

    else:
        LOGGER.warn(f"NO datetimes configured for collection: {collection_config['Name']}!")

    add_collection_information(catalog_config, collection, collection_config)
    return collection


def handle_vector_tile_source(
    catalog_config: dict,
    endpoint_config: dict,
    collection_config: dict,
    coll_path_rel_to_root_catalog: str,
    catalog: Catalog,
    options: Options,
) -> Collection:
    collection = get_or_create_collection(
        catalog, collection_config["Name"], collection_config, catalog_config, endpoint_config
    )
    coll_path_rel_to_root_catalog = f'{coll_path_rel_to_root_catalog}/{collection_config["Name"]}'
    if len(endpoint_config.get("TimeEntries", [])) > 0:
        items = []
        style_link = None
        for time_entry in endpoint_config["TimeEntries"]:
            # create Item for each time entry
            media_type = "application/vnd.mapbox-vector-tile"
            style_type = "text/vector-styles"
            bbox = endpoint_config.get("Bbox", [-180, -85, 180, 85])
            dt = parse_datestring_to_tz_aware_datetime(time_entry["Time"])

            item = Item(
                id=format_datetime_to_isostring_zulu(dt),
                bbox=bbox,
                properties={},
                geometry=create_geometry_from_bbox(bbox),
                datetime=dt,
                extra_fields={},
                assets={"dummy_asset": Asset(href="")},
            )
            extra_fields_link = {}
            add_authentication(item, time_entry["Url"], extra_fields_link)
            # add mapbox vector tile link
            identifier = str(uuid.uuid4())
            extra_fields_link["key"] = identifier
            if vector_tile_id_property := endpoint_config.get("idProperty"):
                extra_fields_link["idProperty"] = vector_tile_id_property
            if vector_tile_id_property := endpoint_config.get("layers"):
                extra_fields_link["layers"] = vector_tile_id_property
            link = Link(
                rel="vector-tile",
                target=time_entry["Url"],
                media_type=media_type,
                title=collection_config["Name"],
                extra_fields=extra_fields_link,
            )
            add_projection_info(
                endpoint_config,
                link,
            )
            item.add_link(link)
            add_projection_info(
                endpoint_config,
                item,
            )
            if endpoint_config.get("Attribution"):
                item.stac_extensions.append(
                    "https://stac-extensions.github.io/attribution/v0.1.0/schema.json"
                )
                item.extra_fields["attribution"] = endpoint_config["Attribution"]
            # add style
            if ep_st := endpoint_config.get("Style"):
                style_link = Link(
                    rel="style",
                    target=ep_st
                    if ep_st.startswith("http")
                    else f"{catalog_config['assets_endpoint']}/{ep_st}",
                    media_type=style_type,
                    extra_fields={"links:keys": [identifier]},
                )
                item.add_link(style_link)
            items.append(item)

        save_items(
            collection,
            items,
            options.outputpath,
            catalog_config["id"],
            coll_path_rel_to_root_catalog,
            options.gp,
        )

    else:
        LOGGER.warn(f"NO datetimes configured for collection: {collection_config['Name']}!")

    add_collection_information(catalog_config, collection, collection_config)
    return collection
