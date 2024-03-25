#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""
import time
import requests
import json
from pystac_client import Client
from dataclasses import dataclass
from typing import List
import os
import re
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader
from itertools import groupby
from operator import itemgetter
from dateutil import parser
from eodash_catalog.sh_endpoint import get_SH_token
from eodash_catalog.utils import (
    create_geojson_point,
    retrieveExtentFromWMSWMTS,
    generateDateIsostringsFromInterval,
    RaisingThread,
)
from pystac import (
    Item,
    Asset,
    Catalog,
    Link,
    CatalogType,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent,
    Summaries,
    Provider,
)
from pystac.layout import TemplateLayoutStrategy
from pystac.validation import validate_all
import spdx_lookup as lookup
import argparse

import click


# make sure we are loading the env local definition
load_dotenv()


def recursive_save(stac_object, no_items=False):
    stac_object.save_object()
    for child in stac_object.get_children():
        recursive_save(child, no_items)
    if not no_items:
        # try to save items if available
        for item in stac_object.get_items():
            item.save_object()


def process_catalog_file(file_path, options):
    print("Processing catalog:", file_path)
    with open(file_path) as f:
        config = yaml.load(f, Loader=SafeLoader)

        if len(options.collections) > 0:
            # create only catalogs containing the passed collections
            process_collections = [
                c for c in config["collections"] if c in options.collections
            ]
        elif (len(options.collections) == 1 and options.collections == "all") or len(
            options.collections
        ) == 0:
            # create full catalog
            process_collections = config["collections"]
        if len(process_collections) == 0:
            print("No applicable collections found for catalog, skipping creation")
            return
        catalog = Catalog(
            id=config["id"],
            description=config["description"],
            title=config["title"],
            catalog_type=CatalogType.RELATIVE_PUBLISHED,
        )
        for collection in process_collections:
            file_path = "%s/%s.yaml" % (options.collectionspath, collection)
            if os.path.isfile(file_path):
                # if collection file exists process it as indicator
                # collection will be added as single collection to indicator
                process_indicator_file(config, file_path, catalog, options)
            else:
                # if not try to see if indicator definition available
                file_path = "%s/%s.yaml" % (options.indicatorspath, collection)
                if os.path.isfile(file_path):
                    process_indicator_file(
                        config,
                        "%s/%s.yaml" % (options.indicatorspath, collection),
                        catalog,
                        options,
                    )
                else:
                    print(
                        "Warning: neither collection nor indicator found for %s"
                        % collection
                    )

        strategy = TemplateLayoutStrategy(item_template="${collection}/${year}")
        catalog.normalize_hrefs(
            "%s/%s" % (options.outputpath, config["id"]), strategy=strategy
        )

        print("Started creation of collection files")
        start = time.time()
        if options.ni:
            recursive_save(catalog, options.ni)
        else:
            # For full catalog save with items this still seems to be faster
            catalog.save(dest_href="%s/%s" % (options.outputpath, config["id"]))
        end = time.time()
        print(f"Catalog {config['id']}: Time consumed in saving: {end - start}")

        if options.vd:
            # try to validate catalog if flag was set
            print("Running validation of catalog %s" % file_path)
            try:
                validate_all(catalog.to_dict(), href=config["endpoint"])
            except Exception as e:
                print("Issue validation collection: %s" % e)


def extract_indicator_info(parent_collection):
    to_extract = [
        "subcode",
        "themes",
        "keywords",
        "satellite",
        "sensor",
        "cities",
        "countries",
    ]
    summaries = {}
    for key in to_extract:
        summaries[key] = set()

    for collection in parent_collection.get_collections():
        for key in to_extract:
            if key in collection.extra_fields:
                param = collection.extra_fields[key]
                if isinstance(param, list):
                    for p in param:
                        summaries[key].add(p)
                else:
                    summaries[key].add(param)
            # extract also summary information
            if collection.summaries.lists:
                if key in collection.summaries.lists:
                    for p in collection.summaries.lists[key]:
                        summaries[key].add(p)

    for key in to_extract:
        # convert all items back to a list
        summaries[key] = list(summaries[key])
        # remove empty ones
        if len(summaries[key]) == 0:
            del summaries[key]
    parent_collection.summaries = Summaries(summaries)


def iter_len_at_least(i, n):
    return sum(1 for _ in zip(range(n), i)) == n


def process_indicator_file(config, file_path, catalog, options):
    with open(file_path) as f:
        print("Processing indicator:", file_path)
        data = yaml.load(f, Loader=SafeLoader)
        parent_indicator, _ = get_or_create_collection(
            catalog, data["Name"], data, config
        )
        if "Collections" in data:
            for collection in data["Collections"]:
                process_collection_file(
                    config,
                    "%s/%s.yaml" % (options.collectionspath, collection),
                    catalog,
                    options,
                )
        else:
            # we assume that collection files can also be loaded directly
            process_collection_file(config, file_path, parent_indicator, options)
        add_collection_information(config, parent_indicator, data)
        if iter_len_at_least(parent_indicator.get_items(recursive=True), 1):
            parent_indicator.update_extent_from_items()
        # Add bbox extents from children
        for c_child in parent_indicator.get_children():
            parent_indicator.extent.spatial.bboxes.append(
                c_child.extent.spatial.bboxes[0]
            )
        # extract collection information and add it to summary indicator level
        extract_indicator_info(parent_indicator)
        # add baselayer and overview information to indicator collection
        add_base_overlay_info(parent_indicator, config, data)
        add_to_catalog(parent_indicator, catalog, None, data)


def process_collection_file(config, file_path, catalog, options):
    print("Processing collection:", file_path)
    with open(file_path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        if "Resources" in data:
            for resource in data["Resources"]:
                if "EndPoint" in resource:
                    if resource["Name"] == "Sentinel Hub":
                        handle_SH_endpoint(config, resource, data, catalog, options)
                    elif resource["Name"] == "Sentinel Hub WMS":
                        collection = handle_SH_WMS_endpoint(
                            config, resource, data, catalog
                        )
                    elif resource["Name"] == "GeoDB":
                        collection = handle_GeoDB_endpoint(
                            config, resource, data, catalog
                        )
                        add_to_catalog(collection, catalog, resource, data)
                    elif resource["Name"] == "VEDA":
                        handle_VEDA_endpoint(config, resource, data, catalog, options)
                    elif resource["Name"] == "marinedatastore":
                        handle_WMS_endpoint(config, resource, data, catalog, wmts=True)
                    elif resource["Name"] == "xcube":
                        handle_xcube_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "WMS":
                        handle_WMS_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "GeoDB Vector Tiles":
                        handle_GeoDB_Tiles_endpoint(config, resource, data, catalog)
                    elif resource["Name"] == "JAXA_WMTS_PALSAR":
                        # somewhat one off creation of individual WMTS layers as individual items
                        handle_WMS_endpoint(config, resource, data, catalog, wmts=True)
                    elif resource["Name"] == "Collection-only":
                        handle_collection_only(config, resource, data, catalog)
                    else:
                        raise ValueError("Type of Resource is not supported")
        elif "Subcollections" in data:
            # if no endpoint is specified we check for definition of subcollections
            parent_collection, _ = get_or_create_collection(
                catalog, data["Name"], data, config
            )

            locations = []
            countries = []
            for sub_coll_def in data["Subcollections"]:
                # Subcollection has only data on one location which is defined for the entire collection
                if "Name" in sub_coll_def and "Point" in sub_coll_def:
                    locations.append(sub_coll_def["Name"])
                    if isinstance(sub_coll_def["Country"], list):
                        countries.extend(sub_coll_def["Country"])
                    else:
                        countries.append(sub_coll_def["Country"])
                    process_collection_file(
                        config,
                        "%s/%s.yaml"
                        % (options.collectionspath, sub_coll_def["Collection"]),
                        parent_collection,
                        options,
                    )
                    # find link in parent collection to update metadata
                    for link in parent_collection.links:
                        if (
                            link.rel == "child"
                            and "id" in link.extra_fields
                            and link.extra_fields["id"] == sub_coll_def["Identifier"]
                        ):
                            latlng = "%s,%s" % (
                                sub_coll_def["Point"][1],
                                sub_coll_def["Point"][0],
                            )
                            link.extra_fields["id"] = sub_coll_def["Identifier"]
                            link.extra_fields["latlng"] = latlng
                            link.extra_fields["name"] = sub_coll_def["Name"]
                    # Update title of collection to use location name
                    sub_collection = parent_collection.get_child(
                        id=sub_coll_def["Identifier"]
                    )
                    if sub_collection:
                        sub_collection.title = sub_coll_def["Name"]
                # The subcollection has multiple locations which need to be extracted and elevated to parent collection level
                else:
                    # create temp catalog to save collection
                    tmp_catalog = Catalog(
                        id="tmp_catalog", description="temp catalog placeholder"
                    )
                    process_collection_file(
                        config,
                        "%s/%s.yaml"
                        % (options.collectionspath, sub_coll_def["Collection"]),
                        tmp_catalog,
                        options,
                    )
                    links = tmp_catalog.get_child(
                        sub_coll_def["Identifier"]
                    ).get_links()
                    for link in links:
                        # extract summary information
                        if "city" in link.extra_fields:
                            locations.append(link.extra_fields["city"])
                        if "country" in link.extra_fields:
                            if isinstance(link.extra_fields["country"], list):
                                countries.extend(link.extra_fields["country"])
                            else:
                                countries.append(link.extra_fields["country"])

                    parent_collection.add_links(links)

            add_collection_information(config, parent_collection, data)
            parent_collection.update_extent_from_items()
            # Add bbox extents from children
            for c_child in parent_collection.get_children():
                parent_collection.extent.spatial.bboxes.append(
                    c_child.extent.spatial.bboxes[0]
                )
            # Fill summaries for locations
            parent_collection.summaries = Summaries(
                {
                    "cities": list(set(locations)),
                    "countries": list(set(countries)),
                }
            )
            add_to_catalog(parent_collection, catalog, None, data)


def handle_collection_only(config, endpoint, data, catalog):
    collection, times = get_or_create_collection(
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
    add_to_catalog(collection, catalog, None, data)


def handle_WMS_endpoint(config, endpoint, data, catalog, wmts=False):
    collection, times = get_or_create_collection(
        catalog, data["Name"], data, config, endpoint
    )
    spatial_extent = collection.extent.spatial.to_dict().get(
        "bbox", [-180, -90, 180, 90]
    )[0]
    if not endpoint.get("Type") == "OverwriteTimes" or not endpoint.get(
        "OverwriteBBox"
    ):
        # some endpoints allow "narrowed-down" capabilities per-layer, which we utilize to not
        # have to process full service capabilities XML
        capabilities_url = endpoint["EndPoint"]
        spatial_extent, times = retrieveExtentFromWMSWMTS(
            capabilities_url, endpoint["LayerId"], wmts=wmts
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
    add_to_catalog(collection, catalog, endpoint, data)


def handle_SH_endpoint(config, endpoint, data, catalog, options):
    token = get_SH_token()
    headers = {"Authorization": "Bearer %s" % token}
    endpoint["EndPoint"] = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/"
    # Overwrite collection id with type, such as ZARR or BYOC
    if "Type" in endpoint:
        endpoint["CollectionId"] = endpoint["Type"] + "-" + endpoint["CollectionId"]
    handle_STAC_based_endpoint(config, endpoint, data, catalog, options, headers)


def handle_SH_WMS_endpoint(config, endpoint, data, catalog):
    # create collection and subcollections (based on locations)
    if "Locations" in data:
        root_collection, _ = get_or_create_collection(
            catalog, data["Name"], data, config, endpoint
        )
        for location in data["Locations"]:
            # create  and populate location collections based on times
            # TODO: Should we add some new description per location?
            location_config = {
                "Title": location["Name"],
                "Description": "",
            }
            collection, _ = get_or_create_collection(
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
            latlng = "%s,%s" % (location["Point"][1], location["Point"][0])
            link.extra_fields["id"] = location["Identifier"]
            link.extra_fields["latlng"] = latlng
            link.extra_fields["country"] = location["Country"]
            link.extra_fields["city"] = location["Name"]
            collection.update_extent_from_items()
            add_visualization_info(collection, data, endpoint)

        root_collection.update_extent_from_items()
        # Add bbox extents from children
        for c_child in root_collection.get_children():
            root_collection.extent.spatial.bboxes.append(
                c_child.extent.spatial.bboxes[0]
            )
        add_to_catalog(root_collection, catalog, endpoint, data)
    return root_collection


def handle_VEDA_endpoint(config, endpoint, data, catalog, options):
    handle_STAC_based_endpoint(config, endpoint, data, catalog, options)


def handle_xcube_endpoint(config, endpoint, data, catalog):
    root_collection = process_STAC_Datacube_Endpoint(
        config=config,
        endpoint=endpoint,
        data=data,
        catalog=catalog,
    )

    add_example_info(root_collection, data, endpoint, config)
    add_to_catalog(root_collection, catalog, endpoint, data)


def get_or_create_collection(catalog, collection_id, data, config, endpoint=None):
    # Check if collection already in catalog
    for collection in catalog.get_collections():
        if collection.id == collection_id:
            return collection, []
    # If none found create a new one
    spatial_extent = [-180.0, -90.0, 180.0, 90.0]
    if endpoint and endpoint.get("OverwriteBBox"):
        spatial_extent = endpoint.get("OverwriteBBox")
    spatial_extent = SpatialExtent(
        [
            spatial_extent,
        ]
    )
    times = []
    temporal_extent = TemporalExtent([[datetime.now(), None]])
    if endpoint and endpoint.get("Type") == "OverwriteTimes":
        if endpoint.get("Times"):
            times = endpoint.get("Times")
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent(
                [[times_datetimes[0], times_datetimes[-1]]]
            )
        elif endpoint.get("DateTimeInterval"):
            start = endpoint["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
            end = endpoint["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
            timedelta_config = endpoint["DateTimeInterval"].get(
                "Timedelta", {"days": 1}
            )
            times = generateDateIsostringsFromInterval(start, end, timedelta_config)
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent(
                [[times_datetimes[0], times_datetimes[-1]]]
            )
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Check if description is link to markdown file
    if "Description" in data:
        description = data["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith(("http")):
                # if full absolute path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("WARNING: Markdown file could not be fetched")
                    description = data["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(
                    "%s/%s" % (config["assets_endpoint"], description)
                )
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("WARNING: Markdown file could not be fetched")
                    description = data["Subtitle"]
    elif "Subtitle" in data:
        # Try to use at least subtitle to fill some information
        description = data["Subtitle"]

    collection = Collection(
        id=collection_id,
        title=data["Title"],
        description=description,
        extent=extent,
    )
    return (collection, times)


def add_to_catalog(collection, catalog, endpoint, data):
    # check if already in catalog, if it is do not re-add it
    # TODO: probably we should add to the catalog only when creating
    for cat_coll in catalog.get_collections():
        if cat_coll.id == collection.id:
            return

    link = catalog.add_child(collection)
    # bubble fields we want to have up to collection link and add them to collection
    if endpoint and "Type" in endpoint:
        collection.extra_fields["endpointtype"] = "%s_%s" % (
            endpoint["Name"],
            endpoint["Type"],
        )
        link.extra_fields["endpointtype"] = "%s_%s" % (
            endpoint["Name"],
            endpoint["Type"],
        )
    elif endpoint:
        collection.extra_fields["endpointtype"] = endpoint["Name"]
        link.extra_fields["endpointtype"] = endpoint["Name"]
    # Disabling bubbling up of description as now it is considered to be
    # used as markdown loading would increase the catalog size unnecessarily
    # link.extra_fields["description"] = collection.description
    if "Subtitle" in data:
        link.extra_fields["subtitle"] = data["Subtitle"]
    link.extra_fields["title"] = collection.title
    link.extra_fields["code"] = data["EodashIdentifier"]
    link.extra_fields["id"] = data["Name"]
    if "Themes" in data:
        link.extra_fields["themes"] = data["Themes"]
    # Check for summaries and bubble up info
    if collection.summaries.lists:
        for sum in collection.summaries.lists:
            link.extra_fields[sum] = collection.summaries.lists[sum]

    add_extra_fields(link, data)
    return link


def add_extra_fields(stac_object, data):
    if "yAxis" in data:
        stac_object.extra_fields["yAxis"] = data["yAxis"]
    if "Themes" in data:
        stac_object.extra_fields["themes"] = data["Themes"]
    if "Locations" in data or "Subcollections" in data:
        stac_object.extra_fields["locations"] = True
    if "Tags" in data:
        stac_object.extra_fields["tags"] = data["Tags"]
    if "Satellite" in data:
        stac_object.extra_fields["satellite"] = data["Satellite"]
    if "Sensor" in data:
        stac_object.extra_fields["sensor"] = data["Sensor"]
    if "Agency" in data:
        stac_object.extra_fields["agency"] = data["Agency"]
    if "yAxis" in data:
        stac_object.extra_fields["yAxis"] = data["yAxis"]
    if "EodashIdentifier" in data:
        stac_object.extra_fields["subcode"] = data["EodashIdentifier"]
    if "DataSource" in data:
        if "Spaceborne" in data["DataSource"]:
            if "Sensor" in data["DataSource"]["Spaceborne"]:
                stac_object.extra_fields["sensor"] = data["DataSource"]["Spaceborne"][
                    "Sensor"
                ]
            if "Satellite" in data["DataSource"]["Spaceborne"]:
                stac_object.extra_fields["satellite"] = data["DataSource"][
                    "Spaceborne"
                ]["Satellite"]
        if "InSitu" in data["DataSource"]:
            stac_object.extra_fields["insituSources"] = data["DataSource"]["InSitu"]
        if "Other" in data["DataSource"]:
            stac_object.extra_fields["otherSources"] = data["DataSource"]["Other"]


def handle_GeoDB_endpoint(config, endpoint, data, catalog):
    collection, _ = get_or_create_collection(
        catalog, endpoint["CollectionId"], data, config, endpoint
    )
    select = "?select=aoi,aoi_id,country,city,time"
    url = (
        endpoint["EndPoint"]
        + endpoint["Database"]
        + "_%s" % endpoint["CollectionId"]
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
        values = [v for v in value]
        times = [datetime.fromisoformat(t["time"]) for t in values]
        unique_values = list({v["aoi_id"]: v for v in values}.values())[0]
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
            + "_%s" % endpoint["CollectionId"]
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


def handle_STAC_based_endpoint(config, endpoint, data, catalog, options, headers=None):
    if "Locations" in data:
        root_collection, _ = get_or_create_collection(
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
            collection.id = location["Identifier"]
            collection.title = (location["Name"],)
            # See if description should be overwritten
            if "Description" in location:
                collection.description = location["Description"]
            else:
                collection.description = location["Name"]
            # TODO: should we remove all assets from sub collections?
            link = root_collection.add_child(collection)
            latlng = "%s,%s" % (location["Point"][1], location["Point"][0])
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
            root_collection.extent.spatial.bboxes.append(
                c_child.extent.spatial.bboxes[0]
            )
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
    add_to_catalog(root_collection, catalog, endpoint, data)


def add_base_overlay_info(collection, config, data):
    # check if default base layers defined
    if "default_base_layers" in config:
        with open("%s.yaml" % config["default_base_layers"]) as f:
            base_layers = yaml.load(f, Loader=SafeLoader)
            for layer in base_layers:
                collection.add_link(create_web_map_link(layer, role="baselayer"))
    # check if default overlay layers defined
    if "default_overlay_layers" in config:
        with open("%s.yaml" % config["default_overlay_layers"]) as f:
            overlay_layers = yaml.load(f, Loader=SafeLoader)
            for layer in overlay_layers:
                collection.add_link(create_web_map_link(layer, role="overlay"))
    if "BaseLayers" in data:
        for layer in data["BaseLayers"]:
            collection.add_link(create_web_map_link(layer, role="baselayer"))
    if "OverlayLayers" in data:
        for layer in data["OverlayLayers"]:
            collection.add_link(create_web_map_link(layer, role="overlay"))
    # TODO: possibility to overwrite default base and overlay layers


def create_web_map_link(layer, role):
    extra_fields = {
        "roles": [role],
        "id": layer["id"],
    }
    if "default" in layer and layer["default"]:
        extra_fields["roles"].append("default")
    if "visible" in layer and layer["visible"]:
        extra_fields["roles"].append("visible")
    if "visible" in layer and not layer["visible"]:
        extra_fields["roles"].append("invisible")

    match layer["protocol"]:
        case "wms":
            # handle wms special config options
            extra_fields["wms:layers"] = layer["layers"]
            if "styles" in layer:
                extra_fields["wms:styles"] = layer["styles"]
            # TODO: handle wms dimensions extra_fields["wms:dimensions"]
        case "wmts":
            extra_fields["wmts:layer"] = layer["layer"]
            # TODO: handle wmts dimensions extra_fields["wmts:dimensions"]

    wml = Link(
        rel=layer["protocol"],
        target=layer["url"],
        media_type="image/png" if "media_type" not in layer else layer["media_type"],
        title=layer["name"],
        extra_fields=extra_fields,
    )
    return wml


def add_example_info(stac_object, data, endpoint, config):
    if "Services" in data:
        for service in data["Services"]:
            if service["Name"] == "Statistical API":
                service_type = "byoc" if "Type" not in service else service["Type"]
                stac_object.add_link(
                    Link(
                        rel="example",
                        target="%s/%s" % (config["assets_endpoint"], service["Script"]),
                        title="evalscript",
                        media_type="application/javascript",
                        extra_fields={
                            "example:language": "JavaScript",
                            "dataId": "%s-%s" % (service_type, service["CollectionId"]),
                        },
                    )
                )
            if service["Name"] == "VEDA Statistics":
                stac_object.add_link(
                    Link(
                        rel="example",
                        target=service["Endpoint"],
                        title=service["Name"],
                        media_type="application/json",
                        extra_fields={
                            "example:language": "JSON",
                        },
                    )
                )
            if service["Name"] == "EOxHub Notebook":
                # TODO: we need to consider if we can improve information added
                stac_object.add_link(
                    Link(
                        rel="example",
                        target=service["Url"],
                        title=(
                            service["Title"] if "Title" in service else service["Name"]
                        ),
                        media_type="application/x-ipynb+json",
                        extra_fields={
                            "example:language": "Jupyter Notebook",
                            "example:container": True,
                        },
                    )
                )
    elif "Resources" in data:
        for service in data["Resources"]:
            if service.get("Name") == "xcube":
                target_url = "%s/timeseries/%s/%s?aggMethods=median" % (
                    endpoint["EndPoint"],
                    endpoint["DatacubeId"],
                    endpoint["Variable"],
                )
                stac_object.add_link(
                    Link(
                        rel="example",
                        target=target_url,
                        title=service["Name"] + " analytics",
                        media_type="application/json",
                        extra_fields={
                            "example:language": "JSON",
                            "example:method": "POST",
                        },
                    )
                )


def generate_veda_cog_link(endpoint, file_url):
    bidx = ""
    if "Bidx" in endpoint:
        # Check if an array was provided
        if hasattr(endpoint["Bidx"], "__len__"):
            for band in endpoint["Bidx"]:
                bidx = bidx + "&bidx=%s" % (band)
        else:
            bidx = "&bidx=%s" % (endpoint["Bidx"])

    colormap = ""
    if "Colormap" in endpoint:
        colormap = "&colormap=%s" % (endpoint["Colormap"])
        # TODO: For now we assume a already urlparsed colormap definition
        # it could be nice to allow a json and better convert it on the fly
        # colormap = "&colormap=%s"%(urllib.parse.quote(str(endpoint["Colormap"])))

    colormap_name = ""
    if "ColormapName" in endpoint:
        colormap_name = "&colormap_name=%s" % (endpoint["ColormapName"])

    rescale = ""
    if "Rescale" in endpoint:
        rescale = "&rescale=%s,%s" % (endpoint["Rescale"][0], endpoint["Rescale"][1])

    if file_url:
        file_url = "url=%s&" % (file_url)
    else:
        file_url = ""

    target_url = (
        "https://staging-raster.delta-backend.com/cog/tiles/WebMercatorQuad/{z}/{x}/{y}?%sresampling_method=nearest%s%s%s%s"
        % (
            file_url,
            bidx,
            colormap,
            colormap_name,
            rescale,
        )
    )
    return target_url


def generate_veda_tiles_link(endpoint, item):
    collection = "collection=%s" % endpoint["CollectionId"]
    assets = ""
    for asset in endpoint["Assets"]:
        assets += "&assets=%s" % asset
    color_formula = ""
    if "ColorFormula" in endpoint:
        color_formula = "&color_formula=%s" % endpoint["ColorFormula"]
    no_data = ""
    if "NoData" in endpoint:
        no_data = "&no_data=%s" % endpoint["NoData"]
    if item:
        item = "&item=%s" % (item)
    else:
        item = ""
    target_url = (
        "https://staging-raster.delta-backend.com/stac/tiles/WebMercatorQuad/{z}/{x}/{y}?%s%s%s%s%s"
        % (
            collection,
            item,
            assets,
            color_formula,
            no_data,
        )
    )
    return target_url


def add_visualization_info(stac_object, data, endpoint, file_url=None, time=None):
    # add extension reference
    if endpoint["Name"] == "Sentinel Hub" or endpoint["Name"] == "Sentinel Hub WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        extra_fields = {
            "wms:layers": [endpoint["LayerId"]],
            "role": ["data"],
        }
        if time != None:
            if endpoint["Name"] == "Sentinel Hub WMS":
                # SH WMS for public collections needs time interval, we use full day here
                datetime_object = datetime.strptime(time, "%Y-%m-%d")
                extra_fields["wms:dimensions"] = {
                    "TIME": "%s/%s"
                    % (
                        datetime_object.isoformat(),
                        (
                            datetime_object
                            + timedelta(days=1)
                            - timedelta(milliseconds=1)
                        ).isoformat(),
                    )
                }
            if endpoint["Name"] == "Sentinel Hub":
                extra_fields["wms:dimensions"] = {"TIME": time}
        stac_object.add_link(
            Link(
                rel="wms",
                target="https://services.sentinel-hub.com/ogc/wms/%s" % (instanceId),
                media_type=(
                    endpoint["MimeType"] if "MimeType" in endpoint else "image/png"
                ),
                title=data["Name"],
                extra_fields=extra_fields,
            )
        )
    # elif resource["Name"] == "GeoDB":
    #     pass
    elif endpoint["Name"] == "WMS":
        extra_fields = {
            "wms:layers": [endpoint["LayerId"]],
            "role": ["data"],
        }
        if time != None:
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
        target_url = "%s" % (endpoint.get("EndPoint"),)
        # custom time just for this special case as a default for collection wmts
        extra_fields = {
            "wmts:layer": endpoint.get("LayerId").replace("{time}", time or "2017")
        }
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
                "%s/tiles/%s/%s/{z}/{y}/{x}?crs=%s&time={time}&vmin=%s&vmax=%s&cbar=%s"
                % (
                    endpoint["EndPoint"],
                    endpoint["DatacubeId"],
                    endpoint["Variable"],
                    crs,
                    vmin,
                    vmax,
                    cbar,
                )
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
        target_url = "%s" % (endpoint.get("EndPoint"),)
        extra_fields = {
            "wmts:layer": endpoint.get("LayerId"),
            "role": ["data"],
        }
        dimensions = {}
        if time != None:
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
        target_url = "%s%s:%s_%s@EPSG:3857@pbf/{z}/{x}/{-y}.pbf" % (
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


def process_STACAPI_Endpoint(
    config,
    endpoint,
    data,
    catalog,
    options,
    headers={},
    bbox=None,
    root_collection=None,
    filter_dates=None,
):
    collection, _ = get_or_create_collection(
        catalog, endpoint["CollectionId"], data, config, endpoint
    )
    # add_visualization_info(collection, data, endpoint)

    api = Client.open(endpoint["EndPoint"], headers=headers)
    if bbox == None:
        bbox = "-180,-90,180,90"
    results = api.search(
        collections=[endpoint["CollectionId"]],
        bbox=bbox,
        datetime=["1900-01-01T00:00:00Z", "3000-01-01T00:00:00Z"],
    )
    # We keep track of potential duplicate times in this list
    added_times = {}
    for item in results.items():
        item_datetime = item.get_datetime()
        if item_datetime != None:
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
                generate_thumbnail(
                    item, data, endpoint, item.assets["cog_default"].href
                )
            else:
                generate_thumbnail(item, data, endpoint)
        # Check if we can create visualization link
        if "Assets" in endpoint:
            add_visualization_info(item, data, endpoint, item.id)
            link.extra_fields["item"] = item.id
        elif "cog_default" in item.assets:
            add_visualization_info(
                item, data, endpoint, item.assets["cog_default"].href
            )
            link.extra_fields["cog_href"] = item.assets["cog_default"].href
        elif item_datetime:
            time_string = item_datetime.isoformat()[:-6] + "Z"
            add_visualization_info(item, data, endpoint, time=time_string)
        elif "start_datetime" in item.properties and "end_datetime" in item.properties:
            add_visualization_info(
                item,
                data,
                endpoint,
                time="%s/%s"
                % (item.properties["start_datetime"], item.properties["end_datetime"]),
            )
        # If a root collection exists we point back to it from the item
        if root_collection != None:
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


def fetch_and_save_thumbnail(data, url):
    collection_path = "../thumbnails/%s_%s/" % (data["EodashIdentifier"], data["Name"])
    Path(collection_path).mkdir(parents=True, exist_ok=True)
    image_path = "%s/thumbnail.png" % (collection_path)
    if not os.path.exists(image_path):
        data = requests.get(url).content
        f = open(image_path, "wb")
        f.write(data)
        f.close()


def generate_thumbnail(
    stac_object, data, endpoint, file_url=None, time=None, styles=None
):
    if endpoint["Name"] == "Sentinel Hub" or endpoint["Name"] == "WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if "InstanceId" in endpoint:
            instanceId = endpoint["InstanceId"]
        # Build example url
        wms_config = "REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&FORMAT=image/png&STYLES=&TRANSPARENT=true"
        bbox = "%s,%s,%s,%s" % (
            stac_object.bbox[1],
            stac_object.bbox[0],
            stac_object.bbox[3],
            stac_object.bbox[2],
        )
        output_format = (
            "format=image/png&WIDTH=256&HEIGHT=128&CRS=EPSG:4326&BBOX=%s" % (bbox)
        )
        item_datetime = stac_object.get_datetime()
        # it is possible for datetime to be null, if it is start and end datetime have to exist
        if item_datetime:
            time = item_datetime.isoformat()[:-6] + "Z"
        url = "https://services.sentinel-hub.com/ogc/wms/%s?%s&layers=%s&time=%s&%s" % (
            instanceId,
            wms_config,
            endpoint["LayerId"],
            time,
            output_format,
        )
        fetch_and_save_thumbnail(data, url)
    elif endpoint["Name"] == "VEDA":
        target_url = generate_veda_cog_link(endpoint, file_url)
        # set to get 0/0/0 tile
        url = re.sub(r"\{.\}", "0", target_url)
        fetch_and_save_thumbnail(data, url)


def process_STAC_Datacube_Endpoint(config, endpoint, data, catalog):
    collection, _ = get_or_create_collection(
        catalog, data["Name"], data, config, endpoint
    )
    add_visualization_info(collection, data, endpoint)

    stac_endpoint_url = endpoint["EndPoint"]
    if endpoint.get("Name") == "xcube":
        stac_endpoint_url = stac_endpoint_url + endpoint.get("StacEndpoint", "")
    # assuming /search not implemented
    api = Client.open(stac_endpoint_url)
    coll = api.get_collection(endpoint.get("CollectionId", "datacubes"))
    item = coll.get_item(endpoint.get("DatacubeId"))
    # slice a datacube along temporal axis to individual items, selectively adding properties
    dimensions = item.properties.get("cube:dimensions", {})
    variables = item.properties.get("cube:variables")
    if not endpoint.get("Variable") in variables.keys():
        raise Exception(
            f'Variable {endpoint.get("Variable")} not found in datacube {variables}'
        )
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


def add_collection_information(config, collection, data):
    # Add metadata information
    # Check license identifier
    if "License" in data:
        # Check if list was provided
        if isinstance(data["License"], list):
            if len(data["License"]) == 1:
                collection.license = "proprietary"
                link = Link(
                    rel="license",
                    target=data["License"][0]["Url"],
                    media_type=(
                        data["License"][0]["Type"]
                        if "Type" in data["License"][0]
                        else "text/html"
                    ),
                )
                if "Title" in data["License"][0]:
                    link.title = data["License"][0]["Title"]
                collection.links.append(link)
            elif len(data["License"]) > 1:
                collection.license = "various"
                for l in data["License"]:
                    link = Link(
                        rel="license",
                        target=l["Url"],
                        media_type="text/html" if "Type" in l else l["Type"],
                    )
                    if "Title" in l:
                        link.title = l["Title"]
                    collection.links.append(link)
        else:
            license = lookup.by_id(data["License"])
            if license is not None:
                collection.license = license.id
                if license.sources:
                    # add links to licenses
                    for source in license.sources:
                        collection.links.append(
                            Link(
                                rel="license",
                                target=source,
                                media_type="text/html",
                            )
                        )
            else:
                # fallback to proprietary
                print(
                    "WARNING: License could not be parsed, falling back to proprietary"
                )
                collection.license = "proprietary"
    else:
        # print("WARNING: No license was provided, falling back to proprietary")
        pass

    if "Provider" in data:
        try:
            collection.providers = [
                Provider(
                    # convert information to lower case
                    **dict((k.lower(), v) for k, v in provider.items())
                )
                for provider in data["Provider"]
            ]
        except:
            print(
                "WARNING: Issue creating provider information for collection: %s"
                % collection.id
            )

    if "Citation" in data:
        if "DOI" in data["Citation"]:
            collection.extra_fields["sci:doi"] = data["Citation"]["DOI"]
        if "Citation" in data["Citation"]:
            collection.extra_fields["sci:citation"] = data["Citation"]["Citation"]
        if "Publication" in data["Citation"]:
            collection.extra_fields["sci:publications"] = [
                # convert keys to lower case
                dict((k.lower(), v) for k, v in publication.items())
                for publication in data["Citation"]["Publication"]
            ]

    if "Subtitle" in data:
        collection.extra_fields["subtitle"] = data["Subtitle"]
    if "Legend" in data:
        collection.add_asset(
            "legend",
            Asset(
                href="%s/%s" % (config["assets_endpoint"], data["Legend"]),
                media_type="image/png",
                roles=["metadata"],
            ),
        )
    if "Story" in data:
        collection.add_asset(
            "story",
            Asset(
                href="%s/%s" % (config["assets_endpoint"], data["Story"]),
                media_type="text/markdown",
                roles=["metadata"],
            ),
        )
    if "Image" in data:
        collection.add_asset(
            "thumbnail",
            Asset(
                href="%s/%s" % (config["assets_endpoint"], data["Image"]),
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
    # Add extra fields to collection if available
    add_extra_fields(collection, data)

    if "References" in data:
        generic_counter = 1
        for ref in data["References"]:
            if "Key" in ref:
                key = ref["Key"]
            else:
                key = "reference_%s" % generic_counter
                generic_counter = generic_counter + 1
            collection.add_asset(
                key,
                Asset(
                    href=ref["Url"],
                    title=ref["Name"],
                    media_type=ref["MediaType"] if "MediaType" in ref else "text/html",
                    roles=["metadata"],
                ),
            )


@dataclass
class Options:
    catalogspath: str
    collectionspath: str
    indicatorspath: str
    outputpath: str
    vd: bool
    ni: bool
    tn: bool
    collections: List[str]


@click.command()
@click.option(
    "--catalog",
    "-ctl",
    help="id of catalog configuration file to be used",
    default=None,
)
@click.option(
    "--catalogspath",
    "-ctp",
    help="path to catalog configuration files",
    default="catalogs",
)
@click.option(
    "--collectionspath",
    "-clp",
    help="path to collection configuration files",
    default="collections",
)
@click.option(
    "--indicatorspath",
    "-inp",
    help="path to indicator configuration files",
    default="indicators",
)
@click.option(
    "--outputpath",
    "-o",
    help="path where the generated catalogs will be saved",
    default="build",
)
@click.option(
    "-vd",
    is_flag=True,
    help="validation flag, if set, validation will be run on generated catalogs",
)
@click.option(
    "-ni", is_flag=True, help="no items flag, if set, items will not be saved"
)
@click.option(
    "-tn",
    is_flag=True,
    help="generate additionally thumbnail image for supported collections",
)
@click.argument(
    "collections",
    nargs=-1,
)
def process_catalogs(
    catalog,
    catalogspath,
    collectionspath,
    indicatorspath,
    outputpath,
    vd,
    ni,
    tn,
    collections,
):
    """STAC generator and harvester:
    This library goes over configured endpoints extracting as much information
    as possible and generating a STAC catalog with the information"""
    options = Options(
        catalogspath=catalogspath,
        collectionspath=collectionspath,
        indicatorspath=indicatorspath,
        outputpath=outputpath,
        vd=vd,
        ni=ni,
        tn=tn,
        collections=collections,
    )
    tasks = []
    for file_name in os.listdir(catalogspath):
        file_path = os.path.join(catalogspath, file_name)
        if os.path.isfile(file_path):
            if catalog == None or os.path.splitext(file_name)[0] == catalog:
                tasks.append(
                    RaisingThread(
                        target=process_catalog_file, args=(file_path, options)
                    )
                )
                tasks[-1].start()
    for task in tasks:
        task.join()
