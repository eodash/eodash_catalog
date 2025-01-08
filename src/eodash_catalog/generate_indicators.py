#!/usr/bin/python
"""
Indicator generator to harvest information from endpoints and generate catalog

"""

import os
import time
from typing import Any

import click
import yaml
from dotenv import load_dotenv
from pystac import Catalog, CatalogType, Collection, Link, Summaries
from pystac.layout import TemplateLayoutStrategy
from pystac.validation import validate_all
from structlog import get_logger
from yaml.loader import SafeLoader

from eodash_catalog.endpoints import (
    handle_collection_only,
    handle_custom_endpoint,
    handle_GeoDB_endpoint,
    handle_rasdaman_endpoint,
    handle_raw_source,
    handle_SH_endpoint,
    handle_SH_WMS_endpoint,
    handle_VEDA_endpoint,
    handle_WMS_endpoint,
    handle_xcube_endpoint,
)
from eodash_catalog.stac_handling import (
    add_base_overlay_info,
    add_collection_information,
    add_extra_fields,
    add_process_info,
    add_projection_info,
    get_or_create_collection,
)
from eodash_catalog.utils import (
    Options,
    RaisingThread,
    add_single_item_if_collection_empty,
    iter_len_at_least,
    recursive_save,
    retry,
)

# make sure we are loading the env local definition
load_dotenv()
LOGGER = get_logger(__name__)


def process_catalog_file(file_path: str, options: Options):
    LOGGER.info(f"Processing catalog: {file_path}")
    with open(file_path) as f:
        catalog_config: dict = yaml.load(f, Loader=SafeLoader)

        if len(options.collections) > 0:
            # create only catalogs containing the passed collections
            process_collections = [
                c for c in catalog_config["collections"] if c in options.collections
            ]
        elif (len(options.collections) == 1 and options.collections == "all") or len(
            options.collections
        ) == 0:
            # create full catalog
            process_collections = catalog_config["collections"]
        if len(process_collections) == 0:
            LOGGER.info("No applicable collections found for catalog, skipping creation")
            return
        catalog = Catalog(
            id=catalog_config["id"],
            description=catalog_config["description"],
            title=catalog_config["title"],
            catalog_type=CatalogType.RELATIVE_PUBLISHED,
        )
        for collection in process_collections:
            file_path = f"{options.collectionspath}/{collection}.yaml"
            if os.path.isfile(file_path):
                # if collection file exists process it as indicator
                # collection will be added as single collection to indicator
                process_indicator_file(catalog_config, file_path, catalog, options)
            else:
                # if not try to see if indicator definition available
                file_path = f"{options.indicatorspath}/{collection}.yaml"
                if os.path.isfile(file_path):
                    process_indicator_file(
                        catalog_config,
                        f"{options.indicatorspath}/{collection}.yaml",
                        catalog,
                        options,
                    )
                else:
                    LOGGER.info(f"Warning: neither collection nor indicator found for {collection}")
        if "MapProjection" in catalog_config:
            catalog.extra_fields["eodash:mapProjection"] = catalog_config["MapProjection"]

        strategy = TemplateLayoutStrategy(item_template="${collection}/${year}")
        # expecting that the catalog will be hosted online, self url should correspond to that
        # default to a local folder + catalog id in case not set

        LOGGER.info("Started creation of collection files")
        start = time.time()
        if options.ni:
            catalog_self_href = f'{options.outputpath}/{catalog_config["id"]}'
            catalog.normalize_hrefs(catalog_self_href, strategy=strategy)
            recursive_save(catalog, options.ni)
        else:
            # For full catalog save with items this still seems to be faster
            catalog_self_href = catalog_config.get(
                "endpoint", "{}/{}".format(options.outputpath, catalog_config["id"])
            )
            catalog.normalize_hrefs(catalog_self_href, strategy=strategy)
            catalog.save(dest_href="{}/{}".format(options.outputpath, catalog_config["id"]))
        end = time.time()
        LOGGER.info(f"Catalog {catalog_config['id']}: Time consumed in saving: {end - start}")

        if options.vd:
            # try to validate catalog if flag was set
            LOGGER.info(f"Running validation of catalog {file_path}")
            try:
                validate_all(catalog.to_dict(), href=catalog_config["endpoint"])
            except Exception as e:
                LOGGER.info(f"Issue validation collection: {e}")


def extract_indicator_info(parent_collection: Collection):
    to_extract = [
        "subcode",
        "themes",
        "keywords",
        "satellite",
        "sensor",
        "cities",
        "countries",
        "thumbnail",
    ]
    summaries: dict[str, Any] = {}
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
            if collection.summaries.lists and collection.summaries.lists.get(key):
                for p in collection.summaries.lists[key]:
                    summaries[key].add(p)

    for key in to_extract:
        # convert all items back to a list
        summaries[key] = list(summaries[key])
        # remove empty ones
        if len(summaries[key]) == 0:
            del summaries[key]
    parent_collection.summaries = Summaries(summaries)


def process_indicator_file(
    catalog_config: dict, file_path: str, catalog: Catalog, options: Options
):
    with open(file_path) as f:
        LOGGER.info(f"Processing indicator: {file_path}")
        indicator_config: dict = yaml.load(f, Loader=SafeLoader)
        parent_indicator = get_or_create_collection(
            catalog, indicator_config["Name"], indicator_config, catalog_config, {}
        )
        if "Collections" in indicator_config:
            for collection in indicator_config["Collections"]:
                process_collection_file(
                    catalog_config,
                    f"{options.collectionspath}/{collection}.yaml",
                    parent_indicator,
                    options,
                )
        else:
            # we assume that collection files can also be loaded directly
            process_collection_file(catalog_config, file_path, parent_indicator, options)
        add_collection_information(catalog_config, parent_indicator, indicator_config)
        if iter_len_at_least(parent_indicator.get_items(recursive=True), 1):
            parent_indicator.update_extent_from_items()
        # Add bbox extents from children
        for c_child in parent_indicator.get_children():
            if isinstance(c_child, Collection):  # typing reason
                parent_indicator.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
        # extract collection information and add it to summary indicator level
        extract_indicator_info(parent_indicator)
        add_process_info(parent_indicator, catalog_config, indicator_config)
        # add baselayer and overview information to indicator collection
        add_base_overlay_info(parent_indicator, catalog_config, indicator_config)
        add_to_catalog(parent_indicator, catalog, {}, indicator_config)


@retry((Exception), tries=3, delay=5, backoff=2, logger=LOGGER)
def process_collection_file(
    catalog_config: dict, file_path: str, catalog: Catalog | Collection, options: Options
):
    LOGGER.info(f"Processing collection: {file_path}")
    with open(file_path) as f:
        collection_config: dict = yaml.load(f, Loader=SafeLoader)
        if "Resources" in collection_config:
            for endpoint_config in collection_config["Resources"]:
                try:
                    collection = None
                    if endpoint_config["Name"] == "Sentinel Hub":
                        collection = handle_SH_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog, options
                        )
                    elif endpoint_config["Name"] == "Sentinel Hub WMS":
                        collection = handle_SH_WMS_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "GeoDB":
                        collection = handle_GeoDB_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "VEDA":
                        collection = handle_VEDA_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog, options
                        )
                    elif endpoint_config["Name"] == "marinedatastore":
                        collection = handle_WMS_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog, wmts=True
                        )
                    elif endpoint_config["Name"] == "xcube":
                        collection = handle_xcube_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "rasdaman":
                        collection = handle_rasdaman_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "WMS":
                        collection = handle_WMS_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "JAXA_WMTS_PALSAR":
                        # somewhat one off creation of individual WMTS layers as individual items
                        collection = handle_WMS_endpoint(
                            catalog_config, endpoint_config, collection_config, catalog, wmts=True
                        )
                    elif endpoint_config["Name"] == "Collection-only":
                        collection = handle_collection_only(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    elif endpoint_config["Name"] == "Custom-Endpoint":
                        collection = handle_custom_endpoint(
                            catalog_config,
                            endpoint_config,
                            collection_config,
                            catalog,
                        )
                    elif endpoint_config["Name"] in [
                        "COG source",
                        "GeoJSON source",
                        "FlatGeobuf source",
                    ]:
                        collection = handle_raw_source(
                            catalog_config, endpoint_config, collection_config, catalog
                        )
                    else:
                        raise ValueError("Type of Resource is not supported")
                    if collection:
                        add_single_item_if_collection_empty(collection)
                        add_projection_info(endpoint_config, collection)
                        add_to_catalog(collection, catalog, endpoint_config, collection_config)
                    else:
                        raise Exception(
                            f"No collection was generated for resource {endpoint_config}"
                        )
                except Exception as e:
                    LOGGER.warn(f"""Exception: {e.args[0]} with config: {endpoint_config}""")
                    raise e

        elif "Subcollections" in collection_config:
            # if no endpoint is specified we check for definition of subcollections
            parent_collection = get_or_create_collection(
                catalog, collection_config["Name"], collection_config, catalog_config, {}
            )

            locations = []
            countries = []
            for sub_coll_def in collection_config["Subcollections"]:
                # Subcollection has only data on one location which
                # is defined for the entire collection
                if "Name" in sub_coll_def and "Point" in sub_coll_def:
                    locations.append(sub_coll_def["Name"])
                    if isinstance(sub_coll_def["Country"], list):
                        countries.extend(sub_coll_def["Country"])
                    else:
                        countries.append(sub_coll_def["Country"])
                    process_collection_file(
                        catalog_config,
                        "{}/{}.yaml".format(options.collectionspath, sub_coll_def["Collection"]),
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
                            latlng = "{},{}".format(
                                sub_coll_def["Point"][1],
                                sub_coll_def["Point"][0],
                            )
                            link.extra_fields["id"] = sub_coll_def["Identifier"]
                            link.extra_fields["latlng"] = latlng
                            link.extra_fields["name"] = sub_coll_def["Name"]
                    # Update title of collection to use location name
                    sub_collection = parent_collection.get_child(id=sub_coll_def["Identifier"])
                    if sub_collection:
                        sub_collection.title = sub_coll_def["Name"]
                # The subcollection has multiple locations which need to be extracted
                # and elevated to parent collection level
                else:
                    # create temp catalog to save collection
                    tmp_catalog = Catalog(id="tmp_catalog", description="temp catalog placeholder")
                    process_collection_file(
                        catalog_config,
                        "{}/{}.yaml".format(options.collectionspath, sub_coll_def["Collection"]),
                        tmp_catalog,
                        options,
                    )
                    links = tmp_catalog.get_child(sub_coll_def["Identifier"]).get_links()  # type: ignore
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

            add_collection_information(catalog_config, parent_collection, collection_config)
            add_process_info(catalog_config, parent_collection, collection_config)
            parent_collection.update_extent_from_items()
            # Add bbox extents from children
            for c_child in parent_collection.get_children():
                if isinstance(c_child, Collection):
                    parent_collection.extent.spatial.bboxes.append(c_child.extent.spatial.bboxes[0])
            # Fill summaries for locations
            parent_collection.summaries = Summaries(
                {
                    "cities": list(set(locations)),
                    "countries": list(set(countries)),
                }
            )
            add_to_catalog(parent_collection, catalog, {}, collection_config)


def add_to_catalog(
    collection: Collection, catalog: Catalog, endpoint: dict, collection_config: dict
):
    # check if already in catalog, if it is do not re-add it
    # TODO: probably we should add to the catalog only when creating
    for cat_coll in catalog.get_collections():
        if cat_coll.id == collection.id:
            return

    link: Link = catalog.add_child(collection)
    # bubble fields we want to have up to collection link and add them to collection
    if endpoint and "Type" in endpoint:
        collection.extra_fields["endpointtype"] = "{}_{}".format(
            endpoint["Name"],
            endpoint["Type"],
        )
        link.extra_fields["endpointtype"] = "{}_{}".format(
            endpoint["Name"],
            endpoint["Type"],
        )
    elif endpoint:
        collection.extra_fields["endpointtype"] = endpoint["Name"]
        link.extra_fields["endpointtype"] = endpoint["Name"]
    if "Subtitle" in collection_config:
        link.extra_fields["subtitle"] = collection_config["Subtitle"]
    link.extra_fields["title"] = collection.title
    link.extra_fields["code"] = collection_config["EodashIdentifier"]
    link.extra_fields["id"] = collection_config["Name"]
    if "Themes" in collection_config:
        link.extra_fields["themes"] = collection_config["Themes"]
    # Check for summaries and bubble up info
    if collection.summaries.lists:
        for summary in collection.summaries.lists:
            link.extra_fields[summary] = collection.summaries.lists[summary]

    add_extra_fields(link, collection_config)
    return link


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
@click.option("-ni", is_flag=True, help="no items flag, if set, items will not be saved")
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
        file_path = f"{catalogspath}/{file_name}"
        if os.path.isfile(file_path) and (
            catalog is None or os.path.splitext(file_name)[0] == catalog
        ):
            tasks.append(RaisingThread(target=process_catalog_file, args=(file_path, options)))
            tasks[-1].start()
    for task in tasks:
        task.join()
