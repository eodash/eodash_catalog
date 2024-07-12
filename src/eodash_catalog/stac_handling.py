from datetime import datetime

import requests
import spdx_lookup as lookup
import yaml
from dateutil import parser
from pystac import (
    Asset,
    Catalog,
    Collection,
    Extent,
    Link,
    Provider,
    SpatialExtent,
    TemporalExtent,
)
from yaml.loader import SafeLoader

from eodash_catalog.utils import generateDateIsostringsFromInterval


def get_or_create_collection(
    catalog: Catalog,
    collection_id: str,
    collection_config: dict,
    catalog_config: dict,
    endpoint_config: dict,
) -> Collection:
    # Check if collection already in catalog
    for collection in catalog.get_collections():
        if collection.id == collection_id:
            return collection
    # If none found create a new one
    spatial_extent = endpoint_config.get("OverwriteBBox", [-180.0, -90.0, 180.0, 90.0])

    spatial_extent = SpatialExtent(
        [
            spatial_extent,
        ]
    )
    times: list[str] = []
    temporal_extent = TemporalExtent([[datetime.now(), None]])
    if endpoint_config and endpoint_config.get("Type") == "OverwriteTimes":
        if endpoint_config.get("Times"):
            times = list(endpoint_config.get("Times", []))
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent([[times_datetimes[0], times_datetimes[-1]]])
        elif endpoint_config.get("DateTimeInterval"):
            start = endpoint_config["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
            end = endpoint_config["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
            timedelta_config = endpoint_config["DateTimeInterval"].get("Timedelta", {"days": 1})
            times = generateDateIsostringsFromInterval(start, end, timedelta_config)
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent([[times_datetimes[0], times_datetimes[-1]]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Check if description is link to markdown file
    if "Description" in collection_config:
        description = collection_config["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith("http"):
                # if full absolute path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in collection_config:
                    print("WARNING: Markdown file could not be fetched")
                    description = collection_config["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(f'{catalog_config["assets_endpoint"]}/{description}')
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in collection_config:
                    print("WARNING: Markdown file could not be fetched")
                    description = collection_config["Subtitle"]
    elif "Subtitle" in collection_config:
        # Try to use at least subtitle to fill some information
        description = collection_config["Subtitle"]

    collection = Collection(
        id=collection_id,
        title=collection_config["Title"],
        description=description,
        extent=extent,
    )
    return collection


def create_web_map_link(layer: dict, role: str) -> Link:
    extra_fields = {
        "roles": [role],
        "id": layer["id"],
    }
    if layer.get("default"):
        extra_fields["roles"].append("default")
    if layer.get("visible"):
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
        media_type=layer.get("media_type", "image/png"),
        title=layer["name"],
        extra_fields=extra_fields,
    )
    return wml


def add_example_info(
    stac_object: Collection | Catalog,
    collection_config: dict,
    endpoint_config: dict,
    catalog_config: dict,
) -> None:
    if "Services" in collection_config:
        for service in collection_config["Services"]:
            if service["Name"] == "Statistical API":
                service_type = service.get("Type", "byoc")
                stac_object.add_link(
                    Link(
                        rel="example",
                        target="{}/{}".format(catalog_config["assets_endpoint"], service["Script"]),
                        title="evalscript",
                        media_type="application/javascript",
                        extra_fields={
                            "example:language": "JavaScript",
                            "dataId": "{}-{}".format(service_type, service["CollectionId"]),
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
                        title=(service["Title"] if "Title" in service else service["Name"]),
                        media_type="application/x-ipynb+json",
                        extra_fields={
                            "example:language": "Jupyter Notebook",
                            "example:container": True,
                        },
                    )
                )
    elif "Resources" in collection_config:
        for service in collection_config["Resources"]:
            if service.get("Name") == "xcube":
                target_url = "{}/timeseries/{}/{}?aggMethods=median".format(
                    endpoint_config["EndPoint"],
                    endpoint_config["DatacubeId"],
                    endpoint_config["Variable"],
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


def add_collection_information(
    catalog_config: dict, collection: Collection, collection_config: dict
) -> None:
    # Add metadata information
    # Check license identifier
    if "License" in collection_config:
        # Check if list was provided
        if isinstance(collection_config["License"], list):
            if len(collection_config["License"]) == 1:
                collection.license = "proprietary"
                link = Link(
                    rel="license",
                    target=collection_config["License"][0]["Url"],
                    media_type=(collection_config["License"][0].get("Type", "text/html")),
                )
                if "Title" in collection_config["License"][0]:
                    link.title = collection_config["License"][0]["Title"]
                collection.links.append(link)
            elif len(collection_config["License"]) > 1:
                collection.license = "various"
                for license_entry in collection_config["License"]:
                    link = Link(
                        rel="license",
                        target=license_entry["Url"],
                        media_type="text/html"
                        if "Type" in license_entry
                        else license_entry["Type"],
                    )
                    if "Title" in license_entry:
                        link.title = license_entry["Title"]
                    collection.links.append(link)
        else:
            license_data = lookup.by_id(collection_config["License"])
            if license_data is not None:
                collection.license = license_data.id
                if license_data.sources:
                    # add links to licenses
                    for source in license_data.sources:
                        collection.links.append(
                            Link(
                                rel="license",
                                target=source,
                                media_type="text/html",
                            )
                        )
            else:
                # fallback to proprietary
                print("WARNING: License could not be parsed, falling back to proprietary")
                collection.license = "proprietary"
    else:
        # print("WARNING: No license was provided, falling back to proprietary")
        pass

    if "Provider" in collection_config:
        try:
            collection.providers = [
                Provider(
                    # convert information to lower case
                    **{k.lower(): v for k, v in provider.items()}
                )
                for provider in collection_config["Provider"]
            ]
        except Exception:
            print(f"WARNING: Issue creating provider information for collection: {collection.id}")

    if "Citation" in collection_config:
        if "DOI" in collection_config["Citation"]:
            collection.extra_fields["sci:doi"] = collection_config["Citation"]["DOI"]
        if "Citation" in collection_config["Citation"]:
            collection.extra_fields["sci:citation"] = collection_config["Citation"]["Citation"]
        if "Publication" in collection_config["Citation"]:
            collection.extra_fields["sci:publications"] = [
                # convert keys to lower case
                {k.lower(): v for k, v in publication.items()}
                for publication in collection_config["Citation"]["Publication"]
            ]

    if "Subtitle" in collection_config:
        collection.extra_fields["subtitle"] = collection_config["Subtitle"]
    if "Legend" in collection_config:
        collection.add_asset(
            "legend",
            Asset(
                href=f'{catalog_config["assets_endpoint"]}/{collection_config["Legend"]}',
                media_type="image/png",
                roles=["metadata"],
            ),
        )
    if "Story" in collection_config:
        collection.add_asset(
            "story",
            Asset(
                href=f'{catalog_config["assets_endpoint"]}/{collection_config["Story"]}',
                media_type="text/markdown",
                roles=["metadata"],
            ),
        )
    if "Image" in collection_config:
        collection.add_asset(
            "thumbnail",
            Asset(
                href=f'{catalog_config["assets_endpoint"]}/{collection_config["Image"]}',
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
    # Add extra fields to collection if available
    add_extra_fields(collection, collection_config)

    if "References" in collection_config:
        generic_counter = 1
        for ref in collection_config["References"]:
            if "Key" in ref:
                key = ref["Key"]
            else:
                key = f"reference_{generic_counter}"
                generic_counter = generic_counter + 1
            collection.add_asset(
                key,
                Asset(
                    href=ref["Url"],
                    title=ref["Name"],
                    media_type=ref.get("MediaType", "text/html"),
                    roles=["metadata"],
                ),
            )


def add_base_overlay_info(
    collection: Collection, catalog_config: dict, collection_config: dict
) -> None:
    # check if default base layers defined
    if "default_base_layers" in catalog_config:
        with open(f'{catalog_config["default_base_layers"]}.yaml') as f:
            base_layers = yaml.load(f, Loader=SafeLoader)
            for layer in base_layers:
                collection.add_link(create_web_map_link(layer, role="baselayer"))
    # check if default overlay layers defined
    if "default_overlay_layers" in catalog_config:
        with open("{}.yaml".format(catalog_config["default_overlay_layers"])) as f:
            overlay_layers = yaml.load(f, Loader=SafeLoader)
            for layer in overlay_layers:
                collection.add_link(create_web_map_link(layer, role="overlay"))
    if "BaseLayers" in collection_config:
        for layer in collection_config["BaseLayers"]:
            collection.add_link(create_web_map_link(layer, role="baselayer"))
    if "OverlayLayers" in collection_config:
        for layer in collection_config["OverlayLayers"]:
            collection.add_link(create_web_map_link(layer, role="overlay"))
    # TODO: possibility to overwrite default base and overlay layers


def add_extra_fields(stac_object: Collection | Catalog | Link, collection_config: dict) -> None:
    if "yAxis" in collection_config:
        stac_object.extra_fields["yAxis"] = collection_config["yAxis"]
    if "Themes" in collection_config:
        stac_object.extra_fields["themes"] = collection_config["Themes"]
    if "Locations" in collection_config or "Subcollections" in collection_config:
        stac_object.extra_fields["locations"] = True
    if "Tags" in collection_config:
        stac_object.extra_fields["tags"] = collection_config["Tags"]
    if "Satellite" in collection_config:
        stac_object.extra_fields["satellite"] = collection_config["Satellite"]
    if "Sensor" in collection_config:
        stac_object.extra_fields["sensor"] = collection_config["Sensor"]
    if "Agency" in collection_config:
        stac_object.extra_fields["agency"] = collection_config["Agency"]
    if "EodashIdentifier" in collection_config:
        stac_object.extra_fields["subcode"] = collection_config["EodashIdentifier"]
    if "DataSource" in collection_config:
        if "Spaceborne" in collection_config["DataSource"]:
            if "Sensor" in collection_config["DataSource"]["Spaceborne"]:
                stac_object.extra_fields["sensor"] = collection_config["DataSource"]["Spaceborne"][
                    "Sensor"
                ]
            if "Satellite" in collection_config["DataSource"]["Spaceborne"]:
                stac_object.extra_fields["satellite"] = collection_config["DataSource"][
                    "Spaceborne"
                ]["Satellite"]
        if "InSitu" in collection_config["DataSource"]:
            stac_object.extra_fields["insituSources"] = collection_config["DataSource"]["InSitu"]
        if "Other" in collection_config["DataSource"]:
            stac_object.extra_fields["otherSources"] = collection_config["DataSource"]["Other"]
    if "MapProjection" in collection_config:
        stac_object.extra_fields["mapProjection"] = collection_config["MapProjection"]


def get_collection_times_from_config(endpoint_config: dict) -> list[str]:
    times: list[str] = []
    if endpoint_config and endpoint_config.get("Type") == "OverwriteTimes":
        if endpoint_config.get("Times"):
            times = list(endpoint_config.get("Times", []))
        elif endpoint_config.get("DateTimeInterval"):
            start = endpoint_config["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
            end = endpoint_config["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
            timedelta_config = endpoint_config["DateTimeInterval"].get("Timedelta", {"days": 1})
            times = generateDateIsostringsFromInterval(start, end, timedelta_config)
    return times
