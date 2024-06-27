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


def get_or_create_collection_and_times(
    catalog: Catalog, collection_id: str, data: dict, config: dict, endpoint: dict
) -> tuple[Collection, list[str]]:
    # Check if collection already in catalog
    for collection in catalog.get_collections():
        if collection.id == collection_id:
            return collection, []
    # If none found create a new one
    spatial_extent = endpoint.get("OverwriteBBox", [-180.0, -90.0, 180.0, 90.0])

    spatial_extent = SpatialExtent(
        [
            spatial_extent,
        ]
    )
    times: list[str] = []
    temporal_extent = TemporalExtent([[datetime.now(), None]])
    if endpoint and endpoint.get("Type") == "OverwriteTimes":
        if endpoint.get("Times"):
            times = list(endpoint.get("Times", []))
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent([[times_datetimes[0], times_datetimes[-1]]])
        elif endpoint.get("DateTimeInterval"):
            start = endpoint["DateTimeInterval"].get("Start", "2020-09-01T00:00:00")
            end = endpoint["DateTimeInterval"].get("End", "2020-10-01T00:00:00")
            timedelta_config = endpoint["DateTimeInterval"].get("Timedelta", {"days": 1})
            times = generateDateIsostringsFromInterval(start, end, timedelta_config)
            times_datetimes = sorted([parser.isoparse(time) for time in times])
            temporal_extent = TemporalExtent([[times_datetimes[0], times_datetimes[-1]]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Check if description is link to markdown file
    if "Description" in data:
        description = data["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith("http"):
                # if full absolute path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif "Subtitle" in data:
                    print("WARNING: Markdown file could not be fetched")
                    description = data["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(f"{config["assets_endpoint"]}/{description}")
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
    stac_object: Collection | Catalog, data: dict, endpoint: dict, config: dict
) -> None:
    if "Services" in data:
        for service in data["Services"]:
            if service["Name"] == "Statistical API":
                service_type = service.get("Type", "byoc")
                stac_object.add_link(
                    Link(
                        rel="example",
                        target="{}/{}".format(config["assets_endpoint"], service["Script"]),
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
    elif "Resources" in data:
        for service in data["Resources"]:
            if service.get("Name") == "xcube":
                target_url = "{}/timeseries/{}/{}?aggMethods=median".format(
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


def add_collection_information(config: dict, collection: Collection, data: dict) -> None:
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
                    media_type=(data["License"][0].get("Type", "text/html")),
                )
                if "Title" in data["License"][0]:
                    link.title = data["License"][0]["Title"]
                collection.links.append(link)
            elif len(data["License"]) > 1:
                collection.license = "various"
                for license_entry in data["License"]:
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
            license_data = lookup.by_id(data["License"])
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

    if "Provider" in data:
        try:
            collection.providers = [
                Provider(
                    # convert information to lower case
                    **{k.lower(): v for k, v in provider.items()}
                )
                for provider in data["Provider"]
            ]
        except Exception:
            print(f"WARNING: Issue creating provider information for collection: {collection.id}")

    if "Citation" in data:
        if "DOI" in data["Citation"]:
            collection.extra_fields["sci:doi"] = data["Citation"]["DOI"]
        if "Citation" in data["Citation"]:
            collection.extra_fields["sci:citation"] = data["Citation"]["Citation"]
        if "Publication" in data["Citation"]:
            collection.extra_fields["sci:publications"] = [
                # convert keys to lower case
                {k.lower(): v for k, v in publication.items()}
                for publication in data["Citation"]["Publication"]
            ]

    if "Subtitle" in data:
        collection.extra_fields["subtitle"] = data["Subtitle"]
    if "Legend" in data:
        collection.add_asset(
            "legend",
            Asset(
                href=f"{config["assets_endpoint"]}/{data["Legend"]}",
                media_type="image/png",
                roles=["metadata"],
            ),
        )
    if "Story" in data:
        collection.add_asset(
            "story",
            Asset(
                href=f"{config["assets_endpoint"]}/{data["Story"]}",
                media_type="text/markdown",
                roles=["metadata"],
            ),
        )
    if "Image" in data:
        collection.add_asset(
            "thumbnail",
            Asset(
                href=f"{config["assets_endpoint"]}/{data["Image"]}",
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


def add_base_overlay_info(collection: Collection, config: dict, data: dict) -> None:
    # check if default base layers defined
    if "default_base_layers" in config:
        with open(f"{config["default_base_layers"]}.yaml") as f:
            base_layers = yaml.load(f, Loader=SafeLoader)
            for layer in base_layers:
                collection.add_link(create_web_map_link(layer, role="baselayer"))
    # check if default overlay layers defined
    if "default_overlay_layers" in config:
        with open("{}.yaml".format(config["default_overlay_layers"])) as f:
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


def add_extra_fields(stac_object: Collection | Catalog | Link, data: dict) -> None:
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
                stac_object.extra_fields["sensor"] = data["DataSource"]["Spaceborne"]["Sensor"]
            if "Satellite" in data["DataSource"]["Spaceborne"]:
                stac_object.extra_fields["satellite"] = data["DataSource"]["Spaceborne"][
                    "Satellite"
                ]
        if "InSitu" in data["DataSource"]:
            stac_object.extra_fields["insituSources"] = data["DataSource"]["InSitu"]
        if "Other" in data["DataSource"]:
            stac_object.extra_fields["otherSources"] = data["DataSource"]["Other"]
