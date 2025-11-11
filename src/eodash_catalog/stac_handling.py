import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import requests
import spdx_lookup as lookup
from pystac import (
    Asset,
    Catalog,
    Collection,
    Extent,
    Item,
    Link,
    Provider,
    SpatialExtent,
    TemporalExtent,
)
from structlog import get_logger

from eodash_catalog.utils import (
    generateDatetimesFromInterval,
    get_full_url,
    make_intervals,
    parse_datestring_to_tz_aware_datetime,
    read_config_file,
)

LOGGER = get_logger(__name__)


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
    temporal_extent = TemporalExtent([[datetime.now(tz=timezone.utc), None]])
    if endpoint_config:
        times_datetimes = get_collection_datetimes_from_config(endpoint_config)
        if len(times_datetimes) > 0:
            temporal_extent = TemporalExtent([[times_datetimes[0], times_datetimes[-1]]])

    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)
    description = ""
    # Check if description is link to markdown file
    if collection_config.get("Description"):
        description = collection_config["Description"]
        if description.endswith((".md", ".MD")):
            if description.startswith("http"):
                # if full absolute path is defined
                response = requests.get(description)
                if response.status_code == 200:
                    description = response.text
                elif collection_config.get("Subtitle"):
                    LOGGER.warn("Markdown file could not be fetched")
                    description = collection_config["Subtitle"]
            else:
                # relative path to assets was given
                response = requests.get(f'{catalog_config["assets_endpoint"]}/{description}')
                if response.status_code == 200:
                    description = response.text
                elif collection_config.get("Subtitle"):
                    LOGGER.warn("Markdown file could not be fetched")
                    description = collection_config["Subtitle"]
    elif collection_config.get("Subtitle"):
        # Try to use at least subtitle to fill some information
        description = collection_config["Subtitle"]

    collection = Collection(
        id=collection_id,
        title=collection_config["Title"],
        description=description,
        extent=extent,
    )
    return collection


def create_service_link(
    endpoint_config: dict, catalog_config: dict, location_id: str | None = None
) -> Link:
    extra_fields = {
        "id": endpoint_config["Identifier"],
        "method": endpoint_config.get("Method", "GET"),
    }
    if endpoint_config.get("EndPoint"):
        extra_fields["endpoint"] = endpoint_config["EndPoint"]
    if endpoint_config.get("Body"):
        extra_fields["body"] = get_full_url(endpoint_config["Body"], catalog_config)
    if endpoint_config.get("Flatstyle"):
        # either a string
        if isinstance(endpoint_config["Flatstyle"], str):
            # update URL if needed
            extra_fields["eox:flatstyle"] = get_full_url(
                endpoint_config["Flatstyle"], catalog_config
            )
        elif isinstance(endpoint_config["Flatstyle"], list):
            # or a list of objects - update URL if needed
            extra_fields["eox:flatstyle"] = []
            for flatstyle_config in endpoint_config["Flatstyle"]:
                flatstyle_obj = {
                    "id": flatstyle_config.get("Identifier"),
                    "url": get_full_url(flatstyle_config.get("Url"), catalog_config),
                }
                extra_fields["eox:flatstyle"].append(flatstyle_obj)
        else:
            LOGGER.warn("Flatstyle is invalid type", endpoint_config["Flatstyle"])
    url = endpoint_config["Url"]
    if location_id:
        url = url.replace("{{feature}}", location_id)
    sl = Link(
        rel="service",
        target=url,
        media_type=endpoint_config["Type"],
        extra_fields=extra_fields,
    )
    return sl


def create_web_map_link(
    collection: Collection, catalog_config: dict, layer_config: dict, role: str
) -> Link:
    extra_fields = {
        "roles": [role],
        "id": layer_config["id"],
    }
    media_type = (layer_config.get("media_type", "image/png"),)
    if layer_config.get("default"):
        extra_fields["roles"].append("default")
    if layer_config.get("visible"):
        extra_fields["roles"].append("visible")
    if "visible" in layer_config and not layer_config["visible"]:
        extra_fields["roles"].append("invisible")

    match layer_config["protocol"].lower():
        case "wms":
            # handle wms special config options
            extra_fields["wms:layers"] = layer_config["layers"]
            if layer_config.get("styles"):
                extra_fields["wms:styles"] = layer_config["styles"]
            if layer_config.get("dimensions"):
                extra_fields["wms:dimensions"] = layer_config["dimensions"]
        case "wmts":
            extra_fields["wmts:layer"] = layer_config["layer"]
            if layer_config.get("dimensions"):
                extra_fields["wmts:dimensions"] = layer_config["dimensions"]
        case "vector-tile":
            identifier = str(uuid.uuid4())
            extra_fields["key"] = identifier
            media_type = "application/vnd.mapbox-vector-tile"
            if vector_tile_id_property := layer_config.get("idProperty"):
                extra_fields["idProperty"] = vector_tile_id_property
            if vector_tile_id_property := layer_config.get("layers"):
                layer_config["layers"] = vector_tile_id_property
            if ep_st := layer_config.get("Style"):
                style_link = Link(
                    rel="style",
                    target=ep_st
                    if ep_st.startswith("http")
                    else f"{catalog_config['assets_endpoint']}/{ep_st}",
                    media_type="text/vector-styles",
                    extra_fields={"links:keys": [identifier]},
                )
                collection.add_link(style_link)
            add_authentication(collection, layer_config["url"], extra_fields)

    if layer_config.get("Attribution"):
        extra_fields["attribution"] = layer_config["Attribution"]
    if layer_config.get("Colorlegend"):
        extra_fields["eox:colorlegend"] = layer_config["Colorlegend"]
    wml = Link(
        rel=layer_config["protocol"],
        target=layer_config["url"],
        media_type=media_type,
        title=layer_config["name"],
        extra_fields=extra_fields,
    )
    add_projection_info(layer_config, wml)
    return wml


def add_example_info(
    stac_object: Collection | Catalog,
    collection_config: dict,
    endpoint_config: dict,
    catalog_config: dict,
) -> None:
    if collection_config.get("Services"):
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
                        title=service.get("Title", service.get("Name")),
                        media_type="application/x-ipynb+json",
                        extra_fields={
                            "example:language": "Jupyter Notebook",
                            "example:container": True,
                        },
                    )
                )


def add_collection_information(
    catalog_config: dict,
    collection: Collection,
    collection_config: dict,
    is_root_collection: bool = False,
) -> None:
    # Add metadata information
    # Check license identifier
    if collection_config.get("License"):
        # Check if list was provided
        if isinstance(collection_config["License"], list):
            if len(collection_config["License"]) == 1:
                collection.license = "proprietary"
                link = Link(
                    rel="license",
                    target=collection_config["License"][0]["Url"],
                    media_type=(collection_config["License"][0].get("Type", "text/html")),
                )
                if collection_config["License"][0].get("Title"):
                    link.title = collection_config["License"][0]["Title"]
                collection.links.append(link)
            elif len(collection_config["License"]) > 1:
                collection.license = "various"
                for license_entry in collection_config["License"]:
                    link = Link(
                        rel="license",
                        target=license_entry["Url"],
                        media_type=license_entry.get("Type", "text/html"),
                    )
                    if license_entry.get("Title"):
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
                LOGGER.warn("License could not be parsed, falling back to proprietary")
                collection.license = "proprietary"
    else:
        pass

    if collection_config.get("Provider"):
        try:
            collection.providers = [
                Provider(
                    # convert information to lower case
                    **{k.lower(): v for k, v in provider.items()}
                )
                for provider in collection_config["Provider"]
            ]
        except Exception:
            LOGGER.warn(f"Issue creating provider information for collection: {collection.id}")

    if collection_config.get("Citation"):
        if collection_config["Citation"].get("DOI"):
            collection.extra_fields["sci:doi"] = collection_config["Citation"]["DOI"]
        if collection_config["Citation"].get("Citation"):
            collection.extra_fields["sci:citation"] = collection_config["Citation"]["Citation"]
        if collection_config["Citation"].get("Publication"):
            collection.extra_fields["sci:publications"] = [
                # convert keys to lower case
                {k.lower(): v for k, v in publication.items()}
                for publication in collection_config["Citation"]["Publication"]
            ]

    if collection_config.get("Subtitle"):
        collection.extra_fields["subtitle"] = collection_config["Subtitle"]

    if collection_config.get("ShortDescription"):
        collection.extra_fields["shortdescription"] = collection_config["ShortDescription"]

    if collection_config.get("Legend"):
        collection.add_asset(
            "legend",
            Asset(
                href=f'{catalog_config["assets_endpoint"]}/{collection_config["Legend"]}',
                media_type="image/png",
                roles=["metadata"],
            ),
        )
    if stories := collection_config.get("Stories"):
        for story in stories:
            story_url = story.get("Url")
            if not story_url.startswith("http"):
                story_url = f'{catalog_config.get("stories_endpoint")}/{story_url}'
            parsed_url = urlparse(story_url)
            # check if it is URL with a query parameter id=story-identifier
            if parsed_url.query and len(parse_qs(parsed_url.query).get("id")) > 0:
                story_id = parse_qs(parsed_url.query).get("id")[0]
            else:
                story_id = parsed_url.path.rsplit("/")[-1].replace(".md", "").replace(".MD", "")
            collection.add_asset(
                story_id,
                Asset(
                    title=story.get("Name"),
                    href=story_url,
                    media_type="text/markdown",
                    roles=["metadata", "story"],
                ),
            )
    if collection_config.get("Image"):
        # Check if absolute URL or relative path
        if collection_config["Image"].startswith("http"):
            image_url = collection_config["Image"]
        else:
            image_url = f'{catalog_config["assets_endpoint"]}/{collection_config["Image"]}'
        collection.add_asset(
            "thumbnail",
            Asset(
                href=image_url,
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
        # Bubble up thumbnail to extra fields
        collection.extra_fields["thumbnail"] = image_url
    # Add extra fields to collection if available
    add_extra_fields(collection, collection_config, is_root_collection)

    if collection_config.get("References"):
        generic_counter = 1
        for ref in collection_config["References"]:
            if ref.get("Key"):
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
    if collection_config.get("Colorlegend"):
        collection.extra_fields["eox:colorlegend"] = collection_config["Colorlegend"]


def add_process_info(collection: Collection, catalog_config: dict, collection_config: dict) -> None:
    if any(collection_config.get(key) for key in ["Locations", "Subcollections"]):
        # add the generic geodb-like selection process on the root collection instead of Processes
        if catalog_config.get("geodb_default_form"):
            # adding default geodb-like map handling for Locations
            collection.extra_fields["eodash:jsonform"] = get_full_url(
                catalog_config["geodb_default_form"], catalog_config
            )
        # link a process definition for getting a collection with {{feature}} placeholder
        sl = Link(
            rel="service",
            target="./" + collection.id + "/{{feature}}/collection.json",
            media_type="application/json; profile=collection",
            extra_fields={
                "id": "locations",
                "method": "GET",
                "type": "application/json; profile=collection",
                "endpoint": "STAC",
            },
        )
        collection.add_link(sl)
        has_geodb = any(
            item.get("Name") == "GeoDB" for item in collection_config.get("Resources", [])
        )
        # adding additional service links
        if has_geodb and collection_config.get("Process", {}).get("EndPoints"):
            for endpoint in collection_config["Process"]["EndPoints"]:
                collection.add_link(create_service_link(endpoint, catalog_config))

        # for geodb collections now based on locations, we want to make sure
        # also manually defined processes are added to the collection
        if has_geodb and collection_config.get("Process", {}).get("VegaDefinition"):
            collection.extra_fields["eodash:vegadefinition"] = get_full_url(
                collection_config["Process"]["VegaDefinition"], catalog_config
            )
    # elif is intentional for cases when Process is defined on collection with Locations
    # then we want to only add it to the "children", not the root
    elif collection_config.get("Process"):
        if collection_config["Process"].get("EndPoints"):
            for endpoint in collection_config["Process"]["EndPoints"]:
                collection.add_link(create_service_link(endpoint, catalog_config))
        if collection_config["Process"].get("JsonForm"):
            collection.extra_fields["eodash:jsonform"] = get_full_url(
                collection_config["Process"]["JsonForm"], catalog_config
            )
        if collection_config["Process"].get("VegaDefinition"):
            collection.extra_fields["eodash:vegadefinition"] = get_full_url(
                collection_config["Process"]["VegaDefinition"], catalog_config
            )
    elif collection_config.get("Resources"):
        # see if geodb resource configured use defaults if available
        for resource in collection_config["Resources"]:
            if resource["Name"] == "GeoDB":
                if catalog_config.get("geodb_default_form"):
                    collection.extra_fields["eodash:jsonform"] = get_full_url(
                        catalog_config["geodb_default_form"], catalog_config
                    )
                if catalog_config.get("geodb_default_vega"):
                    collection.extra_fields["eodash:vegadefinition"] = get_full_url(
                        catalog_config["geodb_default_vega"], catalog_config
                    )
                query_string = "?aoi_id=eq.{{feature}}&select=site_name,city,color_code,time,aoi,measurement_value,indicator_value,reference_time,eo_sensor,reference_value,input_data"  # noqa: E501
                collection.add_link(
                    Link(
                        rel="service",
                        target="{}{}_{}{}".format(
                            resource["EndPoint"],
                            resource["Database"],
                            resource["CollectionId"],
                            query_string,
                        ),
                        media_type="application/json",
                        extra_fields={
                            "method": "GET",
                            "id": resource["CollectionId"],
                        },
                    )
                )
            elif resource["Name"] == "xcube" and catalog_config.get("default_xcube_process"):
                target_url = "{}/timeseries/{}/{}?aggMethods=median".format(
                    resource["EndPoint"],
                    resource["DatacubeId"],
                    resource["Variable"],
                )
                process_endpoint_config = catalog_config["default_xcube_process"]["EndPoints"][0]
                extra_fields = {
                    "id": process_endpoint_config["Identifier"],
                    "method": process_endpoint_config.get("Method", "GET"),
                }
                extra_fields["body"] = get_full_url(process_endpoint_config["Body"], catalog_config)
                if catalog_config["default_xcube_process"].get("JsonForm"):
                    collection.extra_fields["eodash:jsonform"] = get_full_url(
                        catalog_config["default_xcube_process"]["JsonForm"], catalog_config
                    )
                if catalog_config["default_xcube_process"].get("VegaDefinition"):
                    collection.extra_fields["eodash:vegadefinition"] = get_full_url(
                        catalog_config["default_xcube_process"]["VegaDefinition"], catalog_config
                    )

                sl = Link(
                    rel="service",
                    target=target_url,
                    media_type=process_endpoint_config["Type"],
                    extra_fields=extra_fields,
                )
                collection.add_link(sl)


def add_process_info_child_collection(
    collection: Collection, catalog_config: dict, collection_config: dict, location_id: str | None
) -> None:
    # in case of locations, we add the process itself on a child collection
    if collection_config.get("Process"):
        if collection_config["Process"].get("EndPoints"):
            for endpoint in collection_config["Process"]["EndPoints"]:
                link = create_service_link(endpoint, catalog_config, location_id)
                collection.add_link(link)
        if collection_config["Process"].get("JsonForm"):
            if location_id and catalog_config.get("geodb_empty_form"):
                # specific handling of geodb locations to replace with an empty jsonform
                collection.extra_fields["eodash:jsonform"] = get_full_url(
                    catalog_config["geodb_empty_form"], catalog_config
                )
            else:
                # standard json form pass through
                collection.extra_fields["eodash:jsonform"] = get_full_url(
                    collection_config["Process"]["JsonForm"], catalog_config
                )
        if collection_config["Process"].get("VegaDefinition"):
            collection.extra_fields["eodash:vegadefinition"] = get_full_url(
                collection_config["Process"]["VegaDefinition"], catalog_config
            )


def add_base_overlay_info(
    collection: Collection, catalog_config: dict, collection_config: dict
) -> None:
    # add custom baselayers specially for this indicator
    if "BaseLayers" in collection_config:
        for layer in collection_config["BaseLayers"]:
            collection.add_link(
                create_web_map_link(collection, catalog_config, layer, role="baselayer")
            )
    # alternatively use default base layers defined
    elif catalog_config.get("default_base_layers"):
        base_layers = read_config_file(catalog_config["default_base_layers"])
        for layer in base_layers:
            collection.add_link(
                create_web_map_link(collection, catalog_config, layer, role="baselayer")
            )
    # add custom overlays just for this indicator
    if "OverlayLayers" in collection_config:
        for layer in collection_config["OverlayLayers"]:
            collection.add_link(
                create_web_map_link(collection, catalog_config, layer, role="overlay")
            )
    # check if default overlay layers defined
    elif catalog_config.get("default_overlay_layers"):
        overlay_layers = read_config_file(catalog_config["default_overlay_layers"])
        for layer in overlay_layers:
            collection.add_link(
                create_web_map_link(collection, catalog_config, layer, role="overlay")
            )


def add_extra_fields(
    stac_object: Collection | Link, collection_config: dict, is_root_collection: bool = False
) -> None:
    if collection_config.get("yAxis"):
        stac_object.extra_fields["yAxis"] = collection_config["yAxis"]
    if collection_config.get("Themes"):
        stac_object.extra_fields["themes"] = collection_config["Themes"]
    if (
        collection_config.get("Locations") or collection_config.get("Subcollections")
    ) and is_root_collection:
        stac_object.extra_fields["locations"] = True
    if collection_config.get("Tags"):
        stac_object.extra_fields["tags"] = collection_config["Tags"]
    if collection_config.get("Satellite"):
        stac_object.extra_fields["satellite"] = collection_config["Satellite"]
    if collection_config.get("Sensor"):
        stac_object.extra_fields["sensor"] = collection_config["Sensor"]
    if collection_config.get("Agency"):
        stac_object.extra_fields["agency"] = collection_config["Agency"]
    if collection_config.get("EodashIdentifier"):
        stac_object.extra_fields["subcode"] = collection_config["EodashIdentifier"]
    if collection_config.get("CollectionGroup"):
        stac_object.extra_fields["collection_group"] = collection_config["CollectionGroup"]
    if collection_config.get("DataSource"):
        if collection_config["DataSource"].get("Spaceborne"):
            if collection_config["DataSource"]["Spaceborne"].get("Sensor"):
                stac_object.extra_fields["sensor"] = collection_config["DataSource"]["Spaceborne"][
                    "Sensor"
                ]
            if collection_config["DataSource"]["Spaceborne"].get("Satellite"):
                stac_object.extra_fields["satellite"] = collection_config["DataSource"][
                    "Spaceborne"
                ]["Satellite"]
        if collection_config["DataSource"].get("InSitu"):
            stac_object.extra_fields["insituSources"] = collection_config["DataSource"]["InSitu"]
        if collection_config["DataSource"].get("Other"):
            stac_object.extra_fields["otherSources"] = collection_config["DataSource"]["Other"]
    if collection_config.get("MapProjection"):
        stac_object.extra_fields["eodash:mapProjection"] = collection_config["MapProjection"]


def get_collection_datetimes_from_config(endpoint_config: dict) -> list[datetime]:
    times_datetimes: list[datetime] = []
    if endpoint_config:
        interval_between_dates = endpoint_config.get("WMSIntervalsBetweenDates")
        if endpoint_config.get("Times"):
            times = list(endpoint_config.get("Times", []))
            times_datetimes = sorted(
                [parse_datestring_to_tz_aware_datetime(time) for time in times]
            )
            if interval_between_dates:
                # convert to list of datetime_start and datetime_end
                times_datetimes = make_intervals(times_datetimes)
        elif endpoint_config.get("DateTimeInterval"):
            start = endpoint_config["DateTimeInterval"].get("Start", "2020-09-01T00:00:00Z")
            end = endpoint_config["DateTimeInterval"].get("End", "2020-10-01T00:00:00Z")
            timedelta_config = endpoint_config["DateTimeInterval"].get("Timedelta", {"days": 1})
            times_datetimes = generateDatetimesFromInterval(
                start, end, timedelta_config, interval_between_dates
            )
    return times_datetimes


def add_projection_info(
    endpoint_config: dict, stac_object: Item | Asset | Collection | Link
) -> None:
    if proj := endpoint_config.get("DataProjection"):
        if isinstance(proj, str):
            if proj.lower().startswith("epsg"):
                # consider input such as "EPSG:4326"
                proj = proj.lower().split("EPSG:")[1]
            # consider a number only
            proj = int(proj)
        if isinstance(proj, int):
            # only set if not existing on source stac_object
            if not stac_object.extra_fields.get("proj:epsg"):
                # handling EPSG code for "proj:epsg"
                stac_object.extra_fields["proj:epsg"] = proj
        elif isinstance(proj, dict):
            # custom handling due to incompatibility of proj4js supported syntax (WKT1)
            # and STAC supported syntax (projjson or WKT2)
            # so we are taking over the DataProjection as is and deal with it in the eodash client
            # in a non-standard compliant way
            # https://github.com/proj4js/proj4js/issues/400
            stac_object.extra_fields["eodash:proj4_def"] = proj
        else:
            raise Exception(f"Incorrect type of proj definition {proj}")


def add_authentication(stac_object: Item | Collection | Catalog, url: str, extra_fields_link: dict):
    if "mapbox" in url:
        # add authentication info
        auth_extension = "https://stac-extensions.github.io/authentication/v1.1.0/schema.json"
        if auth_extension not in stac_object.stac_extensions:
            stac_object.stac_extensions.append(auth_extension)
        stac_object.extra_fields["auth:schemes"] = {
            "mapboxauth": {
                "type": "apiKey",
                "name": "access_token",
                "in": "query",
            }
        }
        extra_fields_link["auth:refs"] = ["mapboxauth"]
    pass
