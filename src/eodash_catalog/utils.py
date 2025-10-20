import json
import os
import re
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import reduce, wraps
from typing import Any

import pyarrow.compute as pc
import stac_geoparquet as stacgp
import yaml
from dateutil import parser
from owslib.wcs import WebCoverageService
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
from pystac import Asset, Catalog, Collection, Item, RelType, SpatialExtent, TemporalExtent
from pytz import timezone as pytztimezone
from shapely import geometry as sgeom
from shapely import wkb
from six import string_types
from structlog import get_logger

from eodash_catalog.duration import Duration

ISO8601_PERIOD_REGEX = re.compile(
    r"^(?P<sign>[+-])?"
    r"P(?!\b)"
    r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
    r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
    r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
    r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
    r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$"
)
# regular expression to parse ISO duartion strings.

LOGGER = get_logger(__name__)


def create_geojson_point(lon: int | float, lat: int | float) -> dict[str, Any]:
    point = {"type": "Point", "coordinates": [lon, lat]}
    return {"type": "Feature", "geometry": point, "properties": {}}


def create_geometry_from_bbox(bbox: list[float | int]) -> dict:
    """
    Create a GeoJSON geometry from a bounding box.
    Args:
        bbox (list[float | int]): A list containing the bounding box coordinates in the format
        [min_lon, min_lat, max_lon, max_lat].
    Returns:
        dict: A GeoJSON geometry object representing the bounding box.
    """
    coordinates = [
        [bbox[0], bbox[1]],
        [bbox[2], bbox[1]],
        [bbox[2], bbox[3]],
        [bbox[0], bbox[3]],
        [bbox[0], bbox[1]],
    ]
    return {"type": "Polygon", "coordinates": [coordinates]}


def retrieveExtentFromWCS(
    capabilities_url: str,
    coverage: str,
    version: str = "2.0.1",
) -> tuple[list[float], list[datetime]]:
    times = []
    try:
        service = WebCoverageService(capabilities_url, version)
        if coverage in list(service.contents):
            description = service.getDescribeCoverage(coverage)
            area_val = description.findall(".//{http://www.rasdaman.org}areasOfValidity")
            if len(area_val) == 1:
                areas = area_val[0].getchildren()
                if len(areas) > 1:
                    times = [t.get("start") for t in areas]
            # get unique times
            times = reduce(lambda re, x: [*re, x] if x not in re else re, times, [])
    except Exception as e:
        LOGGER.warn("Issue extracting information from service capabilities")
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        LOGGER.warn(message)
        raise e

    bbox = [-180.0, -90.0, 180.0, 90.0]
    owsnmspc = "{http://www.opengis.net/ows/2.0}"
    # somehow this is not parsed from the rasdaman endpoint
    if service and service[coverage].boundingBoxWGS84:
        bbox = [float(x) for x in service[coverage].boundingBoxWGS84]
    elif service:
        # we try to get it ourselves
        wgs84bbox = service.contents[coverage]._elem.findall(".//" + owsnmspc + "WGS84BoundingBox")
        if len(wgs84bbox) == 1:
            lc = wgs84bbox[0].find(".//" + owsnmspc + "LowerCorner").text
            uc = wgs84bbox[0].find(".//" + owsnmspc + "UpperCorner").text
            bbox = [
                float(lc.split()[0]),
                float(lc.split()[1]),
                float(uc.split()[0]),
                float(uc.split()[1]),
            ]
        description.findall(".//{http://www.rasdaman.org}areasOfValidity")

    datetimes = [parse_datestring_to_tz_aware_datetime(time_str) for time_str in times]
    return bbox, datetimes


def retrieveExtentFromWMSWMTS(
    capabilities_url: str, layer: str, version: str = "1.1.1", wmts: bool = False
) -> tuple[list[float], list[datetime]]:
    times = []
    try:
        if not wmts:
            service = WebMapService(capabilities_url, version=version)
        else:
            service = WebMapTileService(capabilities_url)
        if layer in list(service.contents):
            tps = []
            if not wmts and service[layer].timepositions is not None:
                tps = service[layer].timepositions
            elif wmts:
                time_dimension = service[layer].dimensions.get("time")
                # specifically taking 'time' dimension
                if time_dimension:
                    tps = time_dimension["values"]
            for tp in tps:
                tp_def = tp.split("/")
                if len(tp_def) > 1:
                    dates = interval(
                        parser.parse(tp_def[0]),
                        parser.parse(tp_def[1]),
                        parse_duration(tp_def[2]),
                    )
                    times += [x.strftime("%Y-%m-%dT%H:%M:%SZ") for x in dates]
                else:
                    times.append(tp)
            times = [time.replace("\n", "").strip() for time in times]
            # get unique times
            times = reduce(lambda re, x: [*re, x] if x not in re else re, times, [])
    except Exception as e:
        LOGGER.warn("Issue extracting information from service capabilities")
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        LOGGER.warn(message)
        raise e

    bbox = [-180.0, -90.0, 180.0, 90.0]
    if service and service[layer].boundingBoxWGS84:
        bbox = [float(x) for x in service[layer].boundingBoxWGS84]

    datetimes = [parse_datestring_to_tz_aware_datetime(time_str) for time_str in times]
    return bbox, datetimes


def interval(start: datetime, stop: datetime, delta: timedelta) -> Iterator[datetime]:
    while start <= stop:
        yield start
        start += delta
    yield stop


def parse_duration(datestring):
    """
    Parses an ISO 8601 durations into datetime.timedelta
    """
    if not isinstance(datestring, string_types):
        raise TypeError(f"Expecting a string {datestring}")
    match = ISO8601_PERIOD_REGEX.match(datestring)
    groups = {}
    if match:
        groups = match.groupdict()
    for key, val in groups.items():
        if key not in ("separator", "sign"):
            if val is None:
                groups[key] = "0n"
            if key in ("years", "months"):
                groups[key] = Decimal(groups[key][:-1].replace(",", "."))
            else:
                # these values are passed into a timedelta object,
                # which works with floats.
                groups[key] = float(groups[key][:-1].replace(",", "."))
    if groups["years"] == 0 and groups["months"] == 0:
        ret = timedelta(
            days=groups["days"],
            hours=groups["hours"],
            minutes=groups["minutes"],
            seconds=groups["seconds"],
            weeks=groups["weeks"],
        )
        if groups["sign"] == "-":
            ret = timedelta(0) - ret
    else:
        ret = Duration(
            years=groups["years"],
            months=groups["months"],
            days=groups["days"],
            hours=groups["hours"],
            minutes=groups["minutes"],
            seconds=groups["seconds"],
            weeks=groups["weeks"],
        )
        if groups["sign"] == "-":
            ret = Duration(0) - ret
    return ret


def generateDatetimesFromInterval(
    start: str, end: str, timedelta_config: dict | None = None, interval_between_dates: bool = False
) -> list[datetime]:
    if timedelta_config is None:
        timedelta_config = {}
    start_dt = parse_datestring_to_tz_aware_datetime(start)
    if end == "today":
        end_dt = datetime.now(tz=timezone.utc)
    else:
        end_dt = parse_datestring_to_tz_aware_datetime(end)
    delta = timedelta(**timedelta_config)
    dates = []
    while start_dt <= end_dt:
        if interval_between_dates:
            dates.append([start_dt, start_dt + delta - timedelta(seconds=1)])
        else:
            dates.append(start_dt)
        start_dt += delta
    return dates


class RaisingThread(threading.Thread):
    def run(self):
        self._exc = None
        try:
            super().run()
        except Exception as e:
            self._exc = e

    def join(self, timeout=None):
        super().join(timeout=timeout)
        if self._exc:
            raise self._exc


def recursive_save(stac_object: Catalog, no_items: bool = False, geo_parquet: bool = False) -> None:
    for child in stac_object.get_children():
        recursive_save(child, no_items, geo_parquet)
    if not no_items:
        for item in stac_object.get_items():
            item.save_object()
    stac_object.save_object()


def iter_len_at_least(i, n: int) -> int:
    return sum(1 for _ in zip(range(n), i, strict=False)) == n


def generate_veda_cog_link(endpoint_config: dict, file_url: str | None) -> str:
    bidx = ""
    if endpoint_config.get("Bidx"):
        # Check if an array was provided
        if hasattr(endpoint_config["Bidx"], "__len__"):
            for band in endpoint_config["Bidx"]:
                bidx = bidx + f"&bidx={band}"
        else:
            bidx = "&bidx={}".format(endpoint_config["Bidx"])

    colormap = ""
    if endpoint_config.get("Colormap"):
        colormap = "&colormap={}".format(endpoint_config["Colormap"])
        # TODO: For now we assume a already urlparsed colormap definition
        # it could be nice to allow a json and better convert it on the fly
        # colormap = "&colormap=%s"%(urllib.parse.quote(str(endpoint_config["Colormap"])))

    Nodata = ""
    if endpoint_config.get("Nodata"):
        Nodata = "&nodata={}".format(endpoint_config["Nodata"])

    colormap_name = ""
    if endpoint_config.get("ColormapName"):
        colormap_name = "&colormap_name={}".format(endpoint_config["ColormapName"])

    rescale = ""
    if endpoint_config.get("Rescale"):
        rescale = "&rescale={},{}".format(
            endpoint_config["Rescale"][0], endpoint_config["Rescale"][1]
        )

    file_url = f"url={file_url}&" if file_url else ""
    target_url_base = endpoint_config["EndPoint"].replace("/stac/", "")
    target_url = (
        f"{target_url_base}/raster/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?"
        f"{file_url}resampling_method=nearest"
        f"{bidx}{colormap}{colormap_name}{rescale}{Nodata}"
    )
    return target_url


@dataclass
class Options:
    catalogspath: str
    collectionspath: str
    indicatorspath: str
    outputpath: str
    vd: bool
    ni: bool
    tn: bool
    gp: bool
    collections: list[str]


def add_single_item_if_collection_empty(endpoint_config: dict, collection: Collection) -> None:
    for link in collection.links:
        if link.rel in [RelType.CHILD, RelType.ITEM]:
            break
    else:
        item = Item(
            id=str(uuid.uuid4()),
            bbox=[-180.0, -90.0, 180.0, 90.0],
            properties={},
            geometry=None,
            datetime=datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytztimezone("UTC")),
            start_datetime=datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytztimezone("UTC")),
            end_datetime=datetime.now(tz=pytztimezone("UTC")),
            assets={"dummy_asset": Asset(href="")},
        )
        collection.add_item(item)
        if not endpoint_config.get("OverwriteBBox"):
            collection.update_extent_from_items()


def replace_with_env_variables(s: str) -> str:
    # Define the regex pattern to find text within curly brackets
    pattern = r"\{(\w+)\}"

    # Define the replacement function
    def replacer(match):
        # Extract the variable name from the match
        var_name = match.group(1)
        # Get the environment variable value, if it doesn't exist, keep the original placeholder
        return os.getenv(var_name, match.group(0))

    # Use re.sub with the replacement function
    return re.sub(pattern, replacer, s)


def retry(exceptions, tries=3, delay=2, backoff=1, logger=None):
    """
    Retry decorator for retrying exceptions.

    :param exceptions: Exception or tuple of exceptions to catch.
    :param tries: Number of attempts. Default is 3.
    :param delay: Initial delay between attempts in seconds. Default is 2.
    :param backoff: Multiplier applied to delay between attempts. Default is 1 (no backoff).
    :param logger: Logger to use. If None, print. Default is None.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    _tries -= 1
                    if _tries == 0:
                        raise
                    else:
                        msg = f"{e}, Try: {tries-_tries+1}/{tries}, retry in {_delay} seconds..."
                        if logger:
                            logger.warning(msg)
                        else:
                            print(msg)
                        time.sleep(_delay)
                        _delay *= backoff

        return wrapper

    return decorator


def filter_time_entries(time_entries: list[datetime], query: dict[str, str]) -> list[datetime]:
    datetime_query = [
        time_entries[0],
        time_entries[-1],
    ]
    if start := query.get("Start"):
        datetime_query[0] = parse_datestring_to_tz_aware_datetime(start)
    if end := query.get("End"):
        datetime_query[1] = parse_datestring_to_tz_aware_datetime(end)
    # filter times based on query Start/End
    time_entries = [dt for dt in time_entries if datetime_query[0] <= dt < datetime_query[1]]
    return time_entries


def parse_datestring_to_tz_aware_datetime(datestring: str) -> datetime:
    dt = parser.isoparse(datestring)
    dt = pytztimezone("UTC").localize(dt) if dt.tzinfo is None else dt
    return dt


def format_datetime_to_isostring_zulu(datetime_obj: datetime) -> str:
    # although "+00:00" is a valid ISO 8601 timezone designation for UTC,
    # we rather convert it to Zulu based string in order for various clients
    # to understand it better (WMS)
    return (datetime_obj.replace(microsecond=0).isoformat()).replace("+00:00", "Z")


def get_full_url(url: str, catalog_config) -> str:
    if url.startswith("http"):
        return url
    else:
        return f'{catalog_config["assets_endpoint"]}{url}'


def update_extents_from_collection_children(collection: Collection):
    # retrieve extents from children
    c_bboxes = [
        c_child.extent.spatial.bboxes[0]
        for c_child in collection.get_children()
        if isinstance(c_child, Collection)
    ]
    if len(c_bboxes) > 0:
        merged_bbox = merge_bboxes(c_bboxes)
    else:
        LOGGER.warn(
            "No bounding boxes found in children of collection, using default bbox",
        )
        merged_bbox = [-180.0, -90.0, 180.0, 90.0]

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
    individual_datetimes = list(filter(lambda x: x is not None, individual_datetimes))
    if individual_datetimes:
        time_extent = [min(individual_datetimes), max(individual_datetimes)]
        collection.extent.temporal = TemporalExtent([time_extent])


def extract_extent_from_geoparquet(table) -> tuple[TemporalExtent, SpatialExtent]:
    """
    Extract spatial and temporal extents from a GeoParquet file.
    Args:
        table (pyarrow.Table): The table containing the GeoParquet data.
    Returns:
        tuple: A tuple containing spatial and temporal extents.
    """
    # add extent information to the collection
    min_datetime = pc.min(table["datetime"]).as_py()
    max_datetime = pc.max(table["datetime"]).as_py()
    if not min_datetime:
        # cases when datetime was null
        # fallback to start_datetime
        min_datetime = pc.min(table["start_datetime"]).as_py()
        max_datetime = pc.max(table["start_datetime"]).as_py()
    # Making sure time extent is timezone aware
    if min_datetime and min_datetime.tzinfo is None:
        min_datetime = min_datetime.replace(tzinfo=timezone.utc)
    if max_datetime and max_datetime.tzinfo is None:
        max_datetime = max_datetime.replace(tzinfo=timezone.utc)
    temporal = TemporalExtent([min_datetime, max_datetime])
    geoms = [wkb.loads(g.as_py()) for g in table["geometry"] if g is not None]
    bbox = sgeom.MultiPolygon(geoms).bounds
    spatial = SpatialExtent([bbox])
    return [temporal, spatial]


def save_items(
    collection: Collection,
    items: list[Item],
    output_path: str,
    catalog_id: str,
    colpath: str,
    use_geoparquet: bool = False,
) -> None:
    """
    Save a list of items for a collection either as single geoparquet or
    by adding them to the collection in order to be saved by pystac as individual items.
    Args:
        collection (Collection): The collection to which the items will be added.
        items (list[Item]): The list of items to save.
        output_path (str): The path where the items will be saved.
        catalog_id (str): The ID of the catalog to which the collection belongs.
        colpath (str): The expected path where to save the files relative to the catalog root.
        use_geoparquet (bool): If True, save items as a single GeoParquet file.
            If False, add items to the collection and save them individually.
    """
    if len(items) == 0:
        LOGGER.info(
            "No items to save for collection, adding placeholder extents",
            collection_id=collection.id,
            item_count=len(items),
        )
        # we need to add some generic extent to the collection
        collection.extent.spatial = SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])
        collection.extent.temporal = TemporalExtent(
            [
                datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytztimezone("UTC")),
                datetime.now(tz=pytztimezone("UTC")),
            ]
        )
        return
    if use_geoparquet:
        LOGGER.info(
            "Saving items as GeoParquet file",
            collection_id=collection.id,
            item_count=len(items),
        )
        if colpath is None:
            colpath = f"{collection.id}/{collection.id}"
        buildcatpath = f"{output_path}/{catalog_id}"
        record_batch_reader = stacgp.arrow.parse_stac_items_to_arrow(items)
        table = record_batch_reader.read_all()
        output_path = f"{buildcatpath}/{colpath}"
        os.makedirs(output_path, exist_ok=True)
        stacgp.arrow.to_parquet(table, f"{output_path}/items.parquet")
        extents = extract_extent_from_geoparquet(table)
        collection.extent.temporal = extents[0]
        collection.extent.spatial = extents[1]
        # Make sure to also reference the geoparquet as asset
        collection.add_asset(
            "geoparquet",
            Asset(
                href="./items.parquet",
                media_type="application/vnd.apache.parquet",
                title="GeoParquet Items",
                roles=["collection-mirror"],
            ),
        )
    else:
        # go over items and add them to the collection
        LOGGER.info(
            "Adding items to collection to be saved individually",
            collection_id=collection.id,
            item_count=len(items),
        )
        for item in items:
            link = collection.add_item(item)
            # bubble up information we want to the link
            # it is possible for datetime to be null, if it is start and end datetime have to exist
            item_datetime = item.get_datetime()
            if item_datetime:
                link.extra_fields["datetime"] = format_datetime_to_isostring_zulu(item_datetime)
            else:
                link.extra_fields["start_datetime"] = format_datetime_to_isostring_zulu(
                    parse_datestring_to_tz_aware_datetime(item.properties["start_datetime"])
                )
                link.extra_fields["end_datetime"] = format_datetime_to_isostring_zulu(
                    parse_datestring_to_tz_aware_datetime(item.properties["end_datetime"])
                )

            # bubble up data assets based on role
            collected_assets = [
                asset.href
                for asset in item.assets.values()
                if asset.roles and ("data" in asset.roles or "default" in asset.roles)
            ]
            if collected_assets:
                link.extra_fields["assets"] = collected_assets
            # also bubble up item id and cog_href if available
            # TODO: not clear when the item id is needed in the link might be some legacy reference
            # link.extra_fields["item"] = item.id
            if item.assets.get("cog_default"):
                link.extra_fields["cog_href"] = item.assets["cog_default"].href
        collection.update_extent_from_items()


def read_config_file(path: str) -> dict:
    # If the given path exists directly, use it
    if os.path.exists(path):
        return _load_file(path)

    # Otherwise, try appending supported suffixes
    for suffix in [".json", ".yaml", ".yml", ".JSON", ".YAML", ".YML"]:
        candidate = path + suffix
        if os.path.exists(candidate):
            return _load_file(candidate)

    raise FileNotFoundError(
        f"No file found for '{path}' with or without supported suffixes (.json/.yaml/.yml)"
    )


def _load_file(filepath):
    with open(filepath) as file:
        content = file.read()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        try:
            return yaml.safe_load(content)
        except yaml.YAMLError as err:
            raise ValueError(f"Failed to parse '{filepath}' as JSON or YAML: {err}") from err


def merge_bboxes(bboxes: list[list[float]]) -> list[float]:
    """
    Merge  bounding boxes into one bounding box that contains them all.
    Returns:
        A list representing the merged bbox: [min_lon, min_lat, max_lon, max_lat]
    """
    if not bboxes:
        raise ValueError("No bounding boxes provided.")

    min_lon = min(b[0] for b in bboxes)
    min_lat = min(b[1] for b in bboxes)
    max_lon = max(b[2] for b in bboxes)
    max_lat = max(b[3] for b in bboxes)

    return [min_lon, min_lat, max_lon, max_lat]


def make_intervals(datetimes: list[datetime]) -> list[list[datetime]]:
    """
    Converts a list of datetimes into list of lists of datetimes in format of [start,end]
    where end is next element in original list minus 1 second
    """
    intervals = []
    n = len(datetimes)
    for i in range(n):
        start = datetimes[i]
        if i < n - 1:
            # end is next datetime minus one second
            end = datetimes[i + 1] - timedelta(seconds=1)
        else:
            prev_interval = timedelta(seconds=0)
            # last item: use previous interval length added to last start
            if n > 1:
                prev_interval = datetimes[-1] - datetimes[-2]
            end = start + prev_interval
        intervals.append([start, end])
    return intervals
