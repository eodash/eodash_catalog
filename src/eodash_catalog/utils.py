import os
import re
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from functools import reduce, wraps
from typing import Any

from dateutil import parser
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
from pystac import Catalog, Collection, Item, RelType
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


def create_geojson_from_bbox(bbox: list[float | int]) -> dict:
    coordinates = [
        [bbox[0], bbox[1]],
        [bbox[2], bbox[1]],
        [bbox[2], bbox[3]],
        [bbox[0], bbox[3]],
        [bbox[0], bbox[1]],
    ]
    polygon = {"type": "Polygon", "coordinates": [coordinates]}

    feature = {"type": "Feature", "geometry": polygon, "properties": {}}
    feature_collection = {"type": "FeatureCollection", "features": [feature]}
    return feature_collection


def retrieveExtentFromWMSWMTS(
    capabilities_url: str, layer: str, version: str = "1.1.1", wmts: bool = False
) -> tuple[list[float], list[str]]:
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

    bbox = [-180.0, -90.0, 180.0, 90.0]
    if service and service[layer].boundingBoxWGS84:
        bbox = [float(x) for x in service[layer].boundingBoxWGS84]
    return bbox, times


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


def generateDateIsostringsFromInterval(
    start: str, end: str, timedelta_config: dict | None = None
) -> list[str]:
    if timedelta_config is None:
        timedelta_config = {}
    start_dt = datetime.fromisoformat(start)
    if end == "today":
        end = datetime.now().isoformat()
    end_dt = datetime.fromisoformat(end)
    delta = timedelta(**timedelta_config)
    dates = []
    while start_dt <= end_dt:
        dates.append(start_dt.isoformat())
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


def recursive_save(stac_object: Catalog, no_items: bool = False) -> None:
    stac_object.save_object()
    for child in stac_object.get_children():
        recursive_save(child, no_items)
    if not no_items:
        # try to save items if available
        for item in stac_object.get_items():
            item.save_object()


def iter_len_at_least(i, n: int) -> int:
    return sum(1 for _ in zip(range(n), i, strict=False)) == n


def generate_veda_cog_link(endpoint_config: dict, file_url: str | None) -> str:
    bidx = ""
    if "Bidx" in endpoint_config:
        # Check if an array was provided
        if hasattr(endpoint_config["Bidx"], "__len__"):
            for band in endpoint_config["Bidx"]:
                bidx = bidx + f"&bidx={band}"
        else:
            bidx = "&bidx={}".format(endpoint_config["Bidx"])

    colormap = ""
    if "Colormap" in endpoint_config:
        colormap = "&colormap={}".format(endpoint_config["Colormap"])
        # TODO: For now we assume a already urlparsed colormap definition
        # it could be nice to allow a json and better convert it on the fly
        # colormap = "&colormap=%s"%(urllib.parse.quote(str(endpoint_config["Colormap"])))

    colormap_name = ""
    if "ColormapName" in endpoint_config:
        colormap_name = "&colormap_name={}".format(endpoint_config["ColormapName"])

    rescale = ""
    if "Rescale" in endpoint_config:
        rescale = "&rescale={},{}".format(
            endpoint_config["Rescale"][0], endpoint_config["Rescale"][1]
        )

    file_url = f"url={file_url}&" if file_url else ""

    target_url = f"https://openveda.cloud/api/raster/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?{file_url}resampling_method=nearest{bidx}{colormap}{colormap_name}{rescale}"
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
    collections: list[str]


def add_single_item_if_collection_empty(collection: Collection) -> None:
    for link in collection.links:
        if link.rel in [RelType.CHILD, RelType.ITEM]:
            break
    else:
        item = Item(
            id=str(uuid.uuid4()),
            bbox=[-180, -85, 180, 85],
            properties={},
            geometry=None,
            datetime=datetime(1970, 1, 1, 0, 0, 0),
            start_datetime=datetime(1970, 1, 1, 0, 0, 0),
            end_datetime=datetime.now(),
        )
        collection.add_item(item)


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
