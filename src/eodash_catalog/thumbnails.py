import os
import re
from pathlib import Path
from urllib.parse import urlparse

import requests
from pystac import (
    Item,
)

from eodash_catalog.utils import format_datetime_to_isostring_zulu, generate_veda_cog_link


def fetch_and_save_thumbnail(collection_config: dict, url: str) -> None:
    collection_path = "../thumbnails/{}_{}/".format(
        collection_config["EodashIdentifier"], collection_config["Name"]
    )
    Path(collection_path).mkdir(parents=True, exist_ok=True)
    image_path = f"{collection_path}/thumbnail.png"
    if not os.path.exists(image_path):
        dd = requests.get(url).content
        with open(image_path, "wb") as f:
            f.write(dd)


def generate_thumbnail(
    stac_object: Item,
    collection_config: dict,
    endpoint_config: dict,
    file_url: str = "",
    time: str | None = None,
) -> None:
    if endpoint_config["Name"] == "Sentinel Hub" or endpoint_config["Name"] == "WMS":
        instanceId = os.getenv("SH_INSTANCE_ID")
        if endpoint_config.get("InstanceId"):
            instanceId = endpoint_config["InstanceId"]
        # Build example url
        wms_config = (
            "REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&FORMAT=image/png&STYLES=&TRANSPARENT=true"
        )
        bbox = [-180, -85, 180, 85]
        if bbox_s := stac_object.bbox:
            bbox = f"{bbox_s[1]},{bbox_s[0]},{bbox_s[3]},{bbox_s[2]}"  # type: ignore
        output_format = f"format=image/png&WIDTH=256&HEIGHT=128&CRS=EPSG:4326&BBOX={bbox}"
        item_datetime = stac_object.get_datetime()
        # it is possible for datetime to be null,
        # if it is start and end datetime have to exist
        if item_datetime:
            time = format_datetime_to_isostring_zulu(item_datetime)
        endpoint_url_parts = urlparse(endpoint_config["EndPoint"])
        url = "https://{}/ogc/wms/{}?{}&layers={}&time={}&{}".format(
            endpoint_url_parts,
            instanceId,
            wms_config,
            endpoint_config["LayerId"],
            time,
            output_format,
        )
        fetch_and_save_thumbnail(collection_config, url)
    elif endpoint_config["Name"] == "VEDA":
        target_url = generate_veda_cog_link(endpoint_config, file_url)
        # set to get 0/0/0 tile
        url = re.sub(r"\{.\}", "0", target_url)
        fetch_and_save_thumbnail(collection_config, url)
