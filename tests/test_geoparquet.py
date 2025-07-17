import json
import os
import shutil

import pyarrow as pa
import pytest
import stac_geoparquet as stac_gp

from eodash_catalog.utils import (
    Options,
)

# keeping unused imports as text fixtures
# ruff: noqa: F401,F811
from tests.test_generate import (
    catalog_location,
    catalog_output_folder,
    process_catalog_fixture,
)


@pytest.fixture
def test_options():
    outputpath = "build"
    # yield instead of return to run code below yield after fixture released from all tests
    yield Options(
        catalogspath="testing-catalogs",
        collectionspath="testing-collections",
        indicatorspath="testing-indicators",
        outputpath=outputpath,
        vd=None,
        ni=None,
        tn=None,
        gp=True,
        collections=[],
    )
    # cleanup output after tests finish
    shutil.rmtree(outputpath)


def test_geoparquet_geojson_items(catalog_output_folder):
    collection_name = "crop_forecast_at"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    child_collection_path = os.path.join(root_collection_path, collection_name)

    with open(os.path.join(child_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        # check if parquet source is present in assets
        assert "geoparquet" in collection_json["assets"]
        parquet_asset = collection_json["assets"]["geoparquet"]
        assert parquet_asset["type"] == "application/vnd.apache.parquet"
        items_path = os.path.join(child_collection_path, parquet_asset["href"].split("/")[-1])
        assert os.path.exists(items_path)

    with open(items_path, "rb") as fp:
        table = pa.parquet.read_table(fp)
        items = list(stac_gp.arrow.stac_table_to_items(table))
        item = items[0]
        # mimetype saved correctly
        assert item["assets"]["vector_data"]["type"] == "application/geo+json"
        # epsg code is saved to item,
        # proj:epsg is moved to properties by stac-geoparquet
        assert item["properties"]["proj:epsg"] == 3035
        # epsg code is saved to assets
        assert item["assets"]["vector_data"]["proj:epsg"] == 3035


def test_cog_geoparquet_items(catalog_output_folder):
    collection_name = "solar_energy"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    child_collection_path = os.path.join(root_collection_path, collection_name)
    items_path = os.path.join(child_collection_path, "items.parquet")
    assert os.path.exists(items_path)
    with open(items_path, "rb") as fp:
        table = pa.parquet.read_table(fp)
        items = list(stac_gp.arrow.stac_table_to_items(table))
        item = items[0]
        # mimetype saved correctly
        assert item["assets"]["solar_power"]["type"] == "image/tiff"
