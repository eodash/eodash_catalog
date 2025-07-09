import json
import os
import shutil
from datetime import datetime

import pytest
from dateutil import parser

from eodash_catalog.generate_indicators import process_catalog_file
from eodash_catalog.utils import (
    Options,
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
        gp=None,
        collections=[],
    )
    # cleanup output after tests finish
    shutil.rmtree(outputpath)


# --------- FIRST CATALOG FIXTURES ---------


@pytest.fixture()
def catalog_output_folder(process_catalog_fixture, test_options):
    # not-used fixture needs to be here to trigger catalog generation
    return os.path.join(test_options.outputpath, "testing-catalog-id")


@pytest.fixture
def catalog_location(test_options):
    file_path = os.path.join(test_options.catalogspath, "testing.yaml")
    return file_path


@pytest.fixture
def process_catalog_fixture(catalog_location, test_options):
    process_catalog_file(catalog_location, test_options)


@pytest.fixture()
def catalog_output_folder_json(process_catalog_fixture_json, test_options):
    # not-used fixture needs to be here to trigger catalog generation
    return os.path.join(test_options.outputpath, "testing-catalog-id-2")


# --------- SECOND CATALOG FIXTURES ---------


@pytest.fixture
def catalog_location_json(test_options):
    file_path = os.path.join(test_options.catalogspath, "testing-json.json")
    return file_path


@pytest.fixture
def process_catalog_fixture_json(catalog_location_json, test_options):
    process_catalog_file(catalog_location_json, test_options)


def test_catalog_file_exists(catalog_output_folder):
    # test if catalogs were created in target locations
    assert os.path.exists(catalog_output_folder)


def test_catalog_file_json_exists(catalog_output_folder_json):
    assert os.path.exists(catalog_output_folder_json)


def test_collection_test_tif_demo_1(catalog_output_folder_json):
    # test that following collections were created as we expect it
    collection_name = "test_tif_demo_1_json"
    root_collection_path = os.path.join(catalog_output_folder_json, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        # test that custom bbox is set
        assert [-45.24, 61.13, -35.15, 65.05] in collection_json["extent"]["spatial"]["bbox"]
    child_collection_path = os.path.join(root_collection_path, collection_name)
    child_child_collection_path = os.path.join(child_collection_path, collection_name)
    item_dir = os.path.join(child_child_collection_path, "1970")
    item_paths = os.listdir(item_dir)
    assert len(item_paths) == 1
    with open(os.path.join(item_dir, item_paths[0])) as fp:
        item_json = json.load(fp)
        assert item_json["collection"] == collection_name


def test_collection_no_wms_has_a_single_item(catalog_output_folder):
    # test that following collections were created as we expect it
    collection_name = "imperviousness_density_2018"
    start_date = "1970-01-01T00:00:00Z"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        # test that custom bbox is set
        assert [-180.0, -90.0, 180.0, 90.0] in collection_json["extent"]["spatial"]["bbox"]
        # test that time interval is 1970-today
        assert collection_json["extent"]["temporal"]["interval"][0][0] == start_date
        assert (
            datetime.today().date()
            == parser.parse(collection_json["extent"]["temporal"]["interval"][0][1]).date()
        )
    child_collection_path = os.path.join(root_collection_path, collection_name)
    child_child_collection_path = os.path.join(child_collection_path, collection_name)
    item_dir = os.path.join(child_child_collection_path, "1970")
    item_paths = os.listdir(item_dir)
    assert len(item_paths) == 1
    with open(os.path.join(item_dir, item_paths[0])) as fp:
        item_json = json.load(fp)
        assert item_json["properties"]["start_datetime"] == start_date
        assert item_json["collection"] == collection_name


def test_indicator_groups_collections(catalog_output_folder):
    collection_name = "test_indicator_grouping_collections"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        indicator_json = json.load(fp)
        # test that collection has two child links
        child_links = [link for link in indicator_json["links"] if link["rel"] == "child"]
        assert len(child_links) == 2
        # test that summaries are aggregating individual properties of collections
        assert len(indicator_json["summaries"]["themes"]) == 2
        # test that bbox aggregating works
        indicator_bbox = indicator_json["extent"]["spatial"]["bbox"]
        assert len(indicator_bbox) == 3
        assert [-45.24, 61.13, -35.15, 65.05] in indicator_bbox
        assert [-145.24, -61.13, -135.15, -65.05] in indicator_bbox


def test_indicator_map_projection_added(catalog_output_folder):
    collection_name = "test_indicator_grouping_collections"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        indicator_json = json.load(fp)
        # test that collection has map projection defined
        assert indicator_json["eodash:mapProjection"] == 3035


def test_baselayers_and_overlays_added(catalog_output_folder):
    collection_name = "imperviousness_density_2018"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        baselayer_links = [
            link
            for link in collection_json["links"]
            if link.get("roles") and "baselayer" in link["roles"]
        ]
        overlay_links = [
            link
            for link in collection_json["links"]
            if link.get("roles") and "overlay" in link["roles"]
        ]
        assert len(baselayer_links) == 1
        assert len(overlay_links) == 1
        # test that attribution gets passed to a link dict
        assert "attribution" in baselayer_links[0]


def test_geojson_dataset_handled(catalog_output_folder):
    collection_name = "crop_forecast_at"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    child_collection_path = os.path.join(root_collection_path, collection_name)
    child_child_collection_path = os.path.join(child_collection_path, collection_name)
    item_dir = os.path.join(child_child_collection_path, "2024")
    item_paths = os.listdir(item_dir)
    assert len(item_paths) == 1
    with open(os.path.join(child_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        geojson_links = [
            link
            for link in collection_json["links"]
            if (link.get("rel", "") == "item" and len(link.get("assets", [])) > 0)
        ]
        # geojson link with assets exists
        assert len(geojson_links) > 0
        # and has a correct value
        assert (
            geojson_links[0]["assets"][0]
            == "https://raw.githubusercontent.com/eodash/eodash_catalog/main/tests/test-data/regional_forecast.json"
        )
        # epsg code saved on collection
        assert collection_json["proj:epsg"] == 3035
    with open(os.path.join(item_dir, item_paths[0])) as fp:
        item_json = json.load(fp)
        # mimetype saved correctly
        assert item_json["assets"]["vector_data"]["type"] == "application/geo+json"
        assert item_json["collection"] == collection_name
        # epsg code is saved to item
        assert item_json["proj:epsg"] == 3035
        # epsg code is saved to assets
        assert item_json["assets"]["vector_data"]["proj:epsg"] == 3035


def test_cog_dataset_handled(catalog_output_folder):
    collection_name = "solar_energy"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    child_collection_path = os.path.join(root_collection_path, collection_name)
    child_child_collection_path = os.path.join(child_collection_path, collection_name)
    item_dir = os.path.join(child_child_collection_path, "2023")
    item_paths = os.listdir(item_dir)
    with open(os.path.join(item_dir, item_paths[0])) as fp:
        item_json = json.load(fp)
        assert item_json["assets"]["solar_power"]["type"] == "image/tiff"
        assert item_json["collection"] == collection_name


def test_baselayer_with_custom_projection_added(catalog_output_folder):
    collection_name = "test_indicator_grouping_collections"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        indicator_json = json.load(fp)
        baselayer_links = [
            link
            for link in indicator_json["links"]
            if link.get("roles") and "baselayer" in link["roles"]
        ]
        # test that manual BaseLayers definition
        # overwrites default_baselayers, so there is just 1
        assert len(baselayer_links) == 1
        # test that custom proj4 definition is added to link
        assert baselayer_links[0]["eodash:proj4_def"]["name"] == "ORTHO:680500"


def test_collection_locations_processing(catalog_output_folder_json):
    # test that locations is true on root and process was added
    collection_name = "test_locations_processing"
    root_collection_path = os.path.join(catalog_output_folder_json, collection_name)
    # perform checks on child locations
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        indicator_json: dict = json.load(fp)
        # test that locations on indicator level is set to true
        assert indicator_json["locations"] is True
        links = indicator_json["links"]
        # test that link for custom process exists
        assert any([link.get("type") == "application/json; profile=collection" for link in links])
    child_collection_path = os.path.join(root_collection_path, collection_name)
    with open(os.path.join(child_collection_path, "collection.json")) as fp:
        collection_json: dict = json.load(fp)
        # test that locations on collection level is set to true
        assert collection_json["locations"] is True
    location_folders = [
        name
        for name in os.listdir(child_collection_path)
        if os.path.isdir(os.path.join(child_collection_path, name))
    ]
    # we specify two locations in this test config
    assert len(location_folders) == 2
    location_dir = os.path.join(child_collection_path, "Balaton")
    with open(os.path.join(location_dir, "collection.json")) as fp:
        # check that child Location collection has a process defined
        child_json = json.load(fp)
        links = child_json["links"]
        assert any([link.get("endpoint") == "eoxhub_workspaces" for link in links])
        assert "locations" not in child_json
