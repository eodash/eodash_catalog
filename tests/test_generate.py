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
        collections=[],
    )
    # cleanup output after tests finish
    shutil.rmtree(outputpath)


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


def test_catalog_file_exists(catalog_output_folder):
    # test if catalog was created in target location
    assert os.path.exists(catalog_output_folder)


def test_collection_no_wms_has_a_single_item(catalog_output_folder):
    # test that following collections were created as we expect it
    collection_name = "imperviousness_density_2018"
    start_date = "1970-01-01T00:00:00Z"
    root_collection_path = os.path.join(catalog_output_folder, collection_name)
    with open(os.path.join(root_collection_path, "collection.json")) as fp:
        collection_json = json.load(fp)
        # test that custom bbox is set
        assert [-180, -85, 180, 85] in collection_json["extent"]["spatial"]["bbox"]
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
