import os
import shutil
from unittest.mock import MagicMock, patch

import pytest
from pystac import Catalog

from eodash_catalog.endpoints import handle_tipg_endpoint, handle_vector_tile_source
from eodash_catalog.utils import Options


@pytest.fixture
def tipg_options():
    outputpath = "build_tipg"
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
    if os.path.exists(outputpath):
        shutil.rmtree(outputpath)


def test_handle_tipg_endpoint(tipg_options):
    catalog_config = {
        "id": "test_catalog",
        "assets_endpoint": "https://assets.example.com",
    }
    endpoint_config = {
        "Name": "tipg",
        "EndPoint": "https://eoapi.example.com/vector/",
        "CollectionId": "public.e10a1_geometries_for_date",
        "TimesEndpoint": "https://eoapi.example.com/vector/collections/public.e10a1_timeseries/items?properties=time&limit=10000&f=json",
        "LocationsEndpoint": "https://eoapi.example.com/vector/collections/public.e10a1_geometries/items?f=json",
        "TimeParameter": "time",
        "Aggregation": "day",
        "Style": "https://assets.example.com/styles/e10a1.json",
        "Bbox": [11.2, 51.3, 14.8, 53.6],
    }
    collection_config = {
        "Name": "e10a1_test",
        "Title": "E10a1 Productive Area",
        "Description": "Test E10a1 description",
    }

    mock_times = [
        {"time": "2020-03-31T00:00:00Z"},
        {"time": "2020-03-31T12:00:00Z"},  # same day, should aggregate
        {"time": "2020-04-18T00:00:00Z"},
    ]
    mock_locations = [
        {"aoi_id": "DE11", "aoi": "52.13,13.09", "city": "Brandenburg", "country": "DE"},
        {"aoi_id": "ES8", "aoi": "37.18,-6.85", "city": "Huelva", "country": "ES"},
    ]

    def mock_get(url, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        if "timeseries" in url:
            resp.json.return_value = mock_times
        elif "geometries" in url:
            resp.json.return_value = mock_locations
        else:
            resp.json.return_value = []
        return resp

    catalog = Catalog(id="test_root", description="root")

    with patch("requests.get", side_effect=mock_get):
        collection = handle_tipg_endpoint(
            catalog_config,
            endpoint_config,
            collection_config,
            coll_path_rel_to_root_catalog="test_catalog",
            catalog=catalog,
            options=tipg_options,
        )

    assert collection.id == "e10a1_test"
    assert collection.title == "E10a1 Productive Area"
    assert collection.extra_fields.get("locations") is True

    # Check child location links
    child_links = [link for link in collection.links if link.rel == "child"]
    assert len(child_links) == 2
    ids = [link.extra_fields.get("id") for link in child_links]
    assert "DE11" in ids
    assert "ES8" in ids

    # Check summaries
    assert "Brandenburg" in collection.summaries.lists["cities"]
    assert "DE" in collection.summaries.lists["countries"]

    # Check generated items (2 unique aggregated days)
    items = list(collection.get_items())
    assert len(items) == 2
    item_dates = [it.datetime.strftime("%Y-%m-%d") for it in items]
    assert "2020-03-31" in item_dates
    assert "2020-04-18" in item_dates

    # Check vector-tile and style links on item
    first_item = items[0]
    vt_links = [link for link in first_item.links if link.rel == "vector-tile"]
    assert len(vt_links) == 1
    expected_substr = (
        "public.e10a1_geometries_for_date/tiles/WebMercatorQuad/{z}/{x}/{y}?target_date="
    )
    assert expected_substr in vt_links[0].target
    assert vt_links[0].media_type == "application/vnd.mapbox-vector-tile"

    style_links = [link for link in first_item.links if link.rel == "style"]
    assert len(style_links) == 1
    assert style_links[0].target == "https://assets.example.com/styles/e10a1.json"


def test_handle_vector_tile_source_tipg_delegation(tipg_options):
    catalog_config = {
        "id": "test_catalog",
        "assets_endpoint": "https://assets.example.com",
    }
    endpoint_config = {
        "Name": "VectorTile source",
        "EndPoint": "https://eoapi.example.com/vector/",
        "CollectionId": "public.modis_snpp_2023_for_date",
        "TimesEndpoint": "https://eoapi.example.com/vector/collections/public.modis_snpp_2023/items?properties=date_time&limit=10000&f=json&geom-column=",
        "TimeParameter": "date_time",
        "Aggregation": "day",
        "Bbox": [-180, -85, 180, 85],
    }
    collection_config = {
        "Name": "modis_test",
        "Title": "MODIS Active Fires",
        "Description": "Test MODIS description",
    }

    mock_times = [
        {"date_time": "2023-01-02T09:11:00Z"},
        {"date_time": "2023-01-03T10:15:00Z"},
    ]

    def mock_get(url, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = mock_times
        return resp

    catalog = Catalog(id="test_root", description="root")

    with patch("requests.get", side_effect=mock_get):
        collection = handle_vector_tile_source(
            catalog_config,
            endpoint_config,
            collection_config,
            coll_path_rel_to_root_catalog="test_catalog",
            catalog=catalog,
            options=tipg_options,
        )

    assert collection.id == "modis_test"
    items = list(collection.get_items())
    assert len(items) == 2
    vt_links = [link for link in items[0].links if link.rel == "vector-tile"]
    assert len(vt_links) == 1
    assert "public.modis_snpp_2023_for_date" in vt_links[0].target
