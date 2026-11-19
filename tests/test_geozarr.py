from unittest.mock import Mock, patch

from pystac import Catalog, CatalogType

from eodash_catalog.generate_indicators import _process_single_resource
from eodash_catalog.utils import Options

MOCK_ZARR_JSON = {
    "attributes": {
        "proj:code": "EPSG:4326",
        "spatial:bbox": [-6.0, 30.0, 36.0, 46.0],
    },
    "consolidated_metadata": {
        "metadata": {
            "0/time": {
                "node_type": "array",
                "shape": [10],
                "dimension_names": ["time"],
                "attributes": {"units": "days since 2020-01-01"},
            },
            "0/depth": {
                "node_type": "array",
                "shape": [18],
                "dimension_names": ["depth"],
                "attributes": {"long_name": "Depth", "units": "m"},
            },
            "0/lat": {"node_type": "array", "shape": [378], "dimension_names": ["lat"], "attributes": {}},
            "0/lon": {"node_type": "array", "shape": [1012], "dimension_names": ["lon"], "attributes": {}},
            "0/to": {
                "node_type": "array",
                "shape": [10, 18, 378, 1012],
                "dimension_names": ["time", "depth", "lat", "lon"],
                "attributes": {"long_name": "temperature", "units": "degC"},
            },
            "0/so": {
                "node_type": "array",
                "shape": [10, 18, 378, 1012],
                "dimension_names": ["time", "depth", "lat", "lon"],
                "attributes": {"long_name": "salinity", "units": "psu"},
            },
        }
    },
}


def _mock_response(json_data, status_code=200):
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    return resp


def test_geozarr_source_autodiscovery_and_band_filter(tmp_path):
    catalog = Catalog(id="test", description="test", catalog_type=CatalogType.RELATIVE_PUBLISHED)
    catalog_config = {"id": "test", "assets_endpoint": "https://example.com/assets"}
    collection_config = {"Name": "ocean_bio", "Title": "Ocean Biophysics", "Description": "Test"}
    endpoint_config = {
        "Name": "GeoZarr source",
        "Style": "geozarr_style.json",
        "Bbox": [-6.0, 30.0, 36.0, 46.0],
        "Bands": ["so"],
        "TimeEntries": [
            {
                "Time": "2024-01-01T00:00:00Z",
                "Assets": [
                    {
                        "Identifier": "biophysics",
                        "File": "https://example.com/ocean-med-biophysics.zarr",
                        "Title": "Biophysics Cube",
                    }
                ],
            }
        ],
    }
    options = Options(
        catalogspath="",
        collectionspath="",
        indicatorspath="",
        outputpath=str(tmp_path),
        vd=False,
        ni=False,
        tn=False,
        gp=False,
        collections=[],
    )

    with patch("eodash_catalog.stac_handling.requests.get", return_value=_mock_response(MOCK_ZARR_JSON)):
        collection = _process_single_resource(
            catalog_config,
            endpoint_config,
            collection_config,
            "",
            catalog,
            options,
        )

    items = list(collection.get_items())
    assert len(items) == 1
    item = items[0]
    asset = item.assets["biophysics"]

    assert asset.media_type == "application/vnd.zarr"
    assert asset.extra_fields["bands"] == [{"name": "so", "description": "salinity", "unit": "psu"}]
    assert asset.extra_fields["proj:code"] == "EPSG:4326"
    assert "time" in asset.extra_fields["cube:dimensions"]
    assert asset.extra_fields["cube:dimensions"]["time"]["type"] == "temporal"
    assert asset.extra_fields["cube:dimensions"]["depth"]["type"] == "vertical"
    assert list(asset.extra_fields["cube:variables"].keys()) == ["so"]
    assert "https://stac-extensions.github.io/datacube/v2.2.0/schema.json" in item.stac_extensions
    assert "https://stac-extensions.github.io/datacube/v2.2.0/schema.json" in collection.stac_extensions

    style_links = [lnk for lnk in item.links if lnk.rel == "style"]
    assert len(style_links) == 1
    assert style_links[0].media_type == "text/zarr-styles"
    assert style_links[0].target == "https://example.com/assets/geozarr_style.json"
