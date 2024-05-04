import xarray as xr
from pytokamap.plugins import LoadZarr


def test_load_zarr(zarr_file):
    loader = LoadZarr("amc/plasma_current")
    result = loader(zarr_file)
    result = result.compute()
    assert isinstance(result, xr.Dataset)
