import pytest
import xarray as xr
from pytokamap.plugins import LoadZarr, LoadUDA

try:
    import pyuda

    uda_available = True
except ImportError:
    uda_available = False


def test_load_zarr(zarr_file):
    loader = LoadZarr("amc/plasma_current")
    result = loader(zarr_file)
    result = result.compute()
    assert isinstance(result, xr.Dataset)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_load_uda():
    loader = LoadUDA("ip", format="IDA")
    result = loader(30421)
    result = result.compute()
    assert isinstance(result, xr.Dataset)
