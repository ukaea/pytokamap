from pathlib import Path
import xarray as xr
import pytest
import pytokamap

try:
    import pyuda

    uda_available = True
except ImportError:
    uda_available = False


def test_load_dataset(mapping_files, zarr_file):
    mapper = pytokamap.load_mapping(*mapping_files)
    datasets = mapper.load(zarr_file)

    assert "amc/plasma_current" in datasets

    result = datasets["amc/plasma_current"].compute()
    assert isinstance(result, xr.Dataset)
    assert result.data.shape == (100,)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_load_dataset_uda(uda_mapping_files):
    mapper = pytokamap.load_mapping(*uda_mapping_files)
    datasets = mapper.load(30420)

    assert "amc/plasma_current" in datasets

    result = datasets["amc/plasma_current"].compute()
    assert isinstance(result, xr.Dataset)
    assert result.data.shape == (30000,)


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_load_dataset_uda_image(uda_mapping_files):
    mapper = pytokamap.load_mapping(*uda_mapping_files)
    datasets = mapper.load(30397)

    assert "rba" in datasets

    result = datasets["rba"].compute()
    assert isinstance(result, xr.Dataset)
    assert result.data.shape == (3490, 96, 128)


def test_convert_zarr_to_netcdf(tmpdir, mapping_files, zarr_file):
    target = tmpdir / "30420.nc"
    mapper = pytokamap.load_mapping(*mapping_files)
    mapper.to_netcdf(zarr_file, target)

    assert Path(target).exists()


def test_convert_zarr_to_zarr(tmp_path, mapping_files, zarr_file):
    target = tmp_path
    mapper = pytokamap.load_mapping(*mapping_files)
    mapper.to_zarr(zarr_file, target)

    assert Path(target).exists()


@pytest.mark.skipif(not uda_available, reason="UDA is not available")
def test_convert_uda_to_zarr(tmp_path, uda_mapping_files):
    target = tmp_path
    mapper = pytokamap.load_mapping(*uda_mapping_files)
    mapper.to_zarr(30420, target)

    assert Path(target).exists()


def test_convert_zarr_to_zarr_delay_compute(tmp_path, mapping_files, zarr_file):
    target = tmp_path
    mapper = pytokamap.load_mapping(*mapping_files)
    result = mapper.to_zarr(zarr_file, target, compute=False)

    result.compute()

    assert Path(target).exists()
