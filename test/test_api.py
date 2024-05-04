from pathlib import Path
import xarray as xr
import dask
import pytokamap


def test_load_dataset(mapping_files, zarr_file):
    mapper = pytokamap.create_mapping(*mapping_files)
    datasets = mapper.load(zarr_file)

    assert "amc/plasma_current" in datasets

    result = datasets["amc/plasma_current"].compute()
    assert isinstance(result, xr.Dataset)
    assert result.data.shape == (100,)


def test_convert_zarr_to_netcdf(tmpdir, mapping_files, zarr_file):
    target = tmpdir / "30420.nc"
    mapper = pytokamap.create_mapping(*mapping_files)
    mapper.to_netcdf(zarr_file, target)

    assert Path(target).exists()


def test_convert_zarr_to_zarr(tmp_path, mapping_files, zarr_file):
    target = tmp_path
    mapper = pytokamap.create_mapping(*mapping_files)
    mapper.to_zarr(zarr_file, target)

    assert Path(target).exists()


def test_convert_zarr_to_zarr_delay_compute(tmp_path, mapping_files, zarr_file):
    target = tmp_path
    mapper = pytokamap.create_mapping(*mapping_files)
    result = mapper.to_zarr(zarr_file, target, compute=False)

    result.compute()

    assert Path(target).exists()
