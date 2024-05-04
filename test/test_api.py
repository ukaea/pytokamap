from pathlib import Path
import pytokamap


def test_load_dataset(mapping_files, zarr_file):
    mapper = pytokamap.create_mapping(*mapping_files)
    mapper.load(zarr_file)


def test_convert_zarr_to_netcdf(tmpdir, mapping_files, zarr_file):
    target = tmpdir / "30420.nc"
    mapper = pytokamap.create_mapping(*mapping_files)
    result = mapper.to_netcdf(zarr_file, target)
    result.compute()

    assert Path(target).exists()
