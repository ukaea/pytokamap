import xarray as xr
from pathlib import Path
from pytokamap.writers import NetCDFWriter, ZarrWriter


def test_write_zarr(tmpdir, datasets):
    output_file = tmpdir / "30420.zarr"
    writer = ZarrWriter()
    writer.write(datasets, output_file)

    assert Path(output_file).exists()

    for key in datasets.keys():
        assert datasets[key].equals(xr.open_zarr(output_file, group=key))


def test_write_netcdf(tmpdir, datasets):
    output_file = tmpdir / "30420.nc"
    writer = NetCDFWriter()
    writer.write(datasets, output_file)

    assert Path(output_file).exists()

    for key in datasets.keys():
        assert datasets[key].equals(xr.open_dataset(output_file, group=key))
