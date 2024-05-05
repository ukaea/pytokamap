from pathlib import Path
import dask

from pytokamap.mapper import MappingReader
from pytokamap.transforms import (
    DatasetTransformBuilder,
    DatasetTransformer,
    FileTransformer,
)
from pytokamap.writers import NetCDFWriter


def test_dataset_transform_builder(mapping_files):
    reader = MappingReader()
    mapping = reader.build(*mapping_files)

    builder = DatasetTransformBuilder(mapping)
    transformer = builder.build()

    assert isinstance(transformer, DatasetTransformer)
    assert len(transformer.transforms) == 5


def test_transform_zarr(mapping_files, zarr_file):
    reader = MappingReader()
    mapping = reader.build(*mapping_files)

    builder = DatasetTransformBuilder(mapping)
    transformer = builder.build(DatasetTransformer)
    datasets = transformer.transform(zarr_file)
    (datasets,) = dask.compute(datasets)

    assert len(datasets)
    assert "amc/plasma_current" in datasets
    assert "_xsx/tcam_1" in datasets
    assert "_xsx/tcam_2" in datasets
    assert "_xsx/tcam_3" in datasets

    assert datasets["amc/plasma_current"]["data"].shape == (100,)


def test_file_transformer(tmpdir, mapping_files, zarr_file):
    netcdf_file = tmpdir / "30420.nc"

    reader = MappingReader()
    mapping = reader.build(*mapping_files)

    builder = DatasetTransformBuilder(mapping)
    transformer = builder.build(FileTransformer, writer=NetCDFWriter())
    result = transformer.transform(zarr_file, netcdf_file)
    result.compute()

    assert Path(netcdf_file).exists()
