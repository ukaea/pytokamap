from pytokamap.transforms import DatasetTransformBuilder, FileTransformer
from pytokamap.writers import WriterNames, writer_registry
from pytokamap.mapper import Mapping, MappingReader
from pytokamap.types import Datasets, Source, Target


class MapperAPI:

    def __init__(self, mapping: Mapping) -> None:
        self.mapping = mapping

    @classmethod
    def create(cls, template, global_data):
        reader = MappingReader()
        mapping = reader.read(template, global_data)
        return MapperAPI(mapping)

    def load(self, source: Source) -> Datasets:
        builder = DatasetTransformBuilder(self.mapping)
        transformer = builder.build()
        return transformer.transform(source)

    def convert(self, source: Source, target: Target, target_format: str):
        builder = DatasetTransformBuilder(self.mapping)
        target_format = writer_registry.create(target_format)
        transformer = builder.build(FileTransformer, target_format)
        transformer.transform(source, target)

    def to_netcdf(self, source: Source, target: Target):
        self.convert(source, target, WriterNames.NETCDF)

    def to_zarr(self, source: Source, target: Target):
        self.convert(source, target, WriterNames.ZARR)


def create_mapping(template, global_data):
    return MapperAPI.create(template, global_data)
