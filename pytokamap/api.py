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
        mapping = reader.build(template, global_data)
        return MapperAPI(mapping)

    def load(self, source: Source, compute: bool = False) -> Datasets:
        builder = DatasetTransformBuilder(self.mapping)
        transformer = builder.build()
        result = transformer.transform(source)
        return result.compute() if compute else result

    def convert(
        self, source: Source, target: Target, target_format: str, compute: bool = True
    ):
        builder = DatasetTransformBuilder(self.mapping)
        target_format = writer_registry.create(target_format)
        transformer = builder.build(FileTransformer, target_format)
        result = transformer.transform(source, target)
        return result.compute() if compute else result

    def to_netcdf(self, source: Source, target: Target, compute: bool = True):
        return self.convert(source, target, WriterNames.NETCDF, compute)

    def to_zarr(self, source: Source, target: Target, compute: bool = True):
        return self.convert(source, target, WriterNames.ZARR, compute)


def load_mapping(template, global_data):
    return MapperAPI.create(template, global_data)
