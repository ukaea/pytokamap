from abc import abstractmethod
import typing as t
from pytokamap.mapper import MapType, Mapping
from pytokamap.writers import DatasetsWriter
from pytokamap.plugins import plugin_registry
from pytokamap.types import Transforms, Datasets, Source, Target


class Transformer:

    @abstractmethod
    def transform(self, *args, **kwargs) -> t.Any:
        raise NotImplementedError()


class DatasetTransformer(Transformer):
    def __init__(self, transforms: Transforms) -> None:
        self.transforms = transforms

    def transform(self, source: str) -> Datasets:
        results = {}
        for key, transformer in self.transforms.items():
            results[key] = transformer(source)
        return results


class FileTransformer(Transformer):
    def __init__(self, transforms: Transforms, writer: DatasetsWriter) -> None:
        self.transformer = DatasetTransformer(transforms)
        self.writer = writer

    def transform(self, source: Source, target: Target):
        datasets = self.transformer.transform(source)
        self.writer.write(datasets, target)


class DatasetTransformBuilder:

    def __init__(self, mapping: Mapping) -> None:
        self.mapping = mapping

    def build(
        self, transform_cls: t.Optional[t.Type[Transformer]] = None, *args, **kwargs
    ) -> Transformer:
        transforms = {}
        for key, node in self.mapping.nodes.items():
            if node.map_type == MapType.PLUGIN:
                transforms[key] = plugin_registry.create(node.plugin, **node.args)
            else:
                raise RuntimeError(f"Unknown map type {node.map_type}")

        transform_cls = DatasetTransformer if transform_cls is None else transform_cls
        transformer = transform_cls(transforms, *args, **kwargs)
        return transformer
