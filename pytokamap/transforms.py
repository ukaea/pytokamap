from abc import abstractmethod
import typing as t
import dask
import xarray as xr
from pytokamap.mapper import MapType, Mapping
from pytokamap.writers import DatasetsWriter
from pytokamap.plugins import plugin_registry
from pytokamap.types import Transforms, Datasets, Source, Target


class Combiner:
    def __init__(self, parents) -> None:
        super().__init__()
        self.parents = parents

    @dask.delayed()
    def __call__(self, *args, **kwargs) -> xr.Dataset:
        results = [item(*args, **kwargs) for item in self.parents]
        (datasets,) = dask.compute(results)
        return xr.combine_nested(datasets, concat_dim="channel")


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
        return self.writer.write(datasets, target)


class DatasetTransformBuilder:

    def __init__(self, mapping: Mapping) -> None:
        self.mapping = mapping

    def build(
        self, transform_cls: t.Optional[t.Type[Transformer]] = None, *args, **kwargs
    ) -> Transformer:
        transforms = {}

        for key, node in self.mapping.nodes.items():
            if node.map_type == MapType.PLUGIN:
                transforms[key] = plugin_registry.create(
                    node.plugin, scale=node.scale, **node.args
                )
            elif node.map_type == MapType.CUSTOM:
                pass
            else:
                raise RuntimeError(f"Unknown map type {node.map_type}")

        for key, node in self.mapping.nodes.items():
            if node.map_type == MapType.CUSTOM:
                parents = [transforms[key] for key in node.args]
                transforms[key] = Combiner(parents)

        transform_cls = DatasetTransformer if transform_cls is None else transform_cls
        transformer = transform_cls(transforms, *args, **kwargs)
        return transformer
