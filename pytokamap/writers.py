import typing as t
from abc import abstractmethod
from enum import Enum
from pytokamap.types import Datasets, Target


class WriterNames(str, Enum):
    ZARR = "zarr"
    NETCDF = "netcdf"


class DatasetsWriter:

    @abstractmethod
    def write(self, datasets: Datasets, target: Target):
        raise NotImplementedError()


class NetCDFWriter(DatasetsWriter):

    def write(self, datasets: Datasets, target: Target):
        for group, dataset in datasets.items():
            dataset.to_netcdf(target, group=group, mode="a")


class ZarrWriter(DatasetsWriter):

    def write(self, datasets: Datasets, target: Target):
        for group, dataset in datasets.items():
            dataset.to_zarr(target, group=group)


class WriterRegistry:
    def __init__(self) -> None:
        self._plugins = {}

    def register(self, name: str, cls: t.Type[DatasetsWriter]):
        self._plugins[name] = cls

    def create(self, name: str, *args, **kwargs) -> DatasetsWriter:
        return self._plugins[name](*args, **kwargs)


writer_registry = WriterRegistry()
writer_registry.register(WriterNames.ZARR, ZarrWriter)
writer_registry.register(WriterNames.NETCDF, NetCDFWriter)
