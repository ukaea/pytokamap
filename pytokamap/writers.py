import zarr
import dask.delayed
import xarray as xr
import typing as t
import dask
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

    @dask.delayed()
    def write(self, datasets: Datasets, target: Target):
        for group, dataset in datasets.items():
            dataset.to_netcdf(target, group=group, mode="a")


class ZarrWriter(DatasetsWriter):

    def write(self, datasets: Datasets, target: Target):
        results = []
        for group, dataset in datasets.items():
            self._do_write(dataset, target, group)
            result = dataset.to_zarr(target, group=group)
            results.append(result)

        @dask.delayed
        def consolidate(results, target):
            dask.compute(results)
            zarr.consolidate_metadata(target)

        result = consolidate(results, target)
        return result

    @dask.delayed()
    def _do_write(self, dataset: xr.Dataset, target: Target, group: str):
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
