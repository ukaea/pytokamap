import typing as t
from enum import Enum
from pathlib import Path
from pytokamap.mast import MASTClient

import dask
import xarray as xr


class PluginNames(str, Enum):
    ZARR = "ZARR"
    UDA = "UDA"


class UDAType(str, Enum):
    IMAGE = "image"
    SIGNAL = "signal"


class UDAFormat(str, Enum):
    IDA = "IDA"
    NETCDF = "NETCDF"


class Plugin:
    pass


class LoadZarr(Plugin):
    def __init__(self, signal: str) -> None:
        super().__init__()
        self.signal = signal

    @dask.delayed()
    def __call__(self, file_name: t.Union[str, Path]) -> xr.Dataset:
        dataset = xr.open_dataset(file_name, group=self.signal, engine="zarr")
        return dataset


class LoadUDA(Plugin):
    def __init__(self, signal: str, format: UDAFormat, type: UDAType = UDAType.SIGNAL):
        super().__init__()
        self.signal = signal
        self.client = MASTClient()
        self.type = type
        self.format = format

    @dask.delayed()
    def __call__(self, shot: int) -> xr.Dataset:
        if self.type == UDAType.SIGNAL:
            return self.client.get_signal(shot, self.signal, self.format)
        else:
            return self.client.get_image(shot, self.signal)


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins = {}

    def register(self, name: str, cls: t.Type[Plugin]):
        self._plugins[name] = cls

    def create(self, name: str, *args, **kwargs) -> Plugin:
        return self._plugins[name](*args, **kwargs)


plugin_registry = PluginRegistry()
plugin_registry.register(PluginNames.ZARR, LoadZarr)
plugin_registry.register(PluginNames.UDA, LoadUDA)
