import typing as t
import xarray as xr
from enum import Enum
from pathlib import Path


class PluginNames(str, Enum):
    ZARR = "ZARR"


class Plugin:
    pass


class LoadZarr(Plugin):
    def __init__(self, signal: str) -> None:
        super().__init__()
        self.signal = signal

    def __call__(self, file_name: t.Union[str, Path]) -> xr.Dataset:
        dataset = xr.open_dataset(file_name, group=self.signal, engine="zarr")
        return dataset


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins = {}

    def register(self, name: str, cls: t.Type[Plugin]):
        self._plugins[name] = cls

    def create(self, name: str, *args, **kwargs) -> Plugin:
        return self._plugins[name](*args, **kwargs)


plugin_registry = PluginRegistry()
plugin_registry.register(PluginNames.ZARR, LoadZarr)
