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
    IMAGE =  "Image"
    Analysed = "Analysed"
    Raw = "Raw"


class UDAFormat(str, Enum):
    IDA = "IDA"
    NETCDF = "NETCDF"


class Plugin:
    pass


class LoadZarr(Plugin):
    def __init__(self, signal: str, scale: float = 1):
        super().__init__()
        self.signal = signal
        self.scale = scale

    @dask.delayed()
    def __call__(self, file_name: t.Union[str, Path]) -> xr.Dataset:
        dataset = xr.open_dataset(file_name, group=self.signal, engine="zarr")
        dataset["data"] = dataset["data"] * self.scale
        return dataset


class LoadUDA(Plugin):
    def __init__(
        self,
        signal: str,
        format: UDAFormat,
        scale: float = 1,
        type: UDAType = UDAType.Raw,
    ):
        super().__init__()
        self.signal = signal
        self.type = type
        self.format = format
        self.scale = scale

    @dask.delayed()
    def __call__(self, shot: int) -> xr.Dataset:
        client = MASTClient()
        if self.type != UDAType.IMAGE:
            dataset = client.get_signal(shot, self.signal, self.format)
        else:
            dataset = client.get_image(shot, self.signal)
        dataset["data"] = dataset["data"] * self.scale
        return dataset


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins = {}

    def register(self, name: str, cls: t.Type[Plugin]):
        self._plugins[name] = cls

    def create(
        self, name: str, *args, scale: t.Optional[float] = None, **kwargs
    ) -> Plugin:
        return self._plugins[name](*args, scale=scale, **kwargs)


plugin_registry = PluginRegistry()
plugin_registry.register(PluginNames.ZARR, LoadZarr)
plugin_registry.register(PluginNames.UDA, LoadUDA)
