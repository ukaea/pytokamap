from pathlib import Path
import xarray as xr
import typing as t

Transforms = dict[str, t.Any]
Datasets = dict[str, xr.Dataset]
Source = t.Union[str, Path]
Target = t.Union[str, Path]
