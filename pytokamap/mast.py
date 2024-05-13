import re
from multiprocessing import Process
import numpy as np
import xarray as xr


def get_uda_client():
    import pyuda
    client = pyuda.Client()
    client.set_property("get_meta", True)
    client.set_property("timeout", 10)
    return client


class MASTClient:
    def __init__(self) -> None:
        pass

    def get_signal(self, shot_num: int, name: str, format: str) -> xr.Dataset:
        client = get_uda_client()
        # Known PyUDA Bug: Old MAST signals names from IDA are truncated to 23 characters!
        # Must truncate name here or we will miss some signals
        if "IDA" in format:
            signal_name = name[:23]
        else:
            signal_name = name

        # # Pull the signal on another process first.
        # # Sometimes this segfaults, so first we need to check that we can pull it safely
        # # To do this we pull the signal on a another process and check the error code.
        # def _get_signal(signal_name, shot_num):
        #     client = get_uda_client()
        #     client.get(signal_name, shot_num)

        # p = Process(target=_get_signal, args=(signal_name, shot_num))
        # p.start()
        # p.join()
        # code = p.exitcode

        # if code < 0:
        #     raise RuntimeError(
        #         f"Failed to get data for {signal_name}/{shot_num}. Possible segfault with exitcode: {code}"
        #     )

        # Now we know it is safe to access the signal and we will not get a segfault
        signal = client.get(signal_name, shot_num)
        dataset = self._convert_signal_to_dataset(name, signal)
        dataset.attrs["shot_id"] = shot_num
        return dataset

    def get_image(self, shot_num: int, name: str) -> xr.Dataset:
        client = get_uda_client()
        image = client.get_images(name, shot_num)
        dataset = self._convert_image_to_dataset(image)
        dataset.attrs["shot_id"] = shot_num
        return dataset

    def _convert_signal_to_dataset(self, signal_name, signal):
        dim_names = normalize_dimension_names(signal)
        coords = {
            name: xr.DataArray(
                np.atleast_1d(dim.data), dims=[name], attrs=dict(units=dim.units)
            )
            for name, dim in zip(dim_names, signal.dims)
        }

        data = np.atleast_1d(signal.data)
        errors = np.atleast_1d(signal.errors)

        attrs = self._get_dataset_attributes(signal_name, signal)

        data_vars = dict(
            data=xr.DataArray(data, dims=dim_names),
            error=xr.DataArray(errors, dims=dim_names),
        )
        dataset = xr.Dataset(data_vars, coords=coords, attrs=attrs)
        return dataset

    def _convert_image_to_dataset(self, image):
        attrs = {
            name: getattr(image, name)
            for name in dir(image)
            if not name.startswith("_") and not callable(getattr(image, name))
        }

        attrs.pop("frame_times")
        attrs.pop("frames")

        attrs["CLASS"] = "IMAGE"
        attrs["IMAGE_VERSION"] = "1.2"

        time = np.atleast_1d(image.frame_times)
        coords = {"time": xr.DataArray(time, dims=["time"], attrs=dict(units="s"))}

        if image.is_color:
            frames = [np.dstack((frame.r, frame.g, frame.b)) for frame in image.frames]
            frames = np.stack(frames)
            dim_names = ["time", "height", "width", "channel"]

            attrs["IMAGE_SUBCLASS"] = "IMAGE_TRUECOLOR"
            attrs["INTERLACE_MODE"] = "INTERLACE_PIXEL"
        else:
            frames = [frame.k for frame in image.frames]
            frames = np.stack(frames)
            frames = np.atleast_3d(frames)
            dim_names = ["time", "height", "width"]

            attrs["IMAGE_SUBCLASS"] = "IMAGE_INDEXED"

        data = {"data": (dim_names, frames)}
        dataset = xr.Dataset(data, coords=coords, attrs=attrs)
        return dataset

    def _remove_exceptions(self, signal_name, signal):
        """Handles when signal attributes contain exception objects"""
        signal_attributes = dir(signal)
        for attribute in signal_attributes:
            try:
                getattr(signal, attribute)
            except UnicodeDecodeError as exception:
                print(f"{signal_name} {attribute}: {exception}")
                signal_attributes.remove(attribute)
        return signal_attributes

    def _get_signal_metadata_fields(self, signal, signal_name):
        """Retrieves the appropriate metadata field for a given signal"""
        return [
            attribute
            for attribute in self._remove_exceptions(signal_name, signal)
            if not attribute.startswith("_")
            and attribute not in ["data", "errors", "time", "meta", "dims"]
            and not callable(getattr(signal, attribute))
        ]

    def _get_dataset_attributes(self, signal_name: str, signal) -> dict:
        metadata = self._get_signal_metadata_fields(signal, signal_name)

        attrs = {}
        for field in metadata:
            try:
                attrs[field] = getattr(signal, field)
            except TypeError as exception:
                pass

        for key, attr in attrs.items():
            if isinstance(attr, np.generic):
                attrs[key] = attr.item()
            elif isinstance(attr, np.ndarray):
                attrs[key] = attr.tolist()
            elif isinstance(attr, tuple):
                attrs[key] = list(attr)
            elif attr is None:
                attrs[key] = "null"

        return attrs


def normalize_dimension_names(signal):
    """Make the dimension names sensible"""
    dims = [dim.label for dim in signal.dims]
    count = 0
    dim_names = []
    empty_names = ["", " ", "-"]

    for name in dims:
        # Create names for unlabelled dims
        if name in empty_names:
            name = f"dim_{count}"
            count += 1

        # Normalize weird names to standard names
        dim_names.append(name)

    dim_names = list(map(lambda x: x.lower(), dim_names))
    dim_names = [re.sub("[^a-zA-Z0-9_\n\.]", "", dim) for dim in dim_names]
    return dim_names
