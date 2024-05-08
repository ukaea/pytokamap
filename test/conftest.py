from pathlib import Path
import json
import numpy as np
import xarray as xr
import pytest


@pytest.fixture()
def datasets():
    datasets = {}
    datasets["amc/plasma_current"] = xr.Dataset(
        data_vars=dict(
            data=("time", np.random.random(100)), error=("time", np.random.random(100))
        )
    )

    for i in range(1, 4):
        datasets[f"xsx/tcam_{i}"] = xr.Dataset(
            data_vars=dict(
                data=("time", np.random.random(100)),
                error=("time", np.random.random(100)),
            )
        )

    return datasets


@pytest.fixture()
def zarr_file(tmpdir, datasets):
    file_name = tmpdir / "30420.zarr"

    for key, dataset in datasets.items():
        dataset.to_zarr(file_name, group=key)
    return file_name


@pytest.fixture()
def netcdf_file(tmpdir, datasets):
    file_name = tmpdir / "30420.nc"

    for key, dataset in datasets.items():
        dataset.to_netcdf(file_name, group=key)

    return file_name


@pytest.fixture()
def mapping_files(tmpdir):
    contents = """{
        {% macro comma(loop) %}
            {% if not loop.last %},{% endif %}
        {% endmacro %}

        "amc/plasma_current": {
            "MAP_TYPE": "PLUGIN",
            "PLUGIN": "ZARR",
            "ARGS": {"signal": "amc/plasma_current"},
            "SCALE": 1000
        },
        {% for index in range(1, TCAM.N+1) %}
        "_xsx/tcam_{{index}}": {
            "MAP_TYPE": "PLUGIN",
            "PLUGIN": "ZARR",
            "ARGS": {"signal": "xsx/tcam_{{ index }}"}
        },
        {% endfor %}

        "xsx/tcam": {
            "MAP_TYPE": "CUSTOM",
            "CUSTOM_TYPE": "COMBINE",
            "ARGS": [
                {% for index in range(1, TCAM.N+1) %}
                    "_xsx/tcam_{{index}}"{{ comma(loop) }}
                {% endfor %}
            ]
        }
    }
    """

    file_name = tmpdir / "template.j2"
    with Path(file_name).open("w") as f:
        f.write(contents)

    contents = {"TCAM": {"N": 3}}

    globals_file_name = tmpdir / "globals.json"
    with Path(globals_file_name).open("w") as f:
        json.dump(contents, f)

    return file_name, globals_file_name


@pytest.fixture()
def uda_mapping_files(tmpdir):
    contents = """{
        {% macro comma(loop) %}
            {% if not loop.last %},{% endif %}
        {% endmacro %}

        "amc/plasma_current": {
            "MAP_TYPE": "PLUGIN",
            "PLUGIN": "UDA",
            "ARGS": {"signal": "ip", "format": "IDA"},
            "SCALE": 1000
        },
        "rba": {
            "MAP_TYPE": "PLUGIN",
            "PLUGIN": "UDA",
            "ARGS": {"signal": "rba", "format": "IDA", "type": "image"}
        },
        {% for index in range(1, TCAM.N+1) %}
        "_xsx/tcam_{{index}}": {
            "MAP_TYPE": "PLUGIN",
            "PLUGIN": "UDA",
            "ARGS": {"signal": "XSX/TCAM/{{ index }}", "format": "IDA"}
        },
        {% endfor %}
        "xsx/tcam": {
            "MAP_TYPE": "CUSTOM",
            "CUSTOM_TYPE": "COMBINE",
            "ARGS": [
                {% for index in range(1, TCAM.N+1) %}
                    "_xsx/tcam_{{index}}"{{ comma(loop) }}
                {% endfor %}
            ]
        }
    }
    """

    file_name = tmpdir / "template.j2"
    with Path(file_name).open("w") as f:
        f.write(contents)

    contents = {"TCAM": {"N": 3}}

    globals_file_name = tmpdir / "globals.json"
    with Path(globals_file_name).open("w") as f:
        json.dump(contents, f)

    return file_name, globals_file_name
