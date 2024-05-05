# Pytokamap

Add python library for mapping between tokamak data structures.

## Examples


An example mapping template file

```jinja2
{
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

```

An example global parameters file
```json
{"TCAM": {"N": 3}}
```

An example using the mapping to convert from an input file (`input.zarr`) to and output file (`output.nc`)

```python
import pytokamap
mapper = pytokamap.load_mapping('mapping.j2', 'globals.json')
mapper.to_netcdf('input.zarr, ''output.nc')
```

