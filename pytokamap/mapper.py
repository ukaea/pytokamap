import typing as t
import json
from jinja2 import Template
from dataclasses import dataclass
from pathlib import Path
from enum import Enum


def read_json(path: str) -> dict:
    with Path(path).resolve().open("r") as f:
        return json.load(f)


def write_json(path: str, data: dict) -> dict:
    with Path(path).resolve().open("w") as f:
        json.dump(data, f)


def read_template(path: str) -> Template:
    with Path(path).resolve().open("r") as f:
        return Template(f.read())


@dataclass(slots=True)
class Mapping:
    pass


class MapType(str, Enum):
    PLUGIN = "PLUGIN"
    CUSTOM = "CUSTOM"


@dataclass(slots=True)
class MapNode:
    map_type: MapType
    plugin: str
    args: dict[str, str]
    scale: t.Optional[float] = None


@dataclass(slots=True)
class Mapping:
    nodes: dict[str, MapNode]


class MappingReader:

    def read(
        self,
        template: t.Union[Path, str],
        globals_data: t.Union[str, Path, dict],
    ):

        template = self._read_template(template)
        globals_data = self._read_globals(globals_data)
        mapping_data = self._expand_template(globals_data, template)
        nodes = self._load_mapping(mapping_data)
        return Mapping(nodes=nodes)

    def _read_globals(self, globals_data: t.Union[str, Path, dict]):
        if not isinstance(globals_data, dict):
            globals_data = read_json(globals_data)
        return globals_data

    def _read_template(self, template: t.Union[str, Path, Template]) -> Template:
        if isinstance(template, Template):
            return template
        elif Path(template).resolve().exists():
            return read_template(template)
        else:
            return Template(template)

    def _expand_template(self, globals_data: dict, template: Template) -> dict:
        rendered_template = template.render(globals_data)
        return json.loads(rendered_template.strip())

    def _load_mapping(self, data: dict) -> Mapping:
        nodes = {}
        for key, item in data.items():
            item = self._lower_keys(item)
            node = MapNode(**item)
            nodes[key] = node
        return nodes

    def _lower_keys(self, item: dict) -> dict:
        old_keys = list(item.keys())
        new_keys = [key.lower() for key in old_keys]

        for old, new in zip(old_keys, new_keys):
            item[new] = item[old]

        for old in old_keys:
            item.pop(old)

        return item
