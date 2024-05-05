from pytokamap.mapper import MappingReader


def test_load_mappings(mapping_files):
    mapping_file, globals_file = mapping_files
    reader = MappingReader()
    mapping = reader.build(mapping_file, globals_file)

    assert isinstance(mapping.nodes, dict)
    assert len(mapping.nodes) == 5
