from pprint import pformat

import vail as hl


def test_struct_repr_preserves_identifier_fields():
    assert repr(hl.Struct(x=3, y=3)) == "Struct(x=3, y=3)"


def test_struct_repr_quotes_non_identifier_fields():
    assert repr(hl.Struct(**{"x": 3, "y ": 3})) == "Struct(**{'x': 3, 'y ': 3})"


def test_struct_repr_handles_nested_non_identifier_fields():
    value = hl.Struct(x=3, nested=hl.Struct(**{"a ": 5}))
    assert repr(value) == "Struct(x=3, nested=Struct(**{'a ': 5}))"


def test_pformat_array_of_structs_with_non_identifier_fields():
    expected = """[Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123}),
 Struct(**{'x': 3243, 'y ': 123})]"""
    values = [hl.Struct(**{"x": 3243, "y ": 123}) for _ in range(10)]
    assert pformat(values) == expected
