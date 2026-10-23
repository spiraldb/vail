"""Public regressions at compatibility-branch boundaries."""
import pickle

import vail as hl


def test_inferred_frozendict_retains_dictionary_values_and_pickle_contract():
    frozen = hl.utils.frozendict({'a': 3, 'b': 7})
    expression = hl.literal(pickle.loads(pickle.dumps(frozen)))
    assert expression.dtype == hl.tdict(hl.tstr, hl.tint32)
    assert hl.eval(expression.values()) == [3, 7]
    assert hl.eval(expression) == frozen


def test_struct_rename_keeps_nested_dictionary_and_numeric_promotion():
    value = hl.struct(mapping={'a': [hl.int32(2)], 'b': [hl.float64(3)]}, missing=hl.missing(hl.tint32))
    renamed = value.rename({'mapping': 'renamed'})
    assert renamed.renamed.dtype == hl.tdict(hl.tstr, hl.tarray(hl.tfloat64))
    assert hl.eval(renamed) == hl.Struct(missing=None, renamed={'a': [2.0], 'b': [3.0]})
    assert hl.eval(hl.missing(value.dtype).rename({'mapping': 'renamed'})) is None
