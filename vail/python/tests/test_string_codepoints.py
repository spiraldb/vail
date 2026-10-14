"""Vail intentionally indexes UTF-8 strings by Unicode scalar values."""
import pytest
import vail as hl


@pytest.mark.parametrize('text', ['', 'abc', 'é中', 'a😀b', 'e\u0301', '👩\u200d💻', '\x00😀'])
def test_dynamic_strings_match_python_codepoint_positions(text):
    table = hl.Table.parallelize([{'s': text}])
    result = table.annotate(n=hl.len(table.s), reverse=table.s.reverse(),
                            first=table.s[:1], last=table.s[-1:], middle=table.s[1:-1],
                            backwards=table.s[::-1], alternate=table.s[::2]).collect()[0]
    assert (result.n, result.reverse, result.first, result.last, result.middle,
            result.backwards, result.alternate) == (
        len(text), text[::-1], text[:1], text[-1:], text[1:-1], text[::-1], text[::2])
    for i in range(-len(text), len(text)):
        indexed = table.annotate(i=i)
        assert indexed.aggregate(hl.agg.collect(indexed.s[indexed.i])) == [text[i]]


def test_dynamic_index_bounds_missing_and_lazy_branches():
    table = hl.Table.parallelize([{'s': 'a😀b', 'i': 1}, {'s': 'é中', 'i': -1}])
    assert table.annotate(c=table.s[table.i]).c.collect() == ['😀', '中']
    for i in (-4, 3):
        with pytest.raises(RuntimeError, match='string index out of bounds'):
            hl.eval(hl.literal('a😀b')[i])
    invalid = hl.Table.parallelize([{'s': '😀', 'i': 1}])
    with pytest.raises(RuntimeError, match='string index out of bounds'):
        invalid.annotate(c=invalid.s[invalid.i]).collect()
    assert hl.eval(hl.missing(hl.tstr)[0]) is None
    assert hl.eval(hl.literal('😀')[hl.missing(hl.tint32)]) is None
    assert hl.eval(hl.if_else(False, hl.literal('')[0], 'safe')) == 'safe'
    with pytest.raises(RuntimeError, match='step|zero'):
        hl.eval(hl.literal('abc')[::0])


def test_dynamic_bounds_and_bounded_find():
    table = hl.Table.parallelize([{'s': '😀ab😀ab', 'start': 2, 'stop': 6}])
    assert table.annotate(part=table.s[table.start:table.stop],
                          found=table.s.find('ab', table.start, table.stop)).collect()[0].part == 'b😀ab'
    assert table.aggregate(hl.agg.collect(table.s.find('ab', table.start, table.stop))) == [4]
    assert hl.eval(hl.literal('😀ab').find('😀', 1)) == -1
    assert hl.eval(hl.literal('😀ab').find('ab', 0, 2)) == -1
    assert hl.eval(hl.literal('😀ab').find('', 2, 1)) == -1
    assert hl.eval(hl.literal('😀ab').find(hl.missing(hl.tstr), 2, 1)) is None


def test_dynamic_translation_and_reverse_complement():
    table = hl.Table.parallelize([{'s': '😀ab😀'}]).annotate_globals(
        mapping=hl.literal({'😀': '中é', 'a': ''}, hl.tdict(hl.tstr, hl.tstr)))
    assert table.annotate(t=table.s.translate(table.mapping)).t.collect() == ['中éb中é']
    assert hl.eval(hl.literal('a😀b').translate({'😀': '🚀', 'b': ''})) == 'a🚀'
    assert hl.eval(hl.literal('a😀').translate({'😀': 'XYZ'})) == 'aXYZ'
    dna = hl.Table.parallelize([{'s': 'AC😀Gt'}])
    assert dna.annotate(r=hl.reverse_complement(dna.s)).r.collect() == ['aC😀GT']
    invalid = table.annotate_globals(mapping=hl.literal({'ab': 'x'}, hl.tdict(hl.tstr, hl.tstr)))
    with pytest.raises(RuntimeError, match='one Unicode codepoint'):
        invalid.annotate(t=invalid.s.translate(invalid.mapping)).collect()


def test_regex_patterns_replacements_and_limits_can_be_expressions():
    table = hl.Table.parallelize([{'s': '😀12 中34', 'pattern': r'(\d+)', 'replacement': '[$1]', 'n': 2}])
    result = table.annotate(match=table.s.matches(table.pattern),
                            groups=table.s.first_match_in(table.pattern),
                            replaced=table.s.replace(table.pattern, table.replacement),
                            parts=table.s.split(r'\s+', table.n)).collect()[0]
    assert result.match is True
    assert result.groups == ['12']
    assert result.replaced == '😀[12] 中[34]'
    assert result.parts == ['😀12', '中34']
    assert hl.eval(hl.literal('😀a').split('')) == ['😀', 'a', '']
    assert hl.eval(hl.literal('a,,').split(',', 0)) == ['a']
    assert hl.eval(hl.literal('a,b,c').split(',', 2)) == ['a', 'b,c']
    assert hl.eval(hl.literal('😀').first_match_in('😀')) == []
    assert hl.eval(hl.literal('😀').first_match_in('(x)')) is None
    assert hl.eval(hl.literal('😀').matches('😀|ab', full_match=True)) is True
    assert hl.eval(hl.literal('xxab').matches('😀|ab', full_match=True)) is False


def test_hamming_entropy_and_scalar_broadcast():
    assert hl.eval(hl.delimit([1, 2], ':')) == '1:2'
    assert hl.eval(hl.reversed('a😀b')) == 'b😀a'
    assert hl.eval(hl.reversed([1, 2, 3])) == [3, 2, 1]
    assert hl.eval(hl.hamming('a😀', 'a🚀')) == 1
    assert hl.eval(hl.hamming('😀', 'ab')) is None
    assert hl.eval(hl.entropy('😀😀ab')) == 1.5
    assert hl.eval(hl.entropy('')) == 0
    assert hl.eval(hl.entropy(hl.missing(hl.tstr))) is None
    table = hl.range_table(5)
    assert table.annotate(h=hl.hamming('😀', '🚀'), parts=hl.literal('😀a').split('')).collect() == [
        hl.Struct(idx=i, h=1, parts=['😀', 'a', '']) for i in range(5)]


def test_unicode_ordering_and_storage_use_codepoint_order(tmp_path):
    values = ['😀', '\ue000', 'é', '中', 'a', '']
    table = hl.Table.parallelize([{'s': s} for s in values]).key_by('s')
    assert table.s.collect() == sorted(values)
    assert hl.eval(hl.literal('\ue000') < '😀') is True
    table.write(tmp_path / 'unicode-keys')
    assert hl.read_table(tmp_path / 'unicode-keys').s.collect() == sorted(values)
