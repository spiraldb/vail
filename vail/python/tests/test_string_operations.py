import pytest
import vail as hl


@pytest.mark.parametrize('text, size', [('', 0), ('abc', 3), ('é中', 2), ('a😀b', 3), ('😀😀', 2), ('e\u0301', 2)])
def test_codepoint_length(text, size):
    value = hl.literal(text)
    assert hl.eval(value.length()) == hl.eval(hl.len(value)) == size
    assert hl.eval(hl.missing(hl.tstr).length()) is None


def test_concat_and_repetition_missing_semantics():
    value = hl.literal('ab')
    assert hl.eval(value + 'c') == 'abc'
    assert hl.eval('c' + value) == 'cab'
    assert hl.eval(value + hl.missing(hl.tstr)) is None
    assert hl.eval(hl.missing(hl.tstr) + value) is None
    assert hl.eval(value * 3) == 'ababab'
    assert hl.eval(2 * value) == 'abab'
    assert hl.eval(hl.int32(2) * value) == 'abab'
    assert hl.eval('ab' * hl.int32(2)) == 'abab'
    assert hl.eval(value * -1) == ''
    assert hl.eval(hl.missing(hl.tstr) * 0) == ''
    assert hl.eval(hl.missing(hl.tstr) * 1) is None
    assert hl.eval(value * hl.missing(hl.tint32)) is None


def test_case_strip_and_literal_reverse():
    assert hl.eval(hl.literal('ΟΣ İ').lower()) == 'ος i\u0307'
    assert hl.eval(hl.literal('Straße').upper()) == 'STRASSE'
    assert hl.eval(hl.literal('\x00\t\r\n abc \x1f').strip()) == 'abc'
    assert hl.eval(hl.literal('\u00a0abc\u00a0').strip()) == '\u00a0abc\u00a0'
    assert hl.eval(hl.literal('aé中').reverse()) == '中éa'
    assert hl.eval(hl.missing(hl.tstr).lower()) is None
    assert hl.eval(hl.missing(hl.tstr).reverse()) is None
    assert hl.eval(hl.missing(hl.tstr)[1:]) is None


@pytest.mark.parametrize('method, sub, expected', [('contains', 'bc', True), ('startswith', 'ab', True), ('endswith', 'cd', True), ('contains', 'z', False), ('contains', '', True)])
def test_string_search(method, sub, expected):
    value = hl.literal('abcd')
    assert hl.eval(getattr(value, method)(sub)) is expected
    assert hl.eval(getattr(value, method)(hl.missing(hl.tstr))) is None


def test_find_codepoint_positions():
    value = hl.literal('😀abc😀def')
    assert hl.eval(value.find('a')) == 1
    assert hl.eval(value.find('d')) == 5
    assert hl.eval(value.find('😀')) == 0
    assert hl.eval(value.find('no')) == -1
    assert hl.eval(value.find('')) == 0
    assert hl.eval(value.find(hl.missing(hl.tstr))) is None
    assert hl.eval(hl.missing(hl.tstr).find('a')) is None


def test_bmp_literal_index_and_slicing():
    value = hl.literal('abcdef')
    assert hl.eval(value[0]) == 'a'
    assert hl.eval(value[-1]) == 'f'
    for selection in (slice(None), slice(1, 4), slice(-3, -1), slice(20, 30), slice(-20, 2), slice(4, 1)):
        assert hl.eval(value[selection]) == 'abcdef'[selection]
    assert hl.eval(hl.literal('é中')[1:]) == '中'
    with pytest.raises(RuntimeError, match='string index out of bounds'):
        hl.eval(value[6])


def test_literal_regex_pattern_operations():
    value = hl.literal('abcabc')
    assert hl.eval(value.matches('abc')) is True
    assert hl.eval(value.matches('abc', full_match=True)) is False
    assert hl.eval(value.matches('abcabc', full_match=True)) is True
    assert hl.eval(value.replace('abc', 'X')) == 'XX'
    assert hl.eval(hl.literal('a,,b,').split(',')) == ['a', '', 'b', '']
    assert hl.eval(hl.literal('').split(',')) == ['']
    assert hl.eval(hl.missing(hl.tstr).split(',')) is None
    assert hl.eval(hl.missing(hl.tstr).matches('abc')) is None


def test_translate_join_delimit_and_reverse_complement():
    value = hl.literal('ATTTGCA😀')
    assert hl.eval(value.translate({'T': 'U'})) == 'AUUUGCA😀'
    assert hl.eval(value.translate(hl.literal({'T': 'U'}, hl.tdict(hl.tstr, hl.tstr)))) == 'AUUUGCA😀'
    assert hl.eval(hl.literal('abcd').translate({'b': '', 'c': 'X'})) == 'aXd'
    assert hl.eval(hl.literal(',').join(hl.literal(['a', None, 'b'], hl.tarray(hl.tstr)))) == 'a,null,b'
    assert hl.eval(hl.delimit(hl.array(['a', 'b']), ':')) == 'a:b'
    assert hl.eval(hl.literal(',').join(hl.literal([], hl.tarray(hl.tstr)))) == ''
    assert hl.eval(hl.missing(hl.tstr).join(hl.array(['a']))) is None
    assert hl.eval(hl.literal(',').join(hl.missing(hl.tarray(hl.tstr)))) is None
    assert hl.eval(hl.reverse_complement('NNGATTACA')) == 'TGTAATCNN'
    assert hl.eval(hl.reverse_complement('aUgT', rna=True)) == 'AcAu'


def test_string_operations_in_lazy_table_globals_lookup_and_storage(tmp_path):
    people = hl.Table.parallelize([{'id': 1, 'name': 'Alice'}], key='id')
    source = hl.Table.parallelize([{'id': 1, 'text': ' 😀a '}, {'id': 2, 'text': None}],
                                 schema=hl.tstruct(id=hl.tint32, text=hl.tstr), n_partitions=2).annotate_globals(suffix='!')
    lookup = people[source.id].name
    result = source.annotate(clean=source.text.strip(), size=source.text.length(),
                             greeting=lookup + source.suffix, pos=source.text.find('a'))
    assert result.n_partitions() == 2
    assert [(r.clean, r.size, r.greeting, r.pos) for r in result.collect()] == [('😀a', 4, 'Alice!', 2), (None, None, None, None)]
    result.write(str(tmp_path / 'strings'))
    assert hl.read_table(str(tmp_path / 'strings'))._same(result)
    assert source.aggregate(hl.agg.collect(source.text.length())) == [4, None]


def test_datafusion_semantic_gaps_raise_explicitly():
    value = hl.literal('abc')
    for action in (
        lambda: hl.parse_int32('١'), lambda: hl.parse_float64('0x1.8p1'),
        lambda: hl.format('%s', value), lambda: hl.json(value),
        lambda: hl.parse_json('{}', hl.tstruct()),
    ):
        with pytest.raises((NotImplementedError, TypeError)):
            action()
    with pytest.raises(TypeError, match='contains method'):
        'a' in value


def test_row_expression_repetition_and_literal_slice_bounds():
    table = hl.utils.range_table(3)
    result = table.annotate(repeat=table.idx * hl.literal('x'), piece=hl.literal('abcd')[table.idx:])
    assert [(row.repeat, row.piece) for row in result.collect()] == [('', 'abcd'), ('x', 'bcd'), ('xx', 'cd')]
