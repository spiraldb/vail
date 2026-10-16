"""Public variant splitting contracts, including invalid inputs at action time."""
import pytest
import vail as hl


def matrix(alleles=('A', 'C', 'G'), **entries):
    mt = hl.range_matrix_table(1, 1)
    mt = mt.annotate_rows(locus=hl.locus('1', 100), alleles=list(alleles))
    mt = mt.key_rows_by('locus', 'alleles')
    return mt.annotate_entries(**entries)


def entries(mt):
    return hl.split_multi_hts(mt).entries().collect()


def test_diploid_likelihoods_depth_and_quality():
    row = entries(matrix(GT=hl.call(1, 2), AD=[5, 7, 11], PL=[40, 30, 20, 10, 0, 50], GQ=99, DP=23))
    assert [r.alleles for r in row] == [['A', 'C'], ['A', 'G']]
    assert [r.GT.alleles for r in row] == [[0, 1], [0, 1]]
    assert [r.AD for r in row] == [[16, 7], [12, 11]]
    assert [r.PL for r in row] == [[10, 0, 20], [20, 0, 50]]
    assert [r.GQ for r in row] == [10, 20]
    assert all(r.DP == 23 and r.was_split for r in row)
    assert [r.a_index for r in row] == [1, 2]


def test_haploid_phased_and_missing_calls():
    rows = entries(matrix(GT=hl.call(2, phased=True), PGT=hl.call(2, 1, phased=True), PL=[7, 3, 9]))
    assert [r.GT.alleles for r in rows] == [[0], [1]]
    assert all(r.GT.phased and r.PGT.phased for r in rows)
    assert [r.PGT.alleles for r in rows] == [[0, 1], [1, 0]]
    assert [r.PL for r in rows] == [[7, 3], [3, 9]]
    rows = entries(matrix(GT=hl.missing(hl.tcall), PL=[40, 30, 20, 10, 0, 50], GQ=17))
    assert all(r.GT is None and r.PL is None and r.GQ == 17 for r in rows)
    assert [r.PL for r in entries(matrix(PL=[40, 30, 20, 10, 0, 50]))] == [[10, 0, 20], [20, 0, 50]]


def test_missing_likelihood_elements_and_depth():
    rows = entries(matrix(GT=hl.call(0, 1), AD=hl.literal([2, None, 3], hl.tarray(hl.tint32)),
                          PL=hl.literal([None, 9, 8, 7, None, 6], hl.tarray(hl.tint32)), GQ=20))
    assert [r.AD for r in rows] == [[None, None], [2, 3]]
    assert [r.PL for r in rows] == [[6, 9, 8], [8, 7, 6]]
    assert [r.GQ for r in rows] == [2, 1]
    rows = entries(matrix(AD=hl.missing(hl.tarray(hl.tint32)), PL=hl.missing(hl.tarray(hl.tint32)), GQ=12))
    assert all(r.AD is None and r.PL is None and r.GQ == 12 for r in rows)


@pytest.mark.parametrize('reference,alternate,position,expected', [
    ('CAA', 'CGA', 101, ['A', 'G']),
    ('é😀A', 'é😀T', 102, ['A', 'T']),
    ('AAA', 'AA', 100, ['AA', 'A']),
    ('AA', '<DEL>', 100, ['AA', '<DEL>']),
    ('AA', 'A[2:3[', 100, ['AA', 'A[2:3[']),
])
def test_min_rep(reference, alternate, position, expected):
    row = hl.split_multi_hts(matrix((reference, alternate))).rows().collect()[0]
    assert row.locus.position == position
    assert row.alleles == expected


def test_star_duplicates_order_and_left_aligned():
    mt = matrix(('CAA', 'CGA', '*', 'CGA'))
    rows = hl.split_multi_hts(mt).rows().collect()
    assert [r.a_index for r in rows] == [1, 3]
    assert [r.a_index for r in hl.split_multi_hts(mt, keep_star=True).rows().collect()] == [2, 1, 3]
    with pytest.raises(RuntimeError, match='non-left-aligned'):
        hl.split_multi_hts(mt, left_aligned=True).rows().collect()
    assert hl.split_multi_hts(matrix(('A', 'C')), left_aligned=True).count_rows() == 1
    assert hl.split_multi_hts(matrix(('A',))).count_rows() == 0
    assert hl.split_multi_hts(mt.filter_entries(False)).entries().count() == 0


def test_schema_contract():
    mt = matrix().key_rows_by().annotate_rows(alleles='ACG').key_rows_by('locus', 'alleles')
    with pytest.raises(ValueError, match='variant keys'):
        hl.split_multi_hts(mt)
    with pytest.raises(TypeError, match='invalid type for AD'):
        hl.split_multi_hts(matrix(AD=['1']))


@pytest.mark.parametrize('kwargs,message', [
    ({'GT': hl.Call([])}, 'haploid or diploid'),
    ({'GT': hl.Call([3])}, 'indices in range'),
    ({'PGT': hl.Call([4])}, 'indices in range'),
    ({'AD': [1, 2]}, 'AD length'),
    ({'PL': [1, 2]}, 'PL length'),
    ({'GT': hl.Call([1]), 'PL': [1, 2, 3, 4, 5, 6]}, 'PL length'),
])
def test_entry_contract(kwargs, message):
    with pytest.raises(RuntimeError, match=message):
        entries(matrix(**kwargs))


@pytest.mark.parametrize('alleles', [[], [''], ['A', ''], ['A', None]])
def test_invalid_alleles(alleles):
    mt = matrix().key_rows_by().annotate_rows(alleles=hl.literal(alleles, hl.tarray(hl.tstr))).key_rows_by('locus', 'alleles')
    with pytest.raises(RuntimeError, match='nonempty alleles'):
        hl.split_multi_hts(mt).count_rows()


@pytest.mark.parametrize('ploidy', [1, 2])
def test_likelihood_buckets_match_public_downcode_oracle(ploidy):
    import random
    rng = random.Random(472)
    for n in (2, 3, 5, 9):
        genotypes = [(a,) for a in range(n)] if ploidy == 1 else [(a, b) for b in range(n) for a in range(b + 1)]
        likelihoods = [rng.choice([None, rng.randrange(50)]) for _ in genotypes]
        rows = entries(matrix(tuple(['A'] + ['C' + str(i) for i in range(1, n)]),
                              GT=hl.Call([1] * ploidy), PL=hl.literal(likelihoods, hl.tarray(hl.tint32))))
        for row in rows:
            expected = []
            for code in range(ploidy + 1):
                bucket = [v for genotype, v in zip(genotypes, likelihoods)
                          if sum(a == row.a_index for a in genotype) == code and v is not None]
                expected.append(min(bucket) if bucket else None)
            assert row.PL == expected


def test_missing_variant_and_normalized_coordinate_validation():
    mt = matrix().key_rows_by().annotate_rows(alleles=hl.missing(hl.tarray(hl.tstr))).key_rows_by('locus', 'alleles')
    with pytest.raises(RuntimeError, match='nonempty alleles'):
        hl.split_multi_hts(mt).rows().collect()
    mt = matrix(('AC', 'AT')).key_rows_by().annotate_rows(locus=hl.locus('1', 249250621)).key_rows_by('locus', 'alleles')
    with pytest.raises(RuntimeError, match='position|coordinate'):
        hl.split_multi_hts(mt).rows().collect()


def test_columns_globals_and_entry_annotations_are_preserved():
    mt = hl.range_matrix_table(2, 3)
    mt = mt.annotate_rows(locus=hl.locus('1', mt.row_idx + 100), alleles=['A', 'C', 'T'])
    mt = mt.key_rows_by('locus', 'alleles').annotate_cols(label='sample')
    mt = mt.annotate_globals(study='example')
    mt = mt.annotate_entries(GT=hl.call(0, 1), DP=mt.col_idx + 1)
    mt = mt.filter_entries(mt.col_idx != 1)
    result = hl.split_multi_hts(mt)
    assert result.count() == (4, 3)
    assert [r.DP for r in result.entries().collect()] == [1, 3] * 4
    assert [r.label for r in result.cols().collect()] == ['sample'] * 3
    assert hl.eval(result.study) == 'example'
