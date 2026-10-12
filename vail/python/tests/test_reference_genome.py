import pytest
import vail as hl


def _reference(name='test_reference_genome'):
    return hl.ReferenceGenome(
        name,
        ['chr2', 'chr10', 'chr1', 'chrX', 'chrY', 'chrM'],
        {'chr2': 200, 'chr10': 100, 'chr1': 300, 'chrX': 500, 'chrY': 400, 'chrM': 20},
        x_contigs=['chrX'], y_contigs=['chrY'], mt_contigs=['chrM'],
        par=[('chrX', 10, 21), ('chrY', 30, 41)],
    )


def _par_tuples(rg):
    return tuple((interval.start.contig, interval.start.position, interval.end.position,
                  interval.includes_start, interval.includes_end) for interval in rg.par)


def test_coordinate_definition_is_ordered_immutable_and_registered():
    rg = _reference()
    assert rg.contigs == ('chr2', 'chr10', 'chr1', 'chrX', 'chrY', 'chrM')
    assert list(rg.lengths) == list(rg.contigs)
    assert rg.lengths['chr10'] == 100
    assert rg.x_contigs == ('chrX',)
    assert rg.y_contigs == ('chrY',)
    assert rg.mt_contigs == ('chrM',)
    assert _par_tuples(rg) == (('chrX', 10, 21, True, False), ('chrY', 30, 41, True, False))
    assert hl.get_reference(rg.name) is rg
    assert hl.tlocus(rg) == hl.tlocus(rg.name)

    lengths = rg.lengths
    lengths['chr2'] = 999
    assert rg.contig_length('chr2') == 200
    with pytest.raises((AttributeError, TypeError)):
        rg.name = 'changed'


def test_registration_rejects_conflicting_definition_but_accepts_same_definition():
    rg = _reference('registration_identity')
    equivalent = hl.ReferenceGenome(
        'registration_identity', list(rg.contigs), rg.lengths,
        x_contigs=list(rg.x_contigs), y_contigs=list(rg.y_contigs),
        mt_contigs=list(rg.mt_contigs), par=list(rg.par),
    )
    assert equivalent == rg
    # Hail accepts an equivalent re-registration and the registry resolves to
    # the most recently constructed object.
    assert hl.get_reference(rg.name) is equivalent
    with pytest.raises(ValueError, match='already registered'):
        hl.ReferenceGenome(rg.name, ['chr2'], {'chr2': 200})


def test_reference_definition_rejects_invalid_metadata():
    with pytest.raises(ValueError):
        hl.ReferenceGenome('bad_lengths', ['a', 'b'], {'a': 1})
    with pytest.raises(ValueError):
        hl.ReferenceGenome('bad_classification', ['a'], {'a': 10}, x_contigs=['missing'])
    with pytest.raises(ValueError):
        hl.ReferenceGenome('bad_par', ['a'], {'a': 10}, par=[('a', 0, 2)])
    with pytest.raises(ValueError):
        hl.ReferenceGenome('bad_par_end', ['a'], {'a': 10}, par=[('a', 1, 11)])


def test_builtin_reference_coordinates_match_pinned_hail():
    grch37 = hl.get_reference('GRCh37')
    grch38 = hl.get_reference('GRCh38')
    assert grch37.contigs[:3] == ('1', '2', '3')
    assert grch37.lengths['1'] == 249250621
    assert _par_tuples(grch37) == (
        ('X', 60001, 2699521, True, False), ('X', 154931044, 155260561, True, False),
        ('Y', 10001, 2649521, True, False), ('Y', 59034050, 59363567, True, False),
    )
    assert grch38.contigs[:3] == ('chr1', 'chr2', 'chr3')
    assert grch38.lengths['chr1'] == 248956422
    assert _par_tuples(grch38) == (
        ('chrX', 10001, 2781480, True, False), ('chrX', 155701383, 156030896, True, False),
        ('chrY', 10001, 2781480, True, False), ('chrY', 56887903, 57217416, True, False),
    )
