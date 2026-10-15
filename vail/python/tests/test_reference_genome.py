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


def test_equivalent_registrations_preserve_equality_hash_and_coordinate_order():
    first = _reference('review_equivalent_reference')
    equivalent = hl.ReferenceGenome(
        first.name, first.contigs, first.lengths,
        x_contigs=first.x_contigs, y_contigs=first.y_contigs,
        mt_contigs=first.mt_contigs, par=first.par,
    )
    assert first == equivalent
    assert {first, equivalent} == {first}
    assert hash(first) == hash(equivalent)
    loci = [hl.Locus('chr1', 2, first), hl.Locus('chr10', 2, equivalent),
            hl.Locus('chr2', 2, first), hl.Locus('chr2', 1, equivalent)]
    assert [(p.contig, p.position, p.contig_idx) for p in sorted(loci)] == [
        ('chr2', 1, 0), ('chr2', 2, 0), ('chr10', 2, 1), ('chr1', 2, 2),
    ]
    assert len(set(loci + [hl.Locus('chr2', 1, first)])) == 4
    with pytest.raises(ValueError, match='already registered'):
        hl.ReferenceGenome(first.name, reversed(first.contigs), first.lengths,
                            x_contigs=first.x_contigs, y_contigs=first.y_contigs,
                            mt_contigs=first.mt_contigs, par=first.to_wire()['par'])


def test_unsorted_overlapping_nested_and_adjacent_par_preserves_public_order():
    intervals = [
        ('X2', 30, 40), ('Y', 50, 70), ('X2', 10, 20),
        ('X2', 12, 15), ('X2', 18, 35), ('X2', 40, 45),
        ('Y', 5, 10), ('X1', 60, 80), ('X1', 62, 65),
    ]
    reference = hl.ReferenceGenome(
        'review_par_coverage', ['a', 'X2', 'X1', 'Y', 'M'],
        dict.fromkeys(['a', 'X2', 'X1', 'Y', 'M'], 100),
        x_contigs=['X1', 'X2'], y_contigs=['Y'], mt_contigs=['M'], par=intervals,
    )
    assert _par_tuples(reference) == tuple((c, s, e, True, False) for c, s, e in intervals)
    assert reference.to_wire()['par'] == [dict(contig=c, start=s, end=e) for c, s, e in intervals]
    for contig in reference.contigs:
        for position in range(1, 101):
            locus = hl.Locus(contig, position, reference)
            assert locus.in_par == any(c == contig and s <= position < e for c, s, e in intervals)
            assert locus.is_x == (contig in {'X1', 'X2'})
            assert locus.is_y == (contig == 'Y')
            assert locus.is_mitochondrial == (contig == 'M')
            assert reference.is_x_contig(contig) == locus.is_x
            assert reference.is_y_contig(contig) == locus.is_y
            assert reference.is_mt_contig(contig) == locus.is_mitochondrial
            assert reference.is_autosomal(contig) == (contig == 'a')


def test_reference_queries_are_isolated_from_input_and_export_mutation():
    contigs, lengths, x_contigs = ['b', 'a', 'X'], {'b': 20, 'a': 30, 'X': 100}, ['X']
    par = [dict(contig='X', start=10, end=20)]
    reference = hl.ReferenceGenome('review_reference_ownership', contigs, lengths,
                                   x_contigs=x_contigs, par=par)
    expected = reference.to_wire()
    contigs.reverse()
    lengths['b'] = 99
    x_contigs.clear()
    par[0]['start'] = 15
    exported = reference.to_wire()
    exported['contigs'].reverse()
    exported['lengths']['X'] = 1
    exported['x_contigs'].clear()
    exported['par'][0]['end'] = 11
    assert reference.to_wire() == expected
    assert hl.Locus('b', 1, reference).contig_idx == 0
    assert hl.Locus('X', 10, reference).is_x
    assert hl.Locus('X', 19, reference).in_par
    assert not hl.Locus('X', 20, reference).in_par


def test_par_export_reregistration_and_type_cache_preserve_complete_definitions():
    from vail.arrow import arrow_type, dtype_from_arrow
    reference = _reference('review_par_reregistration')
    original = reference.to_wire()
    exported = reference.par
    equivalent = hl.ReferenceGenome(
        reference.name, reference.contigs, reference.lengths,
        x_contigs=reference.x_contigs, y_contigs=reference.y_contigs,
        mt_contigs=reference.mt_contigs, par=exported,
    )
    assert equivalent == reference
    assert equivalent.to_wire() == original
    exported.clear()
    assert reference.to_wire() == original
    assert _par_tuples(reference) == (('chrX', 10, 21, True, False), ('chrY', 30, 41, True, False))
    for dtype in [hl.tlocus(reference), dtype_from_arrow(arrow_type(hl.tlocus(reference)))]:
        assert dtype.reference_genome == equivalent
        assert dtype.reference_genome.to_wire() == original
        interval = hl.Interval(hl.Locus('chrX', 10, reference), hl.Locus('chrX', 21, equivalent),
                               point_type=dtype)
        assert interval.point_type == hl.tlocus(reference)
    changed_lengths = reference.lengths
    changed_lengths['chr2'] += 1
    with pytest.raises(ValueError, match='PAR interval endpoints'):
        hl.ReferenceGenome(
            reference.name, reference.contigs, changed_lengths,
            x_contigs=reference.x_contigs, y_contigs=reference.y_contigs,
            mt_contigs=reference.mt_contigs, par=reference.par,
        )


def test_many_sex_contigs_and_par_intervals_preserve_validation_and_order():
    names = [f'X{i}' for i in range(256)]
    intervals = [(name, 2, 5) for name in reversed(names)]
    reference = hl.ReferenceGenome('review_many_sex_contigs', names, dict.fromkeys(names, 10),
                                   x_contigs=names, par=intervals)
    assert reference.contigs == tuple(names)
    assert reference.x_contigs == tuple(names)
    assert _par_tuples(reference) == tuple((c, s, e, True, False) for c, s, e in intervals)
    for index, name in enumerate(names):
        assert hl.Locus(name, 2, reference).contig_idx == index
        assert hl.Locus(name, 2, reference).is_x
        assert not reference.is_autosomal(name)
        assert not hl.Locus(name, 1, reference).in_par
        assert hl.Locus(name, 2, reference).in_par
        assert hl.Locus(name, 4, reference).in_par
        assert not hl.Locus(name, 5, reference).in_par
    equivalent = hl.ReferenceGenome(reference.name, reference.contigs, reference.lengths,
                                    x_contigs=reference.x_contigs, par=reference.par)
    assert equivalent.to_wire() == reference.to_wire()
