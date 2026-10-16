import pytest
import vail as hl


def _reference(name='locus_test_reference'):
    return hl.ReferenceGenome(
        name,
        ['chr2', 'chr10', 'chr1', 'chrX', 'chrY', 'chrM'],
        {'chr2': 20, 'chr10': 10, 'chr1': 30, 'chrX': 50, 'chrY': 50, 'chrM': 5},
        x_contigs=['chrX'], y_contigs=['chrY'], mt_contigs=['chrM'],
        par=[('chrX', 10, 21), ('chrY', 30, 41)],
    )


def test_locus_literal_and_type_keep_the_reference_definition():
    rg = _reference()
    value = hl.Locus('chr10', 4, rg)
    expr = hl.literal(value, hl.tlocus(rg))
    assert hl.eval(expr) == value
    assert expr.dtype == hl.tlocus(rg)
    assert value.reference_genome == rg


def test_locus_validates_contig_and_one_based_bounds():
    rg = _reference('locus_validation')
    assert rg.locus('chr2', 1) == hl.Locus('chr2', 1, rg)
    assert rg.locus('chr2', 20).position == 20
    for contig, position in [('missing', 1), ('chr2', 0), ('chr2', 21)]:
        with pytest.raises(ValueError):
            hl.Locus(contig, position, rg)


def test_locus_comparison_requires_equal_reference_definitions():
    rg = _reference('locus_comparison')
    same = hl.ReferenceGenome(
        rg.name, list(rg.contigs), rg.lengths,
        x_contigs=rg.x_contigs, y_contigs=rg.y_contigs,
        mt_contigs=rg.mt_contigs, par=rg.par,
    )
    different = hl.ReferenceGenome(
        'locus_comparison_other', list(rg.contigs), rg.lengths,
        x_contigs=rg.x_contigs, y_contigs=rg.y_contigs,
        mt_contigs=rg.mt_contigs, par=[('chrX', 11, 21), ('chrY', 30, 41)],
    )
    first = hl.Locus('chr2', 1, rg)
    equivalent = hl.Locus('chr2', 1, same)
    incompatible = hl.Locus('chr2', 1, different)
    assert first == equivalent
    assert hl.eval(hl.literal(first) == hl.literal(equivalent)) is True
    with pytest.raises((TypeError, ValueError)):
        _ = hl.literal(first) == hl.literal(incompatible)


def test_locus_coordinate_order_is_declared_contig_order():
    rg = _reference('locus_declared_order')
    loci = [hl.Locus('chr1', 1, rg), hl.Locus('chr10', 1, rg), hl.Locus('chr2', 1, rg)]
    table = hl.Table.parallelize(
        [{'locus': locus, 'label': str(i)} for i, locus in enumerate(loci)],
        schema=hl.tstruct(locus=hl.tlocus(rg), label=hl.tstr), n_partitions=2,
    ).key_by('locus')
    assert [row.locus.contig for row in table.collect()] == ['chr2', 'chr10', 'chr1']


def test_locus_table_and_interval_storage_roundtrip(tmp_path):
    rg = _reference('locus_roundtrip')
    locus_type = hl.tlocus(rg)
    points = [hl.Locus('chr2', 1, rg), hl.Locus('chr10', 2, rg), None]
    table = hl.Table.parallelize(
        [{'point': point, 'region': None if point is None else hl.Interval(point, point, True, True, locus_type)}
         for point in points],
        schema=hl.tstruct(point=locus_type, region=hl.tinterval(locus_type)),
        globals={'anchor': hl.Locus('chr1', 3, rg)}, n_partitions=3,
    ).key_by('point').repartition(3)
    path = tmp_path / 'loci'
    table.write(path)
    loaded = hl.read_table(path)
    assert loaded.row.dtype == table.row.dtype
    assert loaded.key.dtype == table.key.dtype
    assert loaded.collect() == table.collect()
    assert hl.eval(loaded.anchor) == hl.Locus('chr1', 3, rg)
    assert loaded.point.dtype.reference_genome == rg
    assert loaded.region.dtype.point_type.reference_genome == rg
    assert hl.eval(loaded.anchor).reference_genome == rg
    selected = hl.read_table(path, _intervals=[hl.Interval(
        hl.Locus('chr2', 1, rg), hl.Locus('chr10', 2, rg), True, False, locus_type,
    )])
    assert [row.point for row in selected.collect()] == [points[0]]


def test_coordinate_locus_expression_functions_and_classification():
    rg = _reference('locus_expression_functions')
    x_start = hl.Locus('chrX', 10, rg)
    x_end = hl.Locus('chrX', 21, rg)
    y_start = hl.Locus('chrY', 30, rg)
    assert hl.eval(hl.locus('chr2', 2, rg)) == hl.Locus('chr2', 2, rg)
    assert hl.eval(hl.parse_locus('chr10:4', rg)) == hl.Locus('chr10', 4, rg)
    assert hl.eval(hl.is_valid_locus('chr10', 10, rg)) is True
    assert hl.eval(hl.is_valid_locus('chr10', 11, rg)) is False
    assert hl.eval(hl.locus_from_global_position(20, rg)) == hl.Locus('chr10', 1, rg)

    table = hl.Table.parallelize(
        [{'locus': value} for value in (
            hl.Locus('chr2', 1, rg), x_start, hl.Locus('chrX', 20, rg), x_end,
            y_start, hl.Locus('chrM', 2, rg),
        )],
        schema=hl.tstruct(locus=hl.tlocus(rg)),
    )
    annotated = table.annotate(
        global_pos=table.locus.global_position(),
        x_par=table.locus.in_x_par(), x_nonpar=table.locus.in_x_nonpar(),
        y_par=table.locus.in_y_par(), y_nonpar=table.locus.in_y_nonpar(),
        autosome=table.locus.in_autosome(), autosome_or_par=table.locus.in_autosome_or_par(),
        mito=table.locus.in_mito(),
    ).collect()
    assert [r.global_pos for r in annotated] == [0, 69, 79, 80, 139, 161]
    assert [(r.x_par, r.x_nonpar, r.y_par, r.y_nonpar, r.autosome, r.autosome_or_par, r.mito)
            for r in annotated] == [
        (False, False, False, False, True, True, False),
        (True, False, False, False, False, True, False),
        (True, False, False, False, False, True, False),
        (False, True, False, False, False, False, False),
        (False, False, True, False, False, True, False),
        (False, False, False, False, False, False, True),
    ]


def test_locus_reference_mismatch_rejected_in_table_operations():
    first = _reference('locus_mismatch_a')
    second = _reference('locus_mismatch_b')
    left = hl.Table.parallelize(
        [{'locus': hl.Locus('chr2', 1, first)}],
        schema=hl.tstruct(locus=hl.tlocus(first)),
    ).key_by('locus')
    right = hl.Table.parallelize(
        [{'locus': hl.Locus('chr2', 1, second)}],
        schema=hl.tstruct(locus=hl.tlocus(second)),
    ).key_by('locus')
    with pytest.raises((TypeError, ValueError)):
        left.join(right)
    with pytest.raises((TypeError, ValueError)):
        left.union(right)
    with pytest.raises((TypeError, ValueError)):
        left.union(right, unify=True)


def test_nested_nullable_locus_classification_preserves_missingness():
    rg = _reference('nested_nullable_locus')
    locus_type = hl.tlocus(rg)
    table = hl.Table.parallelize(
        [{'nested': {'locus': hl.Locus('chrX', 10, rg)}}, {'nested': {'locus': None}}, {'nested': None}],
        schema=hl.tstruct(nested=hl.tstruct(locus=locus_type)),
    )
    rows = table.annotate(in_par=table.nested.locus.in_x_par()).collect()
    assert [row.in_par for row in rows] == [True, None, None]


def test_locus_sequence_and_liftover_apis_are_explicitly_unsupported():
    rg = _reference('locus_no_sequence')
    locus = hl.Locus('chr2', 2, rg)
    for operation in (locus.sequence_context, locus.sequence, locus.get_sequence, lambda: locus[0]):
        with pytest.raises(NotImplementedError):
            operation()
    for operation in (rg.add_sequence, rg.get_sequence, rg.sequence_context, rg.liftover, rg.from_fasta_file):
        with pytest.raises(NotImplementedError):
            operation()


def test_locus_group_join_repartition_and_key_bounds():
    rg = _reference('locus_table_operations')
    locus_type = hl.tlocus(rg)
    values = [hl.Locus('chr2', 1, rg), hl.Locus('chr10', 2, rg), hl.Locus('chr1', 3, rg)]
    left = hl.Table.parallelize(
        [{'locus': value, 'value': i} for i, value in enumerate(values)],
        schema=hl.tstruct(locus=locus_type, value=hl.tint32), n_partitions=2,
    ).key_by('locus')
    right = hl.Table.parallelize(
        [{'locus': value, 'label': str(i)} for i, value in enumerate(values)],
        schema=hl.tstruct(locus=locus_type, label=hl.tstr), n_partitions=2,
    ).key_by('locus')
    assert [(r.locus, r.n) for r in left.group_by('locus').aggregate(n=hl.agg.count()).collect()] == [
        (value, 1) for value in values
    ]
    joined = left.join(right)
    assert [(row.locus, row.value, row.label) for row in joined.collect()] == [
        (value, i, str(i)) for i, value in enumerate(values)
    ]
    repartitioned = joined.repartition(3)
    assert repartitioned.collect() == joined.collect()
    assert repartitioned.n_partitions() <= 3

    # Position bounds are checked against each contig's declared length.
    for position in (0, 21):
        with pytest.raises(ValueError):
            hl.Locus('chr2', position, rg)
    # Contig rank follows the declared order, never lexical string ordering.
    assert [row.locus.contig for row in left.collect()] == ['chr2', 'chr10', 'chr1']
