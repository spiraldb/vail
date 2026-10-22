"""Local allele conversions expressed entirely using native Vail expressions."""

import vail as hl
from vail.expressions import as_expr


def lgt_to_gt(lgt, la):
    """Map a local call through canonical LA: LA[0]=0 and unique global alleles.

    Non-reference call alleles must index defined LA elements. Hom-reference
    calls bypass LA, matching Hail even when the LA array itself is missing.
    """
    lgt, la = as_expr(lgt), as_expr(la)

    if lgt.dtype != hl.tcall or la.dtype != hl.tarray(hl.tint32):
        raise TypeError("lgt_to_gt requires a call and an array<int32>")

    def convert(c, a):
        def phased(p):
            return (
                hl.case()
                .when(c.ploidy == 0, hl.call(phased=p))
                .when(c.ploidy == 1, hl.call(a[c[0]], phased=p))
                .default(hl.call(a[c[0]], a[c[1]], phased=p))
            )

        return hl.if_else(
            c.is_non_ref(), hl.if_else(c.phased, phased(True), phased(False)), c
        )

    return hl.bind(convert, lgt, la)


def local_to_global(array, local_alleles, n_alleles, fill_value, number):
    """Expand canonical local alleles (LA[0]=0, unique mapping).

    This experimental G-numbered implementation supports at most 32767 global
    and local alleles, keeping triangular intermediates within signed int32.
    """
    if number not in ("A", "R", "G"):
        raise ValueError("number must be A, R, or G")
    array, local_alleles, n_alleles = map(as_expr, (array, local_alleles, n_alleles))
    if (
        not isinstance(array.dtype, hl.tarray)
        or local_alleles.dtype != hl.tarray(hl.tint32)
        or n_alleles.dtype != hl.tint32
    ):
        raise TypeError(
            "local_to_global requires an array, array<int32> local alleles, and int32 allele count"
        )
    fill_value = (
        hl.literal(fill_value, array.dtype.element_type)
        if not isinstance(fill_value, hl.Expression)
        else fill_value
    )

    def convert(values, alleles, n):
        bad = alleles.find(lambda a: (a < 0) | (a >= n))
        expected_length = (
            hl.len(alleles) * (hl.len(alleles) + 1) // 2
            if number == "G"
            else hl.len(alleles) - (1 if number == "A" else 0)
        )
        if number == "G":
            pairs = hl.range(hl.len(alleles)).flatmap(
                lambda j: hl.range(j + 1).map(
                    lambda i: (
                        hl.call(alleles[i], alleles[j]).unphased_diploid_gt_index(),
                        values[j * (j + 1) // 2 + i],
                    )
                )
            )
            length, start = n * (n + 1) // 2, 0
        else:
            start = 1 if number == "A" else 0
            pairs = hl.range(start, hl.len(alleles)).map(
                lambda i: (alleles[i], values[i - start])
            )
            length = n
        result = hl.bind(
            lambda d: hl.range(start, length).map(lambda i: d.get(i, fill_value)),
            hl.dict(pairs),
        )
        result = (
            hl.case()
            .when(hl.is_missing(bad), result)
            .or_error(
                "local_to_global: local allele of "
                + hl.str(bad)
                + " out of bounds given n_total_alleles of "
                + hl.str(n)
            )
        )
        result = (
            hl.case()
            .when(~alleles.any(hl.is_missing), result)
            .or_error("local_to_global: local alleles elements cannot be missing")
        )
        result = (
            hl.case()
            .when(hl.len(values) == expected_length, result)
            .or_error("local_to_global: array and local alleles lengths differ")
        )
        result = (
            hl.case()
            .when(n >= 0, result)
            .or_error("local_to_global: n_total_alleles less than 0")
        )
        if number == "G":
            # Keep both global and local triangular products in a supported
            # domain before evaluating any length checks or building outputs.
            result = (
                hl.case()
                .when((n <= 32767) & (hl.len(alleles) <= 32767), result)
                .or_error(
                    "local_to_global: experimental G conversion supports at most 32767 global and local alleles"
                )
            )
        return hl.or_missing(
            hl.is_defined(values) & hl.is_defined(alleles) & hl.is_defined(n), result
        )

    return hl.bind(convert, array, local_alleles, n_alleles)
