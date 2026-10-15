import math
import struct

import pytest
import vail as hl


def _float64(bits):
    return struct.unpack(">d", bits.to_bytes(8, "big"))[0]


def _bits64(value):
    return struct.unpack(">Q", struct.pack(">d", value))[0]


@pytest.mark.parametrize("bits", [0x7FF0000000000001, 0x7FF8000000001234, 0xFFF8000000001234])
def test_native_float64_preserves_nan_sign_and_payload(bits):
    value = _float64(bits)
    assert _bits64(hl.eval(hl.literal(value, hl.tfloat64))) == bits


@pytest.mark.parametrize("bits", [0x7FC01234, 0xFFC01234])
def test_native_float32_preserves_nan_sign_and_payload(bits):
    value = struct.unpack(">f", bits.to_bytes(4, "big"))[0]
    decoded = hl.eval(hl.literal(value, hl.tfloat32))
    assert struct.unpack(">I", struct.pack(">f", decoded))[0] == bits


def test_native_nan_payload_survives_nested_collections():
    bits = 0xFFF8000000000007
    value = _float64(bits)
    dtype = hl.tstruct(
        values=hl.tarray(hl.tfloat64),
        members=hl.tset(hl.tfloat64),
        mapping=hl.tdict(hl.tint32, hl.tfloat64),
    )
    decoded = hl.eval(hl.literal(
        {"values": [value], "members": [value], "mapping": {1: value}}, dtype
    ))
    assert _bits64(decoded["values"][0]) == bits
    assert _bits64(next(iter(decoded["members"]))) == bits
    assert _bits64(decoded["mapping"][1]) == bits


@pytest.mark.parametrize("dtype", [hl.tfloat32, hl.tfloat64])
def test_native_float_keeps_signed_zero_and_infinity(dtype):
    assert math.copysign(1.0, hl.eval(hl.literal(-0.0, dtype))) == -1.0
    assert hl.eval(hl.literal(float("inf"), dtype)) == float("inf")


def test_native_float32_ndarray_keeps_nan_at_element_width():
    import numpy as np

    bits = 0xFFC01234
    value = np.array([bits], dtype=np.uint32).view(np.float32)
    dtype = hl.tndarray(hl.tfloat32, 1)
    decoded = hl.eval(hl.literal(value, dtype))
    assert int(decoded.view(np.uint32)[0]) == bits


def test_float_tag_names_are_ordinary_struct_fields():
    value = hl.Struct(**{"$float": "nan", "bits": "7ff8000000001234"})
    assert hl.eval(hl.literal(value)) == value


@pytest.mark.parametrize(
    "tag",
    [
        {"$float": "nan", "bits": "7ff"},
        {"$float": "nan", "bits": "7ff0000000000000"},
        {"$float": "nan", "bits": "7ff8000000000001", "extra": True},
        {"$float": "oops"},
    ],
)
def test_encoded_tags_are_not_native_float_values(tag):
    with pytest.raises((TypeError, ValueError)):
        hl.literal(tag, hl.tfloat64)
