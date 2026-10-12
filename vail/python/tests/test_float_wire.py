import math
import struct

import pytest
import vail as hl


def _float64(bits):
    return struct.unpack(">d", bits.to_bytes(8, "big"))[0]


def _bits64(value):
    return struct.unpack(">Q", struct.pack(">d", value))[0]


@pytest.mark.parametrize("bits", [0x7FF0000000000001, 0x7FF8000000001234, 0xFFF8000000001234])
def test_float64_nan_wire_preserves_sign_and_payload(bits):
    value = _float64(bits)
    encoded = hl.encode_value(value, hl.tfloat64)
    assert encoded == {"$float": "nan", "bits": f"{bits:016x}"}
    assert _bits64(hl.decode_value(encoded, hl.tfloat64)) == bits


@pytest.mark.parametrize("bits", [0x7FC01234, 0xFFC01234])
def test_float32_nan_wire_preserves_sign_and_payload(bits):
    value = struct.unpack(">f", bits.to_bytes(4, "big"))[0]
    encoded = hl.encode_value(value, hl.tfloat32)
    assert encoded == {"$float": "nan", "bits": f"{bits:08x}"}
    decoded = hl.decode_value(encoded, hl.tfloat32)
    assert struct.unpack(">I", struct.pack(">f", decoded))[0] == bits


def test_nan_wire_recurses_through_typed_collections_and_legacy_tags():
    bits = 0xFFF8000000000007
    value = _float64(bits)
    dtype = hl.tstruct(
        values=hl.tarray(hl.tfloat64),
        members=hl.tset(hl.tfloat64),
        mapping=hl.tdict(hl.tint32, hl.tfloat64),
    )
    encoded = hl.encode_value(
        {"values": [value], "members": [value], "mapping": {1: value}}, dtype
    )
    assert encoded["values"][0]["bits"] == f"{bits:016x}"
    assert encoded["members"][0]["bits"] == f"{bits:016x}"
    assert encoded["mapping"][0]["value"]["bits"] == f"{bits:016x}"
    decoded = hl.decode_value(encoded, dtype)
    assert _bits64(decoded["values"][0]) == bits
    assert _bits64(next(iter(decoded["members"]))) == bits
    assert _bits64(decoded["mapping"][1]) == bits
    assert math.isnan(hl.decode_value({"$float": "nan"}, hl.tfloat64))


@pytest.mark.parametrize("dtype", [hl.tfloat32, hl.tfloat64])
def test_float_wire_keeps_signed_zero_and_infinity_encoding(dtype):
    negative_zero = hl.encode_value(-0.0, dtype)
    assert math.copysign(1.0, hl.decode_value(negative_zero, dtype)) == -1.0
    assert hl.encode_value(float("inf"), dtype) == {"$float": "inf"}


def test_float32_ndarray_encodes_nan_at_element_width():
    import numpy as np

    bits = 0xFFC01234
    value = np.array([bits], dtype=np.uint32).view(np.float32)
    dtype = hl.tndarray(hl.tfloat32, 1)
    encoded = hl.encode_value(value, dtype)
    assert encoded["__vail_ndarray_data"][0] == {"$float": "nan", "bits": f"{bits:08x}"}
    decoded = hl.decode_value(encoded, dtype)
    assert int(decoded.view(np.uint32)[0]) == bits


def test_generic_float_codec_roundtrip_keeps_legacy_and_bit_tags():
    value = _float64(0xFFF8000000001234)
    assert _bits64(hl.decode_value(hl.encode_value(value))) == 0xFFF8000000001234
    assert math.isnan(hl.decode_value({"$float": "nan"}))
    assert hl.decode_value({"$float": "inf"}) == float("inf")


@pytest.mark.parametrize(
    "tag",
    [
        {"$float": "nan", "bits": "7ff"},
        {"$float": "nan", "bits": "7ff0000000000000"},
        {"$float": "nan", "bits": "7ff8000000000001", "extra": True},
        {"$float": "oops"},
    ],
)
def test_malformed_float_tags_are_rejected(tag):
    with pytest.raises(ValueError):
        hl.decode_value(tag, hl.tfloat64)
