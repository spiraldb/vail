"""Small recursive type and value codecs for Vail's Hail-shaped frontend."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import builtins
import math
import struct


@dataclass(frozen=True)
class _PrimitiveType:
    name: str

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


@dataclass(frozen=True)
class tarray:
    element_type: object

    def __post_init__(self):
        if not is_type(self.element_type):
            raise TypeError("tarray requires a Vail type")

    def __str__(self):
        return f"array<{self.element_type}>"

    def __repr__(self):
        return f"tarray({self.element_type!r})"


@dataclass(frozen=True)
class tset:
    element_type: object

    def __post_init__(self):
        if not is_type(self.element_type):
            raise TypeError("tset requires a Vail type")

    def __str__(self):
        return f"set<{self.element_type}>"

    def __repr__(self):
        return f"tset({self.element_type!r})"


@dataclass(frozen=True, init=False)
class ttuple:
    types: tuple

    def __init__(self, *types):
        if not all(is_type(t) for t in types):
            raise TypeError("ttuple requires Vail types")
        object.__setattr__(self, "types", tuple(types))

    def _validate_value(self, value):
        if not isinstance(value, tuple):
            raise TypeError("expected a Python tuple")
        if len(value) != len(self.types):
            raise TypeError(f"expected a tuple with {len(self.types)} elements, found {len(value)}")


@dataclass(frozen=True)
class tdict:
    key_type: object
    value_type: object

    def __post_init__(self):
        if not is_type(self.key_type) or not is_type(self.value_type):
            raise TypeError("tdict requires Vail key and value types")


@dataclass(frozen=True)
class tndarray:
    element_type: object
    ndim: int

    def __post_init__(self):
        if self.element_type not in (tint32, tint64, tfloat32, tfloat64) or type(self.ndim) is not int or self.ndim < 0:
            raise TypeError("tndarray requires a numeric type and nonnegative rank")


@dataclass(frozen=True)
class tinterval:
    point_type: object

    def __post_init__(self):
        if not is_type(self.point_type):
            raise TypeError("tinterval requires a Vail point type")

    def __str__(self): return f"interval<{self.point_type}>"
    def __repr__(self): return f"tinterval({self.point_type!r})"


@dataclass(frozen=True)
class tlocus:
    reference_genome: object = "default"

    def __post_init__(self):
        from .reference_genome import resolve_reference
        object.__setattr__(self, "reference_genome", resolve_reference(self.reference_genome))

    def __str__(self): return f"locus<{self.reference_genome.name}>"
    def __repr__(self): return f"tlocus({self.reference_genome.name!r})"


@dataclass(frozen=True)
class Interval:
    start: object
    end: object
    includes_start: bool = True
    includes_end: bool = False
    point_type: object = None

    def __post_init__(self):
        if type(self.includes_start) is not bool or type(self.includes_end) is not bool:
            raise TypeError("interval inclusivity flags must be bool")
        if self.point_type is None:
            if self.start is None or self.end is None:
                raise ValueError("cannot infer interval point type from missing endpoint")
            start_type = infer_type(self.start)
            end_type = infer_type(self.end)
            point_type = unify_types(start_type, end_type)
            if point_type is None:
                raise TypeError(f"interval endpoints have incompatible types: {start_type} and {end_type}")
            object.__setattr__(self, "point_type", point_type)
        elif not is_type(self.point_type):
            raise TypeError("point_type must be a Vail type")
        if not _value_matches_type(self.start, self.point_type) or not _value_matches_type(self.end, self.point_type):
            raise TypeError(f"interval endpoints must match point_type {self.point_type}")

    def __str__(self):
        left = "[" if self.includes_start else "("
        right = "]" if self.includes_end else ")"
        return f"{left}{self.start}-{self.end}{right}"

    def contains(self, value):
        from . import eval
        from .expressions import literal

        return eval(literal(self, tinterval(self.point_type)).contains(value))

    def overlaps(self, other):
        from . import eval
        from .expressions import literal

        return eval(literal(self, tinterval(self.point_type)).overlaps(other))


def _value_matches_type(value, dtype):
    if value is None:
        return True
    if isinstance(dtype, tlocus):
        from .reference_genome import Locus
        return isinstance(value, Locus) and value.reference_genome == dtype.reference_genome
    if dtype in (tint32, tint64):
        return type(value) is int and -(2 ** (31 if dtype == tint32 else 63)) <= value < 2 ** (31 if dtype == tint32 else 63)
    if dtype in (tfloat32, tfloat64):
        return type(value) in (int, float)
    if dtype == tbool: return type(value) is bool
    if dtype == tstr: return isinstance(value, str)
    if dtype == tcall: return isinstance(value, Call)
    if isinstance(dtype, tarray):
        return isinstance(value, (list, tuple)) and all(_value_matches_type(v, dtype.element_type) for v in value)
    if isinstance(dtype, tset):
        return isinstance(value, (set, frozenset)) and all(_value_matches_type(v, dtype.element_type) for v in value)
    if isinstance(dtype, ttuple):
        return isinstance(value, tuple) and len(value) == len(dtype.types) and all(
            _value_matches_type(v, t) for v, t in zip(value, dtype.types))
    if isinstance(dtype, tdict):
        return isinstance(value, Mapping) and all(_value_matches_type(k, dtype.key_type) and
                                                   _value_matches_type(v, dtype.value_type)
                                                   for k, v in value.items())
    if isinstance(dtype, tstruct):
        return isinstance(value, Mapping) and set(value) == set(dtype) and all(
            _value_matches_type(value[k], t) for k, t in dtype.items())
    if isinstance(dtype, tinterval):
        return isinstance(value, Interval) and value.point_type == dtype.point_type
    if isinstance(dtype, tndarray):
        try:
            import numpy as np
            return isinstance(value, np.ndarray) and value.ndim == dtype.ndim and str(value.dtype) == str(dtype.element_type)
        except ImportError:
            return False
    return False


@dataclass(frozen=True)
class Call:
    alleles: tuple
    phased: bool = False

    def __post_init__(self):
        object.__setattr__(self, "alleles", tuple(self.alleles))
        if len(self.alleles) > 2 or any(type(a) is not int or a < 0 for a in self.alleles):
            raise ValueError("calls require zero, one, or two nonnegative integer alleles")
        if type(self.phased) is not bool:
            raise TypeError("phased must be bool")
        if not self.phased:
            object.__setattr__(self, "alleles", tuple(sorted(self.alleles)))
        _pack_call(self)

    @property
    def ploidy(self):
        return len(self.alleles)

    def __getitem__(self, i):
        return self.alleles[i]


tint32 = _PrimitiveType("int32")
tint64 = _PrimitiveType("int64")
tfloat32 = _PrimitiveType("float32")
tfloat64 = _PrimitiveType("float64")
tbool = _PrimitiveType("bool")
tstr = _PrimitiveType("str")
def _pack_call(value):
    a = value.alleles
    if not a:
        representation = 0
    elif len(a) == 1:
        representation = a[0]
    else:
        j, k = (a[0], a[0] + a[1]) if value.phased else (min(a), max(a))
        representation = k * (k + 1) // 2 + j
    if not 0 <= representation < (1 << 29):
        raise ValueError("Call allele representation exceeds 29 bits")
    word = (representation << 3) | (len(a) << 1) | int(value.phased)
    return word if word < (1 << 31) else word - (1 << 32)


def _unpack_call(word):
    if type(word) is not int or not -(1 << 31) <= word < (1 << 31):
        raise ValueError("invalid packed Call word")
    bits = word & 0xffffffff
    ploidy, representation, phased = (bits >> 1) & 3, bits >> 3, bool(bits & 1)
    if ploidy == 0 and representation == 0:
        alleles = ()
    elif ploidy == 1:
        alleles = (representation,)
    elif ploidy == 2:
        k = (math.isqrt(8 * representation + 1) - 1) // 2
        j = representation - k * (k + 1) // 2
        alleles = (j, k-j if phased else k)
    else:
        raise ValueError("invalid packed Call ploidy")
    result = Call(alleles, phased)
    if _pack_call(result) != word:
        raise ValueError("invalid packed Call")
    return result


tcall = _PrimitiveType("call")
tint = tint32
tfloat = tfloat64


class tstruct(Mapping):
    def __init__(self, **fields):
        for name, dtype in fields.items():
            if not isinstance(name, str):
                raise TypeError("struct field names must be strings")
            if not is_type(dtype):
                raise TypeError(f"unsupported type for {name!r}: {dtype}")
        self._fields = dict(fields)

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, name):
        return self._fields[name]

    def __repr__(self):
        return "struct{" + ", ".join(f"{k}: {v}" for k, v in self.items()) + "}"

    def __eq__(self, other):
        return isinstance(other, tstruct) and tuple(self.items()) == tuple(other.items())


class Struct(Mapping):
    def __init__(self, **fields):
        object.__setattr__(self, "_fields", dict(fields))

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, name):
        return self._fields[name]

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        raise TypeError("Struct is immutable")

    def __delattr__(self, name):
        raise TypeError("Struct is immutable")

    def __repr__(self):
        return "Struct(" + ", ".join(f"{k}={v!r}" for k, v in self.items()) + ")"

    def __eq__(self, other):
        return isinstance(other, Mapping) and dict(self) == dict(other)

    def __hash__(self):
        return 37 + hash(tuple(sorted(self._fields.items())))


def is_type(value):
    return value in (tint32, tint64, tfloat32, tfloat64, tbool, tstr, tcall) or isinstance(value, (tstruct, tarray, tset, tdict, tndarray, ttuple, tinterval, tlocus))


def dtype(type_string):
    """Parse the primitive, array, and struct subset used by the frontend."""
    if not builtins.isinstance(type_string, builtins.str):
        raise TypeError("dtype expects a string")
    return _parse_type(type_string.strip())


def _split_type_fields(body):
    fields = []
    start = 0
    depth = 0
    for i, char in enumerate(body):
        if char in "<{[":
            depth += 1
        elif char in ">}]":
            depth -= 1
        elif char == "," and depth == 0:
            fields.append(body[start:i].strip())
            start = i + 1
    tail = body[start:].strip()
    if tail:
        fields.append(tail)
    return fields


def _parse_type(text):
    aliases = {
        "int": tint32, "int32": tint32, "int64": tint64,
        "float": tfloat64, "float32": tfloat32, "float64": tfloat64,
        "bool": tbool, "str": tstr, "call": tcall,
    }
    if text in aliases:
        return aliases[text]
    if text.startswith("dict<") and text.endswith(">"):
        k, v = _split_type_fields(text[5:-1])
        return tdict(_parse_type(k), _parse_type(v))
    if text.startswith("ndarray<") and text.endswith(">"):
        element, rank = _split_type_fields(text[8:-1])
        return tndarray(_parse_type(element), int(rank))
    if text.startswith("interval<") and text.endswith(">"):
        return tinterval(_parse_type(text[9:-1].strip()))
    if text.startswith("locus<") and text.endswith(">"):
        return tlocus(text[6:-1].strip())
    if text.startswith("array<") and text.endswith(">"):
        return tarray(_parse_type(text[6:-1].strip()))
    if text.startswith("set<") and text.endswith(">"):
        return tset(_parse_type(text[4:-1].strip()))
    if text.startswith("struct{") and text.endswith("}"):
        fields = {}
        for decl in _split_type_fields(text[7:-1]):
            depth = 0
            colon = None
            for i, char in enumerate(decl):
                if char in "<{[": depth += 1
                elif char in ">}]": depth -= 1
                elif char == ":" and depth == 0:
                    colon = i
                    break
            if colon is None:
                raise ValueError(f"invalid struct field type declaration: {decl!r}")
            name = decl[:colon].strip()
            if not name or name in fields:
                raise ValueError(f"invalid or duplicate struct field name: {name!r}")
            fields[name] = _parse_type(decl[colon + 1:].strip())
        return tstruct(**fields)
    raise NotImplementedError(f"unsupported type string: {text!r}")


def type_to_wire(dtype):
    if isinstance(dtype, tlocus):
        return {"locus": dtype.reference_genome.to_wire()}
    if isinstance(dtype, _PrimitiveType):
        return dtype.name
    if isinstance(dtype, ttuple):
        return {"tuple": [type_to_wire(t) for t in dtype.types]}
    if isinstance(dtype, tdict):
        return {"dict": [type_to_wire(dtype.key_type), type_to_wire(dtype.value_type)]}
    if isinstance(dtype, tndarray):
        return {"ndarray": [type_to_wire(dtype.element_type), dtype.ndim]}
    if isinstance(dtype, tinterval):
        return {"interval": type_to_wire(dtype.point_type)}
    if isinstance(dtype, tarray):
        return {"array": type_to_wire(dtype.element_type)}
    if isinstance(dtype, tset):
        return {"set": type_to_wire(dtype.element_type)}
    if isinstance(dtype, tstruct):
        return {"struct": [[name, type_to_wire(field_type)] for name, field_type in dtype.items()]}
    raise TypeError(f"not a Vail type: {dtype!r}")


def type_from_wire(value):
    if isinstance(value, dict) and set(value) == {"locus"}:
        from .reference_genome import ReferenceGenome
        return tlocus(ReferenceGenome.from_wire(value["locus"]))
    if isinstance(value, str):
        primitives = {t.name: t for t in (tint32, tint64, tfloat32, tfloat64, tbool, tstr, tcall)}
        try:
            return primitives[value]
        except KeyError:
            raise ValueError(f"unknown Vail type: {value}") from None
    if isinstance(value, dict) and set(value) == {"tuple"}:
        return ttuple(*(type_from_wire(t) for t in value["tuple"]))
    if isinstance(value, dict) and set(value) == {"dict"}:
        return tdict(*(type_from_wire(t) for t in value["dict"]))
    if isinstance(value, dict) and set(value) == {"ndarray"}:
        return tndarray(type_from_wire(value["ndarray"][0]), value["ndarray"][1])
    if isinstance(value, dict) and set(value) == {"interval"}:
        return tinterval(type_from_wire(value["interval"]))
    if isinstance(value, dict) and set(value) == {"array"}:
        return tarray(type_from_wire(value["array"]))
    if isinstance(value, dict) and set(value) == {"set"}:
        return tset(type_from_wire(value["set"]))
    if isinstance(value, dict) and set(value) == {"struct"}:
        return tstruct(**{name: type_from_wire(field_type) for name, field_type in value["struct"]})
    raise ValueError(f"invalid Vail type encoding: {value!r}")


def infer_type(value):
    from .reference_genome import Locus
    if isinstance(value, Locus):
        return tlocus(value.reference_genome)
    if isinstance(value, Interval):
        return tinterval(value.point_type)
    if isinstance(value, Call):
        return tcall
    import numpy as np
    if isinstance(value, np.ndarray):
        numeric = {str(t): t for t in (tint32, tint64, tfloat32, tfloat64)}
        if str(value.dtype) not in numeric:
            raise TypeError("ndarray elements must be int32/int64/float32/float64")
        return tndarray(numeric[str(value.dtype)], value.ndim)
    if value is None:
        raise ValueError("cannot infer a type from a missing value; provide a type")
    if type(value) is bool:
        return tbool
    if type(value) is int:
        if -(2**31) <= value < 2**31:
            return tint32
        if -(2**63) <= value < 2**63:
            return tint64
        raise OverflowError("integer does not fit int64")
    if isinstance(value, float):
        return tfloat64
    if isinstance(value, str):
        return tstr
    if isinstance(value, Mapping):
        if any(field is None for field in value.values()):
            raise ValueError("cannot infer a struct field type from a missing value; provide a type")
        return tstruct(**{name: infer_type(field) for name, field in value.items()})
    if isinstance(value, list):
        if not value:
            raise ValueError("cannot infer an array element type from an empty array")
        types = [infer_type(x) for x in value if x is not None]
        if not types:
            raise ValueError("cannot infer an array element type from only missing values")
        dtype = types[0]
        for other in types[1:]:
            dtype = unify_types(dtype, other)
            if dtype is None:
                raise TypeError("array elements have incompatible types")
        return tarray(dtype)
    if isinstance(value, (set, frozenset)):
        if not value:
            raise ValueError("cannot infer a set element type from an empty set")
        types = [infer_type(x) for x in value if x is not None]
        if not types:
            raise ValueError("cannot infer a set element type from only missing values")
        element = types[0]
        for other in types[1:]:
            element = unify_types(element, other)
            if element is None:
                raise TypeError("set elements have incompatible types")
        return tset(element)
    if isinstance(value, tuple):
        return ttuple(*(infer_type(v) for v in value))
    raise TypeError(f"cannot infer a Vail type for {type(value).__name__}")


def unify_types(left, right):
    if left == right:
        return left
    if isinstance(left, tlocus) or isinstance(right, tlocus):
        return None
    numeric = (tint32, tint64, tfloat32, tfloat64)
    if left in numeric and right in numeric:
        if tfloat64 in (left, right) or {left, right} == {tfloat32, tint64}:
            return tfloat64
        if tfloat32 in (left, right):
            return tfloat32
        if tint64 in (left, right):
            return tint64
        return tint32
    if isinstance(left, tarray) and isinstance(right, tarray):
        element = unify_types(left.element_type, right.element_type)
        return None if element is None else tarray(element)
    if isinstance(left, tset) and isinstance(right, tset):
        element = unify_types(left.element_type, right.element_type)
        return None if element is None else tset(element)
    if isinstance(left, tstruct) and isinstance(right, tstruct) and list(left) == list(right):
        fields = {name: unify_types(left[name], right[name]) for name in left}
        return None if any(value is None for value in fields.values()) else tstruct(**fields)
    if isinstance(left, ttuple) and isinstance(right, ttuple) and len(left.types) == len(right.types):
        elements = [unify_types(a, b) for a, b in zip(left.types, right.types)]
        return None if any(element is None for element in elements) else ttuple(*elements)
    if isinstance(left, tdict) and isinstance(right, tdict):
        key = unify_types(left.key_type, right.key_type)
        value = unify_types(left.value_type, right.value_type)
        return None if key is None or value is None else tdict(key, value)
    if isinstance(left, tndarray) and isinstance(right, tndarray) and left.ndim == right.ndim:
        element = unify_types(left.element_type, right.element_type)
        return None if element is None else tndarray(element, left.ndim)
    if isinstance(left, tinterval) and isinstance(right, tinterval):
        point = unify_types(left.point_type, right.point_type)
        return None if point is None else tinterval(point)
    return None


def encode_value(value, dtype=None):
    if value is None:
        return None
    if isinstance(dtype, tlocus):
        from .reference_genome import Locus
        if not isinstance(value, Locus) or value.reference_genome != dtype.reference_genome:
            raise TypeError(f"value must be a Locus on reference genome {dtype.reference_genome.name}")
        ordinal = dtype.reference_genome.contigs.index(value.contig)
        return {"__vail_locus_ordinal": ordinal, "__vail_locus_contig": value.contig,
                "__vail_locus_position": value.position}
    if isinstance(dtype, tinterval):
        if not isinstance(value, Interval) or value.point_type != dtype.point_type:
            raise TypeError(f"value must be an Interval with point type {dtype.point_type}")
        return {"start": encode_value(value.start, dtype.point_type), "end": encode_value(value.end, dtype.point_type),
                "includes_start": value.includes_start, "includes_end": value.includes_end}
    if isinstance(dtype, tdict):
        return [{"key": encode_value(k, dtype.key_type), "value": encode_value(v, dtype.value_type)} for k, v in value.items()]
    if isinstance(dtype, ttuple):
        dtype._validate_value(value)
        if not dtype.types:
            return {"__vail_tuple_empty": False}
        return {f"__vail_tuple_{i}": encode_value(value[i], ty) for i, ty in enumerate(dtype.types)}
    if isinstance(dtype, tndarray):
        import numpy as np
        if isinstance(value, Mapping) and "__vail_ndarray_data" in value:
            return {
                "__vail_ndarray_data": encode_value(value["__vail_ndarray_data"], tarray(dtype.element_type)),
                "__vail_ndarray_shape": encode_value(value["__vail_ndarray_shape"]),
            }
        a = np.asarray(value, dtype=str(dtype.element_type))
        if a.ndim != dtype.ndim:
            raise ValueError("ndarray rank does not match dtype")
        return {
            "__vail_ndarray_data": encode_value(a.ravel().tolist(), tarray(dtype.element_type)),
            "__vail_ndarray_shape": list(a.shape),
        }
    if isinstance(dtype, (tarray, tset)):
        return [encode_value(v, dtype.element_type) for v in value]
    if isinstance(dtype, tstruct):
        return {name: encode_value(value[name], ty) for name, ty in dtype.items()}
    if isinstance(value, Call):
        return {"__vail_call_word": _pack_call(value)}
    if isinstance(value, Struct):
        value = dict(value)
    if isinstance(value, Mapping):
        return {name: encode_value(item) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        # Preserve the elements for native typed canonicalization. A generic
        # JSON codec cannot sort all Vail values with their type's semantics.
        return [encode_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            if dtype == tfloat32:
                bits = struct.unpack(">I", struct.pack(">f", value))[0]
                return {"$float": "nan", "bits": f"{bits:08x}"}
            bits = struct.unpack(">Q", struct.pack(">d", value))[0]
            return {"$float": "nan", "bits": f"{bits:016x}"}
        return {"$float": "inf" if value > 0 else "-inf"}
    return value


def decode_value(value, dtype=None):
    if value is None:
        return None
    if isinstance(dtype, tlocus):
        from .reference_genome import Locus
        if not isinstance(value, Mapping) or set(value) != {"__vail_locus_ordinal", "__vail_locus_contig", "__vail_locus_position"}:
            raise ValueError("invalid locus value encoding")
        contig = value["__vail_locus_contig"]
        ordinal = value["__vail_locus_ordinal"]
        if type(ordinal) is not int or not 0 <= ordinal < len(dtype.reference_genome.contigs) or dtype.reference_genome.contigs[ordinal] != contig:
            raise ValueError("locus contig ordinal does not match reference genome order")
        return Locus(contig, value["__vail_locus_position"], dtype.reference_genome)
    if isinstance(dtype, tinterval):
        return Interval(decode_value(value["start"], dtype.point_type), decode_value(value["end"], dtype.point_type),
                        value["includes_start"], value["includes_end"], dtype.point_type)
    if dtype == tcall:
        return _unpack_call(value["__vail_call_word"])
    if isinstance(dtype, ttuple):
        return tuple(decode_value(value[f"__vail_tuple_{i}"], t) for i, t in enumerate(dtype.types))
    if isinstance(dtype, tdict):
        return {_decode_set_element(v["key"], dtype.key_type): decode_value(v["value"], dtype.value_type) for v in value}
    if isinstance(dtype, tndarray):
        import numpy as np
        data = [decode_value(v, dtype.element_type) for v in value["__vail_ndarray_data"]]
        return np.asarray(data, dtype=str(dtype.element_type)).reshape(value["__vail_ndarray_shape"])
    if dtype is None and isinstance(value, dict) and set(value) in ({"$float"}, {"$float", "bits"}):
        # Generic codec callers have no declared type; the tag carries its width.
        bits = value.get("bits")
        dtype = tfloat32 if isinstance(bits, str) and len(bits) == 8 else tfloat64
    if dtype in (tfloat32, tfloat64) and isinstance(value, dict):
        if set(value) == {"$float"}:
            try:
                return {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}[value["$float"]]
            except (KeyError, TypeError) as exc:
                raise ValueError("invalid floating-point tag") from exc
        if set(value) == {"$float", "bits"} and value["$float"] == "nan":
            width = 32 if dtype == tfloat32 else 64
            bits_text = value["bits"]
            if not isinstance(bits_text, str) or len(bits_text) != width // 4:
                raise ValueError("invalid NaN bit pattern")
            if not all(c in "0123456789abcdefABCDEF" for c in bits_text):
                raise ValueError("invalid NaN bit pattern")
            try:
                bits = int(bits_text, 16)
            except ValueError as exc:
                raise ValueError("invalid NaN bit pattern") from exc
            exponent_mask, fraction_mask = ((0x7F800000, 0x007FFFFF) if width == 32 else (0x7FF0000000000000, 0x000FFFFFFFFFFFFF))
            if bits >= 1 << width or bits & exponent_mask != exponent_mask or bits & fraction_mask == 0:
                raise ValueError("NaN bit pattern does not encode a NaN")
            return struct.unpack(">f" if width == 32 else ">d", bits.to_bytes(width // 8, "big"))[0]
        raise ValueError("invalid floating-point tag")
    if isinstance(dtype, tstruct) and isinstance(value, dict):
        return Struct(**{name: decode_value(value.get(name), field_type) for name, field_type in dtype.items()})
    if isinstance(dtype, tarray) and isinstance(value, list):
        return [decode_value(item, dtype.element_type) for item in value]
    if isinstance(dtype, tset) and isinstance(value, list):
        items = [_decode_set_element(item, dtype.element_type) for item in value]
        try:
            return set(items)
        except TypeError as exc:
            raise NotImplementedError(
                f"cannot localize set elements of type {dtype.element_type} as hashable Python values"
            ) from exc
    if isinstance(value, dict):
        return Struct(**{name: decode_value(item) for name, item in value.items()})
    if isinstance(value, list):
        return [decode_value(item) for item in value]
    return value


def _decode_set_element(value, dtype):
    """Decode a set member into a hashable Python value where supported."""
    if isinstance(dtype, tset):
        if value is None:
            return None
        if not isinstance(value, list):
            return decode_value(value, dtype)
        return frozenset(_decode_set_element(item, dtype.element_type) for item in value)
    # Dictionary keys and set members need immutable nested representations.
    if isinstance(dtype, tarray):
        return None if value is None else tuple(_decode_set_element(v, dtype.element_type) for v in value)
    if isinstance(dtype, tstruct):
        return None if value is None else Struct(**{name: _decode_set_element(value[name], ty) for name, ty in dtype.items()})
    return decode_value(value, dtype)


def _ensure_orderable(dtype):
    if isinstance(dtype, (tset, tdict)):
        raise NotImplementedError("Set/Dictionary ordering and keying are unsupported")
    if isinstance(dtype, tstruct):
        children = dtype.values()
    elif isinstance(dtype, ttuple):
        children = dtype.types
    elif isinstance(dtype, (tarray, tndarray)):
        children = (dtype.element_type,)
    elif isinstance(dtype, tinterval):
        children = (dtype.point_type,)
    else:
        children = ()
    for child in children:
        _ensure_orderable(child)
