"""Native type wrappers and Python value localization for the Vail frontend."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
import pprint
import json
import re


from ._native import NativeType


class _NativeType:
    """API wrapper; all type structure is owned by a native Type handle."""
    __slots__ = ('_native_type',)

    def _set_native(self, native):
        object.__setattr__(self, '_native_type', native)

    def __setattr__(self, name, value):
        raise TypeError("Types are immutable")

    def __eq__(self, other):
        return isinstance(other, _NativeType) and self._native_type == other._native_type

    def __hash__(self):
        return hash(self._native_type)


class _PrimitiveType(_NativeType):
    def __init__(self, name):
        self._set_native(NativeType(name))

    @property
    def name(self):
        return self._native_type.kind

    def __str__(self): return self.name
    def __repr__(self): return self.name


class tarray(_NativeType):
    def __init__(self, element_type):
        if not is_type(element_type): raise TypeError("tarray requires a Vail type")
        self._set_native(NativeType.construct('array', [type_to_wire(element_type)]))

    @property
    def element_type(self): return _wrap_native_type(self._native_type.child(0))
    def __str__(self): return f"array<{self.element_type}>"
    def __repr__(self): return f"tarray({self.element_type!r})"


class tset(_NativeType):
    def __init__(self, element_type):
        if not is_type(element_type): raise TypeError("tset requires a Vail type")
        self._set_native(NativeType.construct('set', [type_to_wire(element_type)]))
    @property
    def element_type(self): return _wrap_native_type(self._native_type.child(0))
    def __str__(self): return f"set<{self.element_type}>"
    def __repr__(self): return f"tset({self.element_type!r})"


class ttuple(_NativeType):
    def __init__(self, *types):
        if not all(is_type(t) for t in types): raise TypeError("ttuple requires Vail types")
        self._set_native(NativeType.construct('tuple', [type_to_wire(t) for t in types]))
    @property
    def types(self): return tuple(_wrap_native_type(t) for t in self._native_type.children)
    def _validate_value(self, value):
        if not isinstance(value, tuple): raise TypeError("expected a Python tuple")
        if len(value) != len(self.types):
            raise TypeError(f"expected a tuple with {len(self.types)} elements, found {len(value)}")
    def __str__(self): return f"tuple({', '.join(str(t) for t in self.types)})"
    def __repr__(self): return f"ttuple({', '.join(repr(t) for t in self.types)})"


class tdict(_NativeType):
    def __init__(self, key_type, value_type):
        if not is_type(key_type) or not is_type(value_type): raise TypeError("tdict requires Vail key and value types")
        self._set_native(NativeType.construct('dict', [type_to_wire(key_type), type_to_wire(value_type)]))
    @property
    def key_type(self): return _wrap_native_type(self._native_type.child(0))
    @property
    def value_type(self): return _wrap_native_type(self._native_type.child(1))
    def __str__(self): return f"dict<{self.key_type}, {self.value_type}>"
    def __repr__(self): return f"tdict({self.key_type!r}, {self.value_type!r})"


class tndarray(_NativeType):
    def __init__(self, element_type, ndim):
        if element_type not in (tint32, tint64, tfloat32, tfloat64) or type(ndim) is not int or ndim < 0:
            raise TypeError("tndarray requires a numeric type and nonnegative rank")
        self._set_native(NativeType.construct('ndarray', [type_to_wire(element_type)], rank=ndim))
    @property
    def element_type(self): return _wrap_native_type(self._native_type.child(0))
    @property
    def ndim(self): return self._native_type.ndim
    def __str__(self): return f"ndarray<{self.element_type}, {self.ndim}>"
    def __repr__(self): return f"tndarray({self.element_type!r}, {self.ndim})"


class tinterval(_NativeType):
    def __init__(self, point_type):
        if not is_type(point_type): raise TypeError("tinterval requires a Vail point type")
        self._set_native(NativeType.construct('interval', [type_to_wire(point_type)]))
    @property
    def point_type(self): return _wrap_native_type(self._native_type.child(0))
    def __str__(self): return f"interval<{self.point_type}>"
    def __repr__(self): return f"tinterval({self.point_type!r})"


class tlocus(_NativeType):
    """Immutable native locus type with a cached immutable coordinate reference."""
    __slots__ = ('_reference',)

    def __init__(self, reference_genome='default'):
        from .reference_genome import resolve_reference
        reference = resolve_reference(reference_genome)
        self._set_native(NativeType.construct('locus', [], reference_value=reference.to_wire()))
        object.__setattr__(self, '_reference', reference)

    @property
    def reference_genome(self):
        reference = getattr(self, '_reference', None)
        if reference is None:
            # Wrappers imported from native handles have not resolved a Python
            # reference yet. Both the handle and resolved definition are immutable.
            from .reference_genome import ReferenceGenome
            reference = ReferenceGenome.from_wire(self._native_type.reference())
            object.__setattr__(self, '_reference', reference)
        return reference

    def __str__(self): return f"locus<{_type_identifier(self.reference_genome.name)}>"
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


@dataclass(frozen=True, init=False)
class Call:
    _alleles: tuple
    phased: bool = False

    def __init__(self, alleles, phased=False):
        alleles = tuple(alleles)
        if len(alleles) > 2 or any(type(a) is not int or a < 0 for a in alleles):
            raise ValueError("calls require zero, one, or two nonnegative integer alleles")
        if type(phased) is not bool:
            raise TypeError("phased must be bool")
        object.__setattr__(self, "_alleles", alleles if phased else tuple(sorted(alleles)))
        object.__setattr__(self, "phased", phased)
        _call_fields(self)

    @property
    def alleles(self):
        """Return the alleles as a list without exposing mutable hash state."""
        return list(self._alleles)

    def __repr__(self):
        return f"Call(alleles={self.alleles!r}, phased={self.phased!r})"

    @property
    def ploidy(self):
        return len(self._alleles)

    def __getitem__(self, i):
        return self.alleles[i]


tint32 = _PrimitiveType("int32")
tint64 = _PrimitiveType("int64")
tfloat32 = _PrimitiveType("float32")
tfloat64 = _PrimitiveType("float64")
tbool = _PrimitiveType("bool")
tstr = _PrimitiveType("str")
def _call_fields(value):
    if any(a > 65535 for a in value._alleles):
        raise ValueError("Call allele index must be between 0 and 65535")
    return {"__vail_call_allele0": value._alleles[0] if value._alleles else 0,
            "__vail_call_allele1": value._alleles[1] if len(value._alleles) == 2 else 0,
            "__vail_call_ploidy": len(value._alleles),
            "__vail_call_phased": value.phased}


def _call_from_fields(value):
    a, b, n, phased = (value[k] for k in ("__vail_call_allele0", "__vail_call_allele1",
                                        "__vail_call_ploidy", "__vail_call_phased"))
    if (type(a) is not int or type(b) is not int or type(n) is not int or type(phased) is not bool
            or not 0 <= a <= 65535 or not 0 <= b <= 65535 or not 0 <= n <= 2
            or (n == 0 and a != 0) or (n < 2 and b != 0)
            or (n == 2 and not phased and a > b)):
        raise ValueError("invalid Call fields")
    return Call((a, b)[:n], phased)


tcall = _PrimitiveType("call")
tint = tint32
tfloat = tfloat64


class tstruct(_NativeType, Mapping):
    def __init__(self, **fields):
        for name, dtype in fields.items():
            if not isinstance(name, str): raise TypeError("struct field names must be strings")
            if not is_type(dtype): raise TypeError(f"unsupported type for {name!r}: {dtype}")
        self._set_native(NativeType.construct('struct', [type_to_wire(t) for t in fields.values()], names=list(fields)))
    def __iter__(self): return iter(self._native_type.field_names)
    def __len__(self): return len(self._native_type.field_names)
    def __getitem__(self, name): return _wrap_native_type(self._native_type.field(name))
    def items(self):
        """Return an ordered snapshot without a separate field search per item."""
        return tuple(zip(self._native_type.field_names,
                         map(_wrap_native_type, self._native_type.children)))
    def __repr__(self): return "struct{" + ", ".join(f"{_type_identifier(k)}: {v}" for k, v in self.items()) + "}"




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
        if all(name.isidentifier() for name in self._fields):
            fields = ", ".join(f"{name}={value!r}" for name, value in self.items())
            return f"Struct({fields})"
        fields = ", ".join(f"{name!r}: {value!r}" for name, value in self.items())
        return f"Struct(**{{{fields}}})"

    def __eq__(self, other):
        return isinstance(other, Mapping) and dict(self) == dict(other)

    def __hash__(self):
        return 37 + hash(tuple(sorted(self._fields.items())))


_standard_pretty_printer = pprint.PrettyPrinter


class _StructPrettyPrinter(pprint.PrettyPrinter):
    def _format(self, obj, stream, indent, allowance, context, level, *args, **kwargs):
        if not isinstance(obj, Struct):
            return _standard_pretty_printer._format(
                self, obj, stream, indent, allowance, context, level, *args, **kwargs
            )

        representation = self._repr(obj, context, level)
        if len(representation) <= self._width - indent - allowance:
            stream.write(representation)
            return

        stream.write("Struct(")
        indent += len("Struct(")
        keyword_fields = all(name.isidentifier() for name in obj)
        if keyword_fields:
            fields = tuple(obj.items())
            for index, (name, value) in enumerate(fields):
                if index:
                    stream.write(" " * indent)
                stream.write(name + "=")
                self._format(value, stream, indent + len(name) + 1, allowance, context, level, *args, **kwargs)
                if index < len(fields) - 1:
                    stream.write(",\n")
        else:
            stream.write("**{")
            indent += len("**{")
            fields = tuple(obj.items())
            for index, (name, value) in enumerate(fields):
                if index:
                    stream.write(" " * indent)
                key = repr(name)
                stream.write(key + ": ")
                self._format(value, stream, indent + len(key) + 2, allowance, context, level, *args, **kwargs)
                if index < len(fields) - 1:
                    stream.write(",\n")
            stream.write("}")
        stream.write(")")


pprint.PrettyPrinter = _StructPrettyPrinter


def is_type(value):
    return isinstance(value, _NativeType)


def dtype(type_string):
    """Parse a Hail type string, or return an existing Vail type unchanged."""
    if is_type(type_string):
        return type_string
    if not isinstance(type_string, str):
        raise TypeError("dtype expects a type string or Vail type")
    parser = _TypeParser(type_string)
    result = parser.parse_type()
    parser.skip_space()
    if parser.position != len(type_string):
        parser.error("unexpected trailing input")
    return result


def _type_identifier(name):
    if name and all(char.isalnum() or char == "_" for char in name):
        return name
    # JSON supplies standard control/backslash escapes without changing raw
    # Unicode. Backticks delimit identifiers, so escape those independently.
    encoded = json.dumps(name, ensure_ascii=False)[1:-1].replace('`', r'\`')
    return f"`{encoded}`"


_IDENTIFIER_ESCAPES = re.compile(r"\\(?:N\{[^}]*\}|x.{0,2}|u.{0,4}|U.{0,8}|[0-7]{1,3}|.)", re.DOTALL)


class _TypeParser:
    """Recursive descent over the concrete types supported by Vail."""
    def __init__(self, text):
        self.text = text
        self.position = 0

    def error(self, message):
        raise ValueError(f"invalid type string at position {self.position}: {message}: {self.text!r}")

    def skip_space(self):
        while self.position < len(self.text) and self.text[self.position].isspace():
            self.position += 1

    def accept(self, token):
        self.skip_space()
        if self.text.startswith(token, self.position):
            self.position += len(token)
            return True
        return False

    def expect(self, token):
        if not self.accept(token):
            self.error(f"expected {token!r}")

    def identifier(self):
        self.skip_space()
        if self.accept("`"):
            start = self.position
            escaped = False
            while self.position < len(self.text):
                char = self.text[self.position]
                if char == "`" and not escaped:
                    encoded = self.text[start:self.position]
                    self.position += 1
                    # Decode only escape sequences, never UTF-8 bytes from
                    # ordinary Unicode characters in the identifier.
                    def decode_escape(match):
                        escape = match.group()
                        if escape == r"\`":
                            return "`"
                        if escape[1] not in "\\\"'abfnrtv01234567xuUN\n":
                            return escape
                        try:
                            return escape.encode('ascii').decode('unicode_escape')
                        except (UnicodeDecodeError, UnicodeEncodeError):
                            self.error("invalid escaped identifier")
                    return _IDENTIFIER_ESCAPES.sub(decode_escape, encoded)
                self.position += 1
                escaped = char == "\\" and not escaped
            self.error("unterminated escaped identifier")
        start = self.position
        while self.position < len(self.text):
            char = self.text[self.position]
            if not (char.isalnum() or char == "_"):
                break
            self.position += 1
        if start == self.position:
            self.error("expected identifier")
        return self.text[start:self.position]

    def parse_type(self):
        name = self.identifier()
        aliases = {
            "int": tint32, "int32": tint32, "int64": tint64,
            "float": tfloat64, "float32": tfloat32, "float64": tfloat64,
            "bool": tbool, "str": tstr, "call": tcall,
        }
        # Hail accepts both 'array' and 'tarray', and the analogous primitive names.
        if name.startswith("t") and name[1:] in (*aliases, "array", "set", "dict", "tuple", "struct", "interval", "locus", "ndarray"):
            name = name[1:]
        if name in aliases:
            return aliases[name]
        if name == "tuple":
            self.expect("(")
            elements = []
            if not self.accept(")"):
                elements.append(self.parse_type())
                while self.accept(","):
                    elements.append(self.parse_type())
                self.expect(")")
            return ttuple(*elements)
        if name == "struct":
            self.expect("{")
            fields = {}
            if not self.accept("}"):
                while True:
                    field = self.identifier()
                    if field in fields:
                        self.error(f"duplicate struct field {field!r}")
                    self.expect(":")
                    fields[field] = self.parse_type()
                    if not self.accept(","):
                        break
                self.expect("}")
            return tstruct(**fields)
        if name == "locus":
            self.expect("<")
            reference = self.identifier()
            self.expect(">")
            return tlocus(reference)
        if name in ("array", "set", "interval", "dict", "ndarray"):
            self.expect("<")
            element = self.parse_type()
            if name == "dict":
                self.expect(",")
                value = self.parse_type()
                self.expect(">")
                return tdict(element, value)
            if name == "ndarray":
                self.expect(",")
                self.skip_space()
                start = self.position
                while self.position < len(self.text) and self.text[self.position] in "0123456789":
                    self.position += 1
                if start == self.position:
                    self.error("expected nonnegative ndarray rank")
                rank = int(self.text[start:self.position])
                self.expect(">")
                return tndarray(element, rank)
            self.expect(">")
            return {"array": tarray, "set": tset, "interval": tinterval}[name](element)
        self.error(f"unknown or unsupported type {name!r}")


def type_to_wire(dtype):
    """Return native type memory; retained spelling for existing API constructors."""
    if isinstance(dtype, NativeType): return dtype
    if isinstance(dtype, _NativeType): return dtype._native_type
    raise TypeError(f"not a Vail type: {dtype!r}")


def _wrap_native_type(native):
    kind = native.kind
    primitives = {t.name: t for t in (tint32, tint64, tfloat32, tfloat64, tbool, tstr, tcall)}
    if kind in primitives: return primitives[kind]
    classes = {'array': tarray, 'set': tset, 'dict': tdict, 'tuple': ttuple,
               'struct': tstruct, 'interval': tinterval, 'ndarray': tndarray, 'locus': tlocus}
    result = object.__new__(classes[kind])
    result._set_native(native)
    return result


def type_from_wire(value):
    from ._native import NativeType
    if isinstance(value, NativeType):
        return _wrap_native_type(value)
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


def infer_type(value, *, _mapping_as_struct=False):
    from .expressions import Expression
    if isinstance(value, Expression):
        return value.dtype
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
    if isinstance(value, Struct) or _mapping_as_struct and isinstance(value, Mapping):
        if any(field is None for field in value.values()):
            raise ValueError("cannot infer a struct field type from a missing value; provide a type")
        return tstruct(**{name: infer_type(field, _mapping_as_struct=_mapping_as_struct)
                          for name, field in value.items()})
    if isinstance(value, Mapping):
        def infer_items(items, label):
            result = None
            for item in items:
                if item is None:
                    continue
                current = infer_type(item)
                result = current if result is None else unify_types(result, current)
                if result is None:
                    raise TypeError(f"dictionary {label} have incompatible types")
            if result is None:
                raise ValueError(f"cannot infer dictionary {label} from empty or missing values; provide a type")
            return result
        return tdict(infer_items(value.keys(), "keys"), infer_items(value.values(), "values"))
    if isinstance(value, (list, set, frozenset)):
        kind = 'array' if isinstance(value, list) else 'set'
        if not value:
            raise ValueError(f"cannot infer an {kind} element type from an empty {kind}")
        element = None
        for item in value:
            if item is None:
                continue
            inferred = infer_type(item, _mapping_as_struct=_mapping_as_struct)
            element = inferred if element is None else unify_types(element, inferred)
            if element is None:
                raise TypeError(f"{kind} elements have incompatible types")
        if element is None:
            raise ValueError(f"cannot infer an {kind} element type from only missing values")
        return tarray(element) if kind == 'array' else tset(element)
    if isinstance(value, tuple):
        return ttuple(*(infer_type(v, _mapping_as_struct=_mapping_as_struct) for v in value))
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
        fields = {name: unify_types(a, b) for (name, a), (_, b) in zip(left.items(), right.items())}
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
        return _call_from_fields(value)
    if isinstance(dtype, ttuple):
        return tuple(decode_value(value[f"__vail_tuple_{i}"], t) for i, t in enumerate(dtype.types))
    if isinstance(dtype, tdict):
        return {_decode_set_element(v["key"], dtype.key_type): decode_value(v["value"], dtype.value_type) for v in value}
    if isinstance(dtype, tndarray):
        import numpy as np
        data = [decode_value(v, dtype.element_type) for v in value["__vail_ndarray_data"]]
        return np.asarray(data, dtype=str(dtype.element_type)).reshape(value["__vail_ndarray_shape"])
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
        children = (ty for _, ty in dtype.items())
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
