"""Coordinate-only reference genome definitions.

Sequence-backed operations are deliberately unavailable; a reference here is
only the immutable coordinate system used to type and compare loci.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from types import MappingProxyType


def _names(values, label):
    if isinstance(values, str):
        values = [values]
    result = tuple(values)
    if any(not isinstance(v, str) or not v for v in result) or len(set(result)) != len(result):
        raise ValueError(f"{label} must contain unique nonempty contig names")
    return result


@dataclass(frozen=True, eq=False)
class Locus:
    contig: str
    position: int
    reference_genome: "ReferenceGenome" = "default"

    def __post_init__(self):
        object.__setattr__(self, "reference_genome", resolve_reference(self.reference_genome))
        if isinstance(self.contig, int):
            object.__setattr__(self, "contig", str(self.contig))
        if not isinstance(self.contig, str):
            raise ValueError(f"contig {self.contig!r} is not in reference genome {self.reference_genome.name}")
        try:
            length = self.reference_genome.contig_length(self.contig)
        except KeyError:
            raise ValueError(f"contig {self.contig!r} is not in reference genome {self.reference_genome.name}") from None
        if type(self.position) is not int or not 1 <= self.position <= length:
            raise ValueError(f"position must be between 1 and {length} on {self.contig}")

    def __repr__(self):
        return f"Locus(contig={self.contig!r}, position={self.position}, reference_genome={self.reference_genome.name})"

    def __str__(self): return f"{self.contig}:{self.position}"

    def _compare(self, other, op):
        if not isinstance(other, Locus): return NotImplemented
        if self.reference_genome != other.reference_genome:
            raise TypeError("loci can only be compared when their reference genomes are equal")
        left = (self.reference_genome.contigs.index(self.contig), self.position)
        right = (other.reference_genome.contigs.index(other.contig), other.position)
        return op(left, right)

    def __eq__(self, other):
        from operator import eq
        return self._compare(other, eq)
    def __ne__(self, other):
        from operator import ne
        return self._compare(other, ne)
    def __lt__(self, other):
        from operator import lt
        return self._compare(other, lt)
    def __le__(self, other):
        from operator import le
        return self._compare(other, le)
    def __gt__(self, other):
        from operator import gt
        return self._compare(other, gt)
    def __ge__(self, other):
        from operator import ge
        return self._compare(other, ge)
    def __hash__(self): return hash((self.reference_genome, self.contig, self.position))

    @property
    def contig_idx(self): return self.reference_genome.contigs.index(self.contig)
    @property
    def is_x(self): return self.contig in self.reference_genome.x_contigs
    @property
    def is_y(self): return self.contig in self.reference_genome.y_contigs
    @property
    def is_mitochondrial(self): return self.contig in self.reference_genome.mt_contigs
    @property
    def in_par(self): return any(c == self.contig and s <= self.position < e for c, s, e in self.reference_genome._par)

    def _sequence_unavailable(self, *_args, **_kwargs):
        raise NotImplementedError("sequence operations require a FASTA-backed reference genome")

    sequence_context = _sequence_unavailable
    sequence = _sequence_unavailable
    get_sequence = _sequence_unavailable
    __getitem__ = _sequence_unavailable


class ReferenceGenome:
    """An immutable coordinate definition, registered by its unique name."""
    __slots__ = ("_name", "_contigs", "_lengths", "_x", "_y", "_mt", "_par", "_hash", "_sealed")

    def __init__(self, name, contigs, lengths, x_contigs=(), y_contigs=(), mt_contigs=(), par=()):
        if not isinstance(name, str) or not name:
            raise ValueError("reference genome name must be a nonempty string")
        contigs = _names(contigs, "contigs")
        if not contigs:
            raise ValueError("contigs must be nonempty")
        if not isinstance(lengths, Mapping) or set(lengths) != set(contigs):
            raise ValueError("lengths must define exactly the declared contigs")
        lengths = {c: lengths[c] for c in contigs}
        if any(type(n) is not int or n <= 0 or n > 2**31 - 1 for n in lengths.values()):
            raise ValueError("contig lengths must be positive integers that fit int32 locus positions")
        groups = [_names(v, label) for v, label in ((x_contigs, "x_contigs"), (y_contigs, "y_contigs"), (mt_contigs, "mt_contigs"))]
        if any(c not in lengths for group in groups for c in group):
            raise ValueError("sex chromosome and mitochondrial contigs must be declared")
        if set(groups[0]) & set(groups[1]) or set(groups[0]) & set(groups[2]) or set(groups[1]) & set(groups[2]):
            raise ValueError("a contig cannot have more than one chromosome classification")
        par_values = []
        par_references = []
        for interval in par:
            if isinstance(interval, Mapping):
                c, start, end = interval["contig"], interval["start"], interval["end"]
            elif hasattr(interval, "start") and hasattr(interval, "end"):
                start_locus, end_locus = interval.start, interval.end
                if not isinstance(start_locus, Locus) or not isinstance(end_locus, Locus):
                    raise ValueError("PAR intervals must have Locus endpoints")
                if start_locus.contig != end_locus.contig:
                    raise ValueError("PAR interval endpoints must be on the same contig")
                if interval.includes_start is not True or interval.includes_end is not False:
                    raise ValueError("PAR intervals must be half-open [start, end)")
                c, start, end = start_locus.contig, start_locus.position, end_locus.position
                par_references.extend((start_locus.reference_genome, end_locus.reference_genome))
            else:
                c, start, end = interval
            if c not in lengths or c not in groups[0] + groups[1] or type(start) is not int or type(end) is not int or not 1 <= start < end <= lengths[c]:
                raise ValueError(f"invalid PAR interval {(c, start, end)!r}")
            par_values.append((c, start, end))
        if len(set(par_values)) != len(par_values):
            raise ValueError("PAR intervals must be unique")
        expected_par = [{"contig": c, "start": s, "end": e} for c, s, e in par_values]
        for reference in par_references:
            definition = reference.to_wire()
            if (definition["name"] != name or definition["contigs"] != list(contigs)
                    or definition["lengths"] != lengths or definition["x_contigs"] != list(groups[0])
                    or definition["y_contigs"] != list(groups[1]) or definition["mt_contigs"] != list(groups[2])
                    or definition["par"] != expected_par):
                raise ValueError("PAR interval endpoints must use this reference genome coordinate definition")
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_contigs", contigs)
        object.__setattr__(self, "_lengths", MappingProxyType(lengths))
        object.__setattr__(self, "_x", groups[0])
        object.__setattr__(self, "_y", groups[1])
        object.__setattr__(self, "_mt", groups[2])
        object.__setattr__(self, "_par", tuple(par_values))
        object.__setattr__(self, "_hash", hash((name, contigs, tuple(lengths.items()), *groups, tuple(par_values))))
        existing = _registry.get(name)
        if existing is not None and existing != self:
            raise ValueError(f"reference genome {name!r} is already registered with a different coordinate definition")
        _registry[name] = self
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False):
            raise TypeError("ReferenceGenome is immutable")
        object.__setattr__(self, name, value)

    @property
    def name(self): return self._name
    @property
    def contigs(self): return self._contigs
    @property
    def lengths(self): return self._lengths.copy()
    @property
    def x_contigs(self): return self._x
    @property
    def y_contigs(self): return self._y
    @property
    def mt_contigs(self): return self._mt
    @property
    def par(self):
        from .types import Interval, tlocus
        return [Interval(Locus(c, start, self), Locus(c, end, self), True, False, tlocus(self))
                for c, start, end in self._par]

    def contig_length(self, contig):
        try: return self._lengths[contig]
        except KeyError: raise KeyError(f"Contig {contig!r} is not in reference genome {self.name}.") from None

    def locus(self, contig, position): return Locus(contig, position, self)
    def is_x_contig(self, contig): return contig in self._x
    def is_y_contig(self, contig): return contig in self._y
    def is_mt_contig(self, contig): return contig in self._mt
    def is_autosomal(self, contig): return contig not in self._x and contig not in self._y and contig not in self._mt

    def to_wire(self):
        return {"name": self.name, "contigs": list(self.contigs), "lengths": self.lengths,
                "x_contigs": list(self.x_contigs), "y_contigs": list(self.y_contigs),
                "mt_contigs": list(self.mt_contigs),
                "par": [{"contig": c, "start": s, "end": e} for c, s, e in self._par]}

    @classmethod
    def read(cls, path):
        """Load a coordinate reference from Hail's public JSON definition."""
        from .vcf import _local_path
        with open(_local_path(path), encoding="utf-8") as stream:
            config = json.load(stream)
        if "lengths" in config:
            return cls.from_wire(config)
        contigs = config["contigs"]
        par = []
        for interval in config.get("par", []):
            if interval["start"]["contig"] != interval["end"]["contig"]:
                raise ValueError("PAR interval endpoints must be on the same contig")
            par.append(dict(contig=interval["start"]["contig"],
                            start=interval["start"]["position"],
                            end=interval["end"]["position"]))
        return cls.from_wire(dict(name=config["name"], contigs=[c["name"] for c in contigs],
                                  lengths={c["name"]: c["length"] for c in contigs},
                                  x_contigs=config.get("xContigs", []),
                                  y_contigs=config.get("yContigs", []),
                                  mt_contigs=config.get("mtContigs", []), par=par))

    @classmethod
    def from_wire(cls, definition):
        if not isinstance(definition, Mapping) or set(definition) != {"name", "contigs", "lengths", "x_contigs", "y_contigs", "mt_contigs", "par"}:
            raise ValueError("invalid reference genome definition")
        existing = _registry.get(definition["name"])
        if existing is not None:
            if existing.to_wire() != dict(definition):
                raise ValueError(f"reference genome {definition['name']!r} conflicts with its registered coordinate definition")
            return existing
        return cls(**definition)

    def __eq__(self, other):
        return isinstance(other, ReferenceGenome) and self.to_wire() == other.to_wire()
    def __hash__(self): return self._hash
    def __str__(self): return self.name
    def __repr__(self): return f"ReferenceGenome({self.name!r})"

    def add_sequence(self, *_args, **_kwargs): raise NotImplementedError("FASTA-backed sequence support is not implemented")
    def get_sequence(self, *_args, **_kwargs): raise NotImplementedError("FASTA-backed sequence support is not implemented")
    def sequence_context(self, *_args, **_kwargs): raise NotImplementedError("FASTA-backed sequence support is not implemented")
    def liftover(self, *_args, **_kwargs): raise NotImplementedError("liftover is not implemented")
    @classmethod
    def from_fasta_file(cls, *_args, **_kwargs): raise NotImplementedError("FASTA-backed reference construction is not implemented")
    from_fasta = from_fasta_file

    def has_sequence(self): return False


_registry = {}
_default = None


def _load_builtin(filename):
    path = Path(__file__).with_name("reference_data") / filename
    with path.open(encoding="utf-8") as f:
        config = json.load(f)
    contigs = config["contigs"]
    par = [(p["start"]["contig"], p["start"]["position"], p["end"]["position"]) for p in config["par"]]
    return ReferenceGenome(config["name"], [c["name"] for c in contigs],
                           {c["name"]: c["length"] for c in contigs},
                           config["xContigs"], config["yContigs"], config["mtContigs"], par)


GRCh37 = _load_builtin("grch37.json")
GRCh38 = _load_builtin("grch38.json")
_default = GRCh37


def get_reference(name):
    if name == "default" and _default is not None:
        return _default
    try: return _registry[name]
    except KeyError: raise KeyError(f"unknown reference genome {name!r}") from None


def resolve_reference(reference_genome):
    """Resolve a ReferenceGenome or registered name without changing default."""
    if reference_genome == "default":
        if _default is None: raise ValueError("no default reference genome has been set")
        return _default
    if isinstance(reference_genome, str): return get_reference(reference_genome)
    if isinstance(reference_genome, ReferenceGenome): return reference_genome
    raise TypeError("reference genome must be a ReferenceGenome or registered name")


def default_reference(reference_genome=None):
    global _default
    if reference_genome is None:
        if _default is None: raise ValueError("no default reference genome has been set")
        return _default
    _default = resolve_reference(reference_genome)
    return _default


def reference_genomes(): return dict(_registry)
