"""Embed samply's saved symbol names and merge instruction-level function IDs.

Usage: python embed_symbols.py profile.json.gz profile.json.syms.json output.json.gz
Unknown addresses remain visible. This does not invent source lines or inline frames.
"""
import bisect
import gzip
import json
import sys


def embed(profile_path, symbols_path, output_path):
    with gzip.open(profile_path, "rt") as stream:
        profile = json.load(stream)
    with open(symbols_path) as stream:
        symbols = json.load(stream)
    tables = {}
    for library in symbols["data"]:
        rows = sorted(library["symbol_table"], key=lambda row: row["rva"])
        key = library["debug_name"], library["debug_id"].replace("-", "").lower()
        tables[key] = rows, [row["rva"] for row in rows]

    resolved = total = 0
    for thread in profile["threads"]:
        strings = thread["stringArray"]
        functions = thread["funcTable"]
        frames = thread["frameTable"]
        resources = thread["resourceTable"]
        native = thread["nativeSymbols"]
        canonical = {}
        # Save the raw names before changing any shared function-table entry.
        raw_names = [strings[index] for index in functions["name"]]
        for frame in range(frames["length"]):
            function = frames["func"][frame]
            resource = functions["resource"][function]
            raw_name = raw_names[function]
            if resource < 0 or not raw_name.startswith("0x"):
                continue
            total += 1
            lib_index = resources["lib"][resource]
            if lib_index is None:
                continue
            library = profile["libs"][lib_index]
            # Mach-O breakpad IDs append an age digit to the UUID.
            key = library["debugName"], library["breakpadId"][:-1].lower()
            table = tables.get(key)
            if table is None:
                continue
            rows, starts = table
            address = int(raw_name, 16)
            index = bisect.bisect_right(starts, address) - 1
            if index < 0:
                continue
            row = rows[index]
            if address >= row["rva"] + row["size"]:
                continue
            name = symbols["string_table"][row["symbol"]]
            if name == "UNKNOWN":
                continue
            symbol_key = lib_index, row["rva"]
            if symbol_key not in canonical:
                name_index = len(strings)
                strings.append(name)
                functions["name"][function] = name_index
                symbol_index = native["length"]
                native["length"] += 1
                native["address"].append(row["rva"])
                native["functionSize"].append(row["size"])
                native["libIndex"].append(lib_index)
                native["name"].append(name_index)
                canonical[symbol_key] = function, symbol_index
            frames["func"][frame], frames["nativeSymbol"][frame] = canonical[symbol_key]
            resolved += 1
    profile["meta"]["symbolicated"] = True
    with gzip.open(output_path, "wt") as stream:
        json.dump(profile, stream, separators=(",", ":"))
    print(json.dumps({"resolved_address_frames": resolved, "address_frames": total,
                      "output": output_path}))


if __name__ == "__main__":
    embed(*sys.argv[1:])
