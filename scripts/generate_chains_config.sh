#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
RPC_DIR="$ROOT_DIR/scripts/rpc"
OUTPUT="$ROOT_DIR/src/config/chains.json"

python3 - "$RPC_DIR" "$OUTPUT" <<'PY'
import json
import os
import re
import shlex
import sys
from pathlib import Path

rpc_dir = Path(sys.argv[1])
output = Path(sys.argv[2])

name_overrides = {
    "check_factom.sh": "Factom Accumulate",
    "check_klaytn.sh": "Kaia Legacy",
    "check_xtz.sh": "Tezos Legacy",
    "check_zel.sh": "Flux",
}

preferred_sources = {
    "nimiq": "check_nimiq.sh",
    "ontology": "check_ontology.sh",
}


def canonical_chain_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def parse_script(path: Path):
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line.startswith("test_rpc "):
            continue

        args = shlex.split(line)
        if len(args) != 7:
            raise ValueError(f"Unexpected test_rpc format in {path}: {line}")

        name = name_overrides.get(path.name, args[1])
        return {
            "name": name,
            "family": args[2],
            "ankr_slug": args[3],
            "alchemy_slug": args[4],
            "infura_slug": args[5],
            "public_rpc": args[6],
            "_source": path.name,
        }

    raise ValueError(f"No test_rpc invocation found in {path}")


def score(entry):
    return sum(
        1
        for field in ("ankr_slug", "alchemy_slug", "infura_slug", "public_rpc")
        if entry[field]
    )


entries_by_key = {}
for path in sorted(rpc_dir.glob("check_*.sh")):
    if path.name == "check_all.sh":
        continue

    entry = parse_script(path)
    key = canonical_chain_key(entry["name"])
    existing = entries_by_key.get(key)
    if existing is None:
        entries_by_key[key] = entry
        continue

    preferred = preferred_sources.get(key)
    if preferred == entry["_source"]:
        entries_by_key[key] = entry
        continue
    if preferred == existing["_source"]:
        continue

    if score(entry) > score(existing):
        entries_by_key[key] = entry
        continue

    if score(entry) == score(existing) and entry["_source"] > existing["_source"]:
        entries_by_key[key] = entry


rows = []
for key in sorted(entries_by_key):
    entry = entries_by_key[key]
    rows.append(
        {
            "name": entry["name"],
            "family": entry["family"],
            "ankr_slug": entry["ankr_slug"],
            "alchemy_slug": entry["alchemy_slug"],
            "infura_slug": entry["infura_slug"],
            "public_rpc": entry["public_rpc"],
        }
    )

output.write_text(json.dumps(rows, indent=2) + "\n")
print(f"Wrote {len(rows)} chains to {output}")
PY
