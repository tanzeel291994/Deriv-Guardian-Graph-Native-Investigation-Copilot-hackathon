"""Parse HI-Small_Patterns.txt into structured fraud ring data."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

from pipeline.config import RAW_DATA_DIR, TRANSFORMED_DIR, PATTERNS_FILE


_BEGIN_RE = re.compile(r"^BEGIN LAUNDERING ATTEMPT - ([^:]+)(?::\s*(.*))?$")
_END_RE = re.compile(r"^END LAUNDERING ATTEMPT")

_TX_FIELDS = [
    "timestamp", "from_bank", "from_account",
    "to_bank", "to_account",
    "amount_received", "recv_currency",
    "amount_paid", "pay_currency",
    "payment_format", "is_laundering",
]


def _parse_tx_line(line: str) -> dict | None:
    """Parse a CSV transaction line inside a pattern block."""
    parts = line.split(",")
    if len(parts) < 11:
        return None
    # Timestamp has a comma between date and time: "2022/09/01 00:06"
    # But the file is comma-separated and the timestamp is a single field
    # like "2022/09/01 00:06" — no internal comma because the format is
    # YYYY/MM/DD HH:MM (space-separated, not comma).
    # Actually looking at the raw data: "2022/09/01 00:06,021174,..."
    # So timestamp is field 0, and the rest follows.
    # But we have 11 fields total and parts may have exactly 11 elements.
    if len(parts) != 11:
        return None
    rec = dict(zip(_TX_FIELDS, parts))
    # Clean up
    rec["timestamp"] = rec["timestamp"].strip()
    rec["from_bank"] = rec["from_bank"].strip()
    rec["from_account"] = rec["from_account"].strip()
    rec["to_bank"] = rec["to_bank"].strip()
    rec["to_account"] = rec["to_account"].strip()
    rec["amount_received"] = float(rec["amount_received"])
    rec["amount_paid"] = float(rec["amount_paid"])
    rec["recv_currency"] = rec["recv_currency"].strip()
    rec["pay_currency"] = rec["pay_currency"].strip()
    rec["payment_format"] = rec["payment_format"].strip()
    rec["is_laundering"] = int(rec["is_laundering"])
    return rec


def _find_hub(transactions: list[dict]) -> str:
    """Find the most-connected account in a ring (hub)."""
    counter: Counter[str] = Counter()
    for tx in transactions:
        counter[tx["from_account"]] += 1
        counter[tx["to_account"]] += 1
    if not counter:
        return ""
    return counter.most_common(1)[0][0]


def parse_patterns(patterns_path: Path | None = None) -> list[dict]:
    """Parse Patterns.txt and return list of fraud ring dicts."""
    if patterns_path is None:
        patterns_path = RAW_DATA_DIR / PATTERNS_FILE

    rings: list[dict] = []
    current_ring: dict | None = None

    with open(patterns_path, "r") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            begin_match = _BEGIN_RE.match(line)
            if begin_match:
                pattern_type = begin_match.group(1).strip()
                description = (begin_match.group(2) or "").strip()
                current_ring = {
                    "ring_id": len(rings),
                    "pattern_type": pattern_type,
                    "description": description,
                    "transactions": [],
                    "accounts": set(),
                }
                continue

            if _END_RE.match(line):
                if current_ring is not None:
                    txs = current_ring["transactions"]
                    current_ring["hub_account"] = _find_hub(txs)
                    current_ring["accounts"] = sorted(current_ring["accounts"])
                    current_ring["num_transactions"] = len(txs)
                    if txs:
                        timestamps = [t["timestamp"] for t in txs]
                        current_ring["temporal_span"] = [
                            min(timestamps), max(timestamps)
                        ]
                    else:
                        current_ring["temporal_span"] = []
                    rings.append(current_ring)
                current_ring = None
                continue

            # Transaction line inside a ring
            if current_ring is not None:
                tx = _parse_tx_line(line)
                if tx:
                    current_ring["transactions"].append(tx)
                    current_ring["accounts"].add(tx["from_account"])
                    current_ring["accounts"].add(tx["to_account"])

    return rings


def save_fraud_rings(rings: list[dict], output_path: Path | None = None) -> Path:
    """Serialize fraud rings to JSON."""
    if output_path is None:
        output_path = TRANSFORMED_DIR / "fraud_rings.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert sets to lists for JSON serialization (already done in parse)
    with open(output_path, "w") as f:
        json.dump(rings, f, indent=2)
    return output_path


def load_fraud_rings(path: Path | None = None) -> list[dict]:
    """Load previously parsed fraud rings from JSON."""
    if path is None:
        path = TRANSFORMED_DIR / "fraud_rings.json"
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    rings = parse_patterns()
    out = save_fraud_rings(rings)
    print(f"Parsed {len(rings)} fraud rings → {out}")
    # Summary by type
    from collections import Counter as C
    types = C(r["pattern_type"] for r in rings)
    for t, n in types.most_common():
        print(f"  {t}: {n}")
