#!/usr/bin/env python3
"""Persist generated DolphinDB code into a .dos file."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write DolphinDB code to a .dos file")
    parser.add_argument("--factor", required=True, help="Factor name")
    parser.add_argument(
        "--execute-type",
        required=True,
        choices=["execute", "mrExecute"],
        help="Execution path used for the final code",
    )
    parser.add_argument("--output-dir", default="outputs", help="Directory for .dos files")
    parser.add_argument("--source", required=True, help="Path to a text file containing code")
    return parser.parse_args()


def sanitize_factor_name(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_-]+", "_", name.strip())
    return cleaned.strip("_") or "factor"


def target_filename(factor: str, execute_type: str) -> str:
    stem = sanitize_factor_name(factor)
    if execute_type == "mrExecute":
        return f"{stem}_mr.dos"
    return f"{stem}.dos"


def main() -> int:
    args = parse_args()
    source = Path(args.source)
    code = source.read_text(encoding="utf-8")
    if not code.strip():
        raise SystemExit("Source code file is empty")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target = output_dir / target_filename(args.factor, args.execute_type)
    target.write_text(code, encoding="utf-8")
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
