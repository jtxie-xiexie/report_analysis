#!/usr/bin/env python3
"""
Run Dolphin Script (.dos) through DolphinDB Python API.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import dolphindb as ddb


def parse_set_values(items: list[str]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --set value '{item}', expected key=value")
        key, raw = item.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            raise ValueError(f"Invalid --set value '{item}', empty key")
        values[key] = parse_scalar(raw)
    return values


def parse_scalar(raw: str) -> Any:
    text = raw.strip()
    lowered = text.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none"):
        return None

    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        pass

    if (text.startswith("[") and text.endswith("]")) or (
        text.startswith("{") and text.endswith("}")
    ):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    return text


def load_script(script_path: str | None, inline_eval: str | None) -> str:
    blocks: list[str] = []
    if script_path:
        script = Path(script_path).read_text(encoding="utf-8")
        blocks.append(script)
    if inline_eval:
        blocks.append(inline_eval)
    code = "\n\n".join(blocks).strip()
    if not code:
        raise ValueError("No code to run. Provide --script and/or --eval.")
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute Dolphin Script via Python API")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--script", help="Path to .dos file")
    parser.add_argument("--eval", help="Inline DolphinDB code snippet")
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Upload scalar/json params as variables before running script",
    )
    parser.add_argument("--ssl", action="store_true", help="Enable SSL connection")
    args = parser.parse_args()

    script = load_script(args.script, args.eval)
    variables = parse_set_values(args.set)

    session = ddb.session(ssl=args.ssl)
    session.connect(args.host, args.port, args.user, args.password)

    if variables:
        session.upload(variables)

    result = session.run(script)
    print(json.dumps(serialize_result(result), ensure_ascii=False, default=str, indent=2))
    return 0


def serialize_result(result: Any) -> Any:
    to_dict = getattr(result, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict(orient="records")
        except TypeError:
            return to_dict()
    to_list = getattr(result, "tolist", None)
    if callable(to_list):
        return to_list()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
