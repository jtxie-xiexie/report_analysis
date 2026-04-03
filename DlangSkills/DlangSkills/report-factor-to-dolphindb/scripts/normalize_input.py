#!/usr/bin/env python3
"""Normalize report-factor requests from PDF or JSON into a stable task JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

VALID_MODES = {"generate_only", "generate_and_validate"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize research-report factor input into a task JSON."
    )
    parser.add_argument("--pdf", help="Path to the report PDF")
    parser.add_argument("--json", dest="json_path", help="Path to the input JSON file")
    parser.add_argument("--factor", help="Factor name, required when using --pdf")
    parser.add_argument(
        "--mode",
        default="generate_only",
        help="Execution mode: generate_only or generate_and_validate",
    )
    parser.add_argument(
        "--output-path",
        default="outputs",
        help="Directory for generated .dos files",
    )
    return parser.parse_args()


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("pypdf is required for PDF normalization. Install dependencies from environment.yml.") from exc

    reader = PdfReader(str(pdf_path))
    chunks: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            chunks.append(text.strip())
    return "\n\n".join(chunks).strip()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def validate_mode(mode: str) -> str:
    if mode not in VALID_MODES:
        raise ValueError(f"Invalid mode '{mode}'. Expected one of {sorted(VALID_MODES)}")
    return mode


def normalize_force_mr(value: Any) -> Any:
    if value in (True, False, None):
        return value
    raise ValueError("forceMR must be true, false, or null")


def normalize_data_sources(value: Any) -> tuple[list[dict[str, str]], bool]:
    if value is None:
        return [], True
    if not isinstance(value, list):
        raise ValueError("dataSources must be a list when provided")
    if not value:
        return [], True

    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("each data source must be an object")
        db_name = item.get("dbName")
        tb_name = item.get("tbName")
        if not db_name or not tb_name:
            raise ValueError("each data source must contain dbName and tbName")
        normalized.append({"dbName": str(db_name), "tbName": str(tb_name)})
    return normalized, False


def normalize_from_json(payload: dict[str, Any], mode: str, output_path: str) -> dict[str, Any]:
    content = payload.get("content", "")
    factor = payload.get("factor", "")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("content is required and must be a non-empty string")
    if not isinstance(factor, str) or not factor.strip():
        raise ValueError("factor is required and must be a non-empty string")

    data_sources, needs_discovery = normalize_data_sources(payload.get("dataSources"))

    return {
        "content": content.strip(),
        "factor": factor.strip(),
        "dataSources": data_sources,
        "forceMR": normalize_force_mr(payload.get("forceMR")),
        "mode": validate_mode(payload.get("mode", mode)),
        "outputPath": payload.get("outputPath", output_path),
        "needsDiscovery": needs_discovery,
        "sourceType": "json",
    }


def normalize_from_pdf(pdf_path: Path, factor: str, mode: str, output_path: str) -> dict[str, Any]:
    if not factor or not factor.strip():
        raise ValueError("factor is required when using --pdf")
    content = extract_pdf_text(pdf_path)
    if not content:
        raise ValueError(f"No extractable text found in PDF: {pdf_path}")
    return {
        "content": content,
        "factor": factor.strip(),
        "dataSources": [],
        "forceMR": None,
        "mode": validate_mode(mode),
        "outputPath": output_path,
        "needsDiscovery": True,
        "sourceType": "pdf",
        "pdfPath": str(pdf_path),
    }


def main() -> int:
    args = parse_args()

    if bool(args.pdf) == bool(args.json_path):
        raise SystemExit("Provide exactly one of --pdf or --json")

    if args.json_path:
        normalized = normalize_from_json(
            load_json(Path(args.json_path)), args.mode, args.output_path
        )
    else:
        normalized = normalize_from_pdf(
            Path(args.pdf), args.factor or "", args.mode, args.output_path
        )

    print(json.dumps(normalized, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
