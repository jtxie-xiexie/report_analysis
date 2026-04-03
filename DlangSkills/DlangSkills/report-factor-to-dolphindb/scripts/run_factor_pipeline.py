#!/usr/bin/env python3
"""Thin Python shell for the DolphinDB-driven factor pipeline."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import dolphindb as ddb

from normalize_input import load_json, normalize_from_json
from write_dos import target_filename


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DolphinDB factor pipeline and write artifacts")
    parser.add_argument("--host", default="192.168.100.43")
    parser.add_argument("--port", type=int, default=7301)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="123456")
    parser.add_argument("--task-json", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def serialize_result(result: Any) -> Any:
    if isinstance(result, dict):
        return {str(key): serialize_result(value) for key, value in result.items()}
    if isinstance(result, list):
        return [serialize_result(item) for item in result]
    if isinstance(result, tuple):
        return [serialize_result(item) for item in result]

    to_dict = getattr(result, "to_dict", None)
    if callable(to_dict):
        try:
            return serialize_result(to_dict(orient="records"))
        except TypeError:
            return serialize_result(to_dict())

    to_list = getattr(result, "tolist", None)
    if callable(to_list):
        return serialize_result(to_list())

    if isinstance(result, float) and math.isnan(result):
        return None
    return result


def core_script_path() -> Path:
    return Path(__file__).resolve().parent.parent / "dos" / "factor_pipeline_core.dos"


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = load_json(Path(args.task_json))
    normalized = normalize_from_json(payload, "generate_and_validate", str(output_dir))

    db_names = [item["dbName"] for item in normalized["dataSources"]]
    tb_names = [item["tbName"] for item in normalized["dataSources"]]

    session = ddb.session()
    session.connect(args.host, args.port, args.user, args.password)
    session.upload(
        {
            "pipelineContent": normalized["content"],
            "pipelineFactor": normalized["factor"],
            "pipelineDbNames": db_names,
            "pipelineTbNames": tb_names,
        }
    )

    force_mr = normalized["forceMR"]
    force_mr_expr = "NULL"
    if force_mr is True:
        force_mr_expr = "true"
    elif force_mr is False:
        force_mr_expr = "false"

    session.run(core_script_path().read_text(encoding="utf-8"))
    pipeline_result = serialize_result(
        session.run(
            f"runFactorPipeline(pipelineContent, pipelineFactor, {force_mr_expr}, pipelineDbNames, pipelineTbNames)"
        )
    )

    code = str(pipeline_result["code"])
    result_payload = pipeline_result["result"]
    evaluation_payload = pipeline_result["evaluation"]
    meta_payload = pipeline_result.get("meta")
    preview_payload = pipeline_result.get("preview")
    execute_type = str(result_payload.get("executeType", "execute"))

    final_code_path = output_dir / "final_code.txt"
    final_code_path.write_text(code, encoding="utf-8")

    dos_path = output_dir / target_filename(normalized["factor"], execute_type)
    dos_path.write_text(code, encoding="utf-8")

    result_path = output_dir / "result.json"
    result_path.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    evaluation_path = output_dir / "evaluation.json"
    evaluation_path.write_text(json.dumps(evaluation_payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    summary = {
        "normalizedInput": normalized,
        "executeType": execute_type,
        "codeFile": str(dos_path),
        "resultFile": str(result_path),
        "evaluationFile": str(evaluation_path),
        "meta": meta_payload,
        "preview": preview_payload,
    }
    summary_path = output_dir / "demo_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
