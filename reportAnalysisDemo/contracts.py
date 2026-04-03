from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent / "outputs"


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{key}' must be a non-empty string")
    return value.strip()


def _optional_string(payload: dict[str, Any], key: str, default: str = "") -> str:
    value = payload.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ValueError(f"'{key}' must be a string when provided")
    return value


def sanitize_factor_name(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_-]+", "_", name.strip())
    return cleaned.strip("_") or "factor"


def target_filename(factor: str, execute_type: str) -> str:
    stem = sanitize_factor_name(factor)
    if execute_type == "mrExecute":
        return f"{stem}_mr.dos"
    return f"{stem}.dos"


@dataclass(slots=True)
class DataSource:
    dbName: str
    tbName: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DataSource":
        if not isinstance(payload, dict):
            raise ValueError("Each data source must be an object")
        return cls(
            dbName=_require_string(payload, "dbName"),
            tbName=_require_string(payload, "tbName"),
        )

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(slots=True)
class TaskInput:
    content: str
    factor: str
    dataSources: list[DataSource]
    forceMR: bool | None
    outputDir: Path

    @classmethod
    def from_json(
        cls,
        raw: str | dict[str, Any],
        default_output_root: Path | None = None,
    ) -> "TaskInput":
        default_root = default_output_root or DEFAULT_OUTPUT_ROOT
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(payload, dict):
            raise ValueError("Task input must be a JSON object")

        content = _require_string(payload, "content")
        factor = _require_string(payload, "factor")

        raw_sources = payload.get("dataSources", [])
        if raw_sources is None:
            raw_sources = []
        if not isinstance(raw_sources, list):
            raise ValueError("'dataSources' must be an array")
        data_sources = [DataSource.from_dict(item) for item in raw_sources]

        force_mr = payload.get("forceMR")
        if force_mr not in (True, False, None):
            raise ValueError("'forceMR' must be true, false, or null")

        raw_output_dir = payload.get("outputDir")
        if raw_output_dir is None:
            output_dir = default_root / sanitize_factor_name(factor)
        elif isinstance(raw_output_dir, str) and raw_output_dir.strip():
            output_dir = Path(raw_output_dir.strip())
        else:
            raise ValueError("'outputDir' must be a non-empty string when provided")

        return cls(
            content=content,
            factor=factor,
            dataSources=data_sources,
            forceMR=force_mr,
            outputDir=output_dir,
        )

    def to_prompt_payload(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "factor": self.factor,
            "dataSources": [source.to_dict() for source in self.dataSources],
            "forceMR": self.forceMR,
            "outputDir": str(self.outputDir),
        }


@dataclass(slots=True)
class PreResultPayload:
    description: str
    formulation: str
    factor_chinese_name: str = ""
    field_requirements: str = ""
    targetFreqGuess: str = ""
    isCSGuess: str = ""
    isSlideGuess: str = ""
    computeFreqGuess: str = ""
    windowSizeGuess: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PreResultPayload":
        if not isinstance(payload, dict):
            raise ValueError("pre-result args must be an object")
        return cls(
            description=_require_string(payload, "description"),
            formulation=_require_string(payload, "formulation"),
            factor_chinese_name=_optional_string(payload, "factor_chinese_name"),
            field_requirements=_optional_string(payload, "field_requirements"),
            targetFreqGuess=_optional_string(payload, "targetFreqGuess"),
            isCSGuess=_optional_string(payload, "isCSGuess"),
            isSlideGuess=_optional_string(payload, "isSlideGuess"),
            computeFreqGuess=_optional_string(payload, "computeFreqGuess"),
            windowSizeGuess=_optional_string(payload, "windowSizeGuess"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ResultPayload:
    factor_chinese_name: str
    hypothesis: str
    description: str
    formulation: str
    feedback: str
    code: str
    executeType: str
    hypothesis_feedback: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ResultPayload":
        if not isinstance(payload, dict):
            raise ValueError("result args must be an object")
        execute_type = _require_string(payload, "executeType")
        if execute_type not in {"execute", "mrExecute"}:
            raise ValueError("'executeType' must be either 'execute' or 'mrExecute'")
        return cls(
            factor_chinese_name=_require_string(payload, "factor_chinese_name"),
            hypothesis=_require_string(payload, "hypothesis"),
            description=_require_string(payload, "description"),
            formulation=_require_string(payload, "formulation"),
            feedback=_require_string(payload, "feedback"),
            code=_require_string(payload, "code"),
            executeType=execute_type,
            hypothesis_feedback=_require_string(payload, "hypothesis_feedback"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_placeholder_evaluation(
    task: TaskInput,
    execute_type: str,
    selected_source: dict[str, Any] | None,
    execution_meta: dict[str, Any] | None,
    tool_trace: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "placeholder",
        "factor": task.factor,
        "executeType": execute_type,
        "selectedDataSource": selected_source or {},
        "resultMeta": execution_meta or {},
        "toolTrace": tool_trace,
        "note": "v1 does not run a separate evaluation engine; this file preserves a stable artifact contract.",
    }
