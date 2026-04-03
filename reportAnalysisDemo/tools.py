from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import dolphindb as ddb

try:
    from .contracts import (
        PreResultPayload,
        ResultPayload,
        TaskInput,
        build_placeholder_evaluation,
        target_filename,
    )
except ImportError:
    from contracts import (
        PreResultPayload,
        ResultPayload,
        TaskInput,
        build_placeholder_evaluation,
        target_filename,
    )


DDB_HOST = os.environ.get("REPORT_ANALYSIS_DDB_HOST", "192.168.100.43")
DDB_PORT = int(os.environ.get("REPORT_ANALYSIS_DDB_PORT", "7301"))
DDB_USER = os.environ.get("REPORT_ANALYSIS_DDB_USER", "admin")
DDB_PASSWORD = os.environ.get("REPORT_ANALYSIS_DDB_PASSWORD", "123456")

DOLPHINX_HOST = os.environ.get("REPORT_ANALYSIS_DOLPHINX_HOST", DDB_HOST)
DOLPHINX_PORT = int(os.environ.get("REPORT_ANALYSIS_DOLPHINX_PORT", str(DDB_PORT)))
DOLPHINX_USER = os.environ.get("REPORT_ANALYSIS_DOLPHINX_USER", DDB_USER)
DOLPHINX_PASSWORD = os.environ.get("REPORT_ANALYSIS_DOLPHINX_PASSWORD", DDB_PASSWORD)

RESULT_COLUMNS = ["tradeTime", "securityId", "factorname", "value"]
VALID_FREQS = {
    "NANOTIME",
    "NANOTIMESTAMP",
    "TIMESTAMP",
    "SECOND",
    "DATETIME",
    "MINUTE",
    "TIME",
    "HOUR",
    "DAY",
    "DATE",
    "MONTH",
    "YEAR",
}
FREQ_ALIASES = {
    "纳秒": "NANOTIMESTAMP",
    "纳秒频": "NANOTIMESTAMP",
    "秒": "SECOND",
    "秒频": "SECOND",
    "datetime": "DATETIME",
    "分钟": "MINUTE",
    "分钟频": "MINUTE",
    "min": "MINUTE",
    "1min": "MINUTE",
    "hour": "HOUR",
    "小时": "HOUR",
    "小时频": "HOUR",
    "日": "DAY",
    "日频": "DAY",
    "date": "DATE",
    "月": "MONTH",
    "月频": "MONTH",
    "year": "YEAR",
    "年": "YEAR",
    "年频": "YEAR",
}

CORE_DOS_PATH = (
    Path(__file__).resolve().parent.parent
    / "DlangSkills"
    / "DlangSkills"
    / "report-factor-to-dolphindb"
    / "dos"
    / "factor_pipeline_core.dos"
)


def build_tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "pre-result",
                "description": "Extract the factor explanation and formula from the report before any data inspection.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "description": {"type": "string"},
                        "formulation": {"type": "string"},
                        "factor_chinese_name": {"type": "string"},
                        "field_requirements": {"type": "string"},
                        "targetFreqGuess": {"type": "string"},
                        "isCSGuess": {"type": "string"},
                        "isSlideGuess": {"type": "string"},
                        "computeFreqGuess": {"type": "string"},
                        "windowSizeGuess": {"type": "string"},
                    },
                    "required": ["description", "formulation"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "getAllAvailableData",
                "description": "Discover candidate DolphinDB data sources when dataSources is empty.",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "coldefs",
                "description": "Inspect the schema of a DolphinDB table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "dbname": {"type": "string"},
                        "tbname": {"type": "string"},
                    },
                    "required": ["dbname", "tbname"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "testsql",
                "description": "Inspect sample rows from a DolphinDB table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "dbname": {"type": "string"},
                        "tbname": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["dbname", "tbname"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "mrEligible",
                "description": "Check whether a factor can run with the DolphinDB MR execution path.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "dbName": {"type": "string"},
                        "tbName": {"type": "string"},
                        "targetFreq": {"type": "string"},
                        "isCS": {"type": "boolean"},
                        "isSlide": {"type": "boolean"},
                        "computeFreq": {"type": ["string", "null"]},
                        "windowSize": {"type": ["integer", "null"]},
                        "forceMR": {"type": ["boolean", "null"]},
                    },
                    "required": ["dbName", "tbName", "targetFreq", "isCS", "isSlide"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "mrExecute",
                "description": "Execute factor code through the MR path.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "mapFuncString": {"type": "string"},
                        "dbName": {"type": "string"},
                        "tbName": {"type": "string"},
                    },
                    "required": ["mapFuncString", "dbName", "tbName"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "execute",
                "description": "Execute a plain DolphinDB script that produces a result table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                    },
                    "required": ["code"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "result",
                "description": "Persist the final factor analysis result and artifacts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "factor_chinese_name": {"type": "string"},
                        "hypothesis": {"type": "string"},
                        "description": {"type": "string"},
                        "formulation": {"type": "string"},
                        "feedback": {"type": "string"},
                        "code": {"type": "string"},
                        "executeType": {"type": "string"},
                        "hypothesis_feedback": {"type": "string"},
                    },
                    "required": [
                        "factor_chinese_name",
                        "hypothesis",
                        "description",
                        "formulation",
                        "feedback",
                        "code",
                        "executeType",
                        "hypothesis_feedback",
                    ],
                },
            },
        },
    ]


def _ddb_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


def _normalize_freq(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    mapped = FREQ_ALIASES.get(normalized, FREQ_ALIASES.get(normalized.lower(), normalized.upper()))
    if mapped not in VALID_FREQS:
        raise ValueError(
            f"'{field_name}' must be one of {sorted(VALID_FREQS)}, got {value!r}"
        )
    return mapped


def _serialize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return None if math.isnan(value) else value

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _serialize_value(to_dict(orient="records"))
        except TypeError:
            return _serialize_value(to_dict())

    to_list = getattr(value, "tolist", None)
    if callable(to_list):
        return _serialize_value(to_list())

    columns = getattr(value, "columns", None)
    head = getattr(value, "head", None)
    if columns is not None and callable(head):
        preview = head(10)
        return {
            "columns": [str(item) for item in columns],
            "rows": _serialize_value(preview.to_dict(orient="records")),
            "rowCount": int(len(value)),
        }

    return str(value)


@dataclass(slots=True)
class ExecutionSnapshot:
    executeType: str
    code: str
    columns: list[str]
    rowCount: int
    preview: list[dict[str, Any]]
    selectedDataSource: dict[str, str] | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReportAnalysisToolExecutor:
    def __init__(self, task: TaskInput, logger=print):
        self.task = task
        self.logger = logger
        self.session: Any | None = None
        self.pre_result: PreResultPayload | None = None
        self.final_result: ResultPayload | None = None
        self.last_execution: ExecutionSnapshot | None = None
        self.selected_source: dict[str, str] | None = None
        self.tool_trace: list[dict[str, Any]] = []
        self.artifacts: dict[str, str] = {}
        self._core_loaded = False

    def _log_timing(self, tag: str, start: float) -> None:
        self.logger(f"[TIMING][{tag}] {time.perf_counter() - start:.2f}s")

    def connect(self) -> None:
        if self.session is not None:
            return
        start = time.perf_counter()
        session = ddb.session()
        session.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASSWORD)
        self.session = session
        self._log_timing("DDB_CONNECT", start)

    def close(self) -> None:
        if self.session is not None:
            self.session.close()
            self.session = None

    def execute_tool_call(self, name: str, arguments: str) -> tuple[str, bool]:
        try:
            parsed_args = json.loads(arguments) if arguments else {}
        except json.JSONDecodeError as exc:
            payload = {"status": "error", "message": f"Tool arguments are not valid JSON: {exc}"}
            self._record_trace(name, False, payload)
            return json.dumps(payload, ensure_ascii=False), False

        try:
            if name == "pre-result":
                payload, terminal = self._handle_pre_result(parsed_args), False
            elif name == "getAllAvailableData":
                payload, terminal = self._handle_get_all_available_data(), False
            elif name == "coldefs":
                payload, terminal = self._handle_coldefs(parsed_args), False
            elif name == "testsql":
                payload, terminal = self._handle_testsql(parsed_args), False
            elif name == "mrEligible":
                payload, terminal = self._handle_mr_eligible(parsed_args), False
            elif name == "mrExecute":
                payload, terminal = self._handle_mr_execute(parsed_args), False
            elif name == "execute":
                payload, terminal = self._handle_execute(parsed_args), False
            elif name == "result":
                payload, terminal = self._handle_result(parsed_args), True
            else:
                payload, terminal = {"status": "error", "message": f"Unknown tool: {name}"}, False
        except Exception as exc:  # noqa: BLE001
            payload = {"status": "error", "message": str(exc)}
            terminal = False

        success = payload.get("status") != "error"
        self._record_trace(name, success, payload)
        if not success:
            terminal = False
        return json.dumps(payload, ensure_ascii=False), terminal

    def final_summary(self) -> str:
        if self.final_result is None:
            raise RuntimeError("The run did not finish with a final result.")
        return (
            f"Task completed: {self.task.factor}\n"
            f"Execute type: {self.final_result.executeType}\n"
            f"Code file: {self.artifacts.get('codeFile', '')}\n"
            f"Result file: {self.artifacts.get('resultFile', '')}\n"
            f"Evaluation file: {self.artifacts.get('evaluationFile', '')}\n"
            f"Summary file: {self.artifacts.get('summaryFile', '')}\n"
            f"Hypothesis: {self.final_result.hypothesis[:120]}"
        )

    def _ensure_session(self) -> Any:
        self.connect()
        if self.session is None:
            raise RuntimeError("DolphinDB session was not created")
        return self.session

    def _ensure_core_dos_loaded(self) -> None:
        if self._core_loaded:
            return
        session = self._ensure_session()
        start = time.perf_counter()
        session.run(CORE_DOS_PATH.read_text(encoding="utf-8"))
        self._core_loaded = True
        self._log_timing("LOAD_FACTOR_PIPELINE_CORE", start)

    def _record_trace(self, tool: str, success: bool, payload: dict[str, Any]) -> None:
        summary = payload.get("message") or payload.get("status") or ""
        self.tool_trace.append(
            {
                "tool": tool,
                "success": success,
                "summary": str(summary),
            }
        )

    def _handle_pre_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        pre_result = PreResultPayload.from_dict(payload)
        self.pre_result = pre_result
        return {
            "status": "ok",
            "message": "pre-result saved",
            "saved": pre_result.to_dict(),
        }

    def _handle_get_all_available_data(self) -> dict[str, Any]:
        session = self._ensure_session()
        discovered = session.run("starfish::facplfBasic::facplf_get_all_tables_for_AI_task()")
        serialized = _serialize_value(discovered)
        return {
            "status": "ok",
            "message": "getAllAvailableData completed",
            "data": serialized,
        }

    def _handle_coldefs(self, payload: dict[str, Any]) -> dict[str, Any]:
        dbname = self._require_tool_string(payload, "dbname")
        tbname = self._require_tool_string(payload, "tbname")
        self.selected_source = {"dbName": dbname, "tbName": tbname}

        session = self._ensure_session()
        script = f"schema(loadTable({_ddb_literal(dbname)}, {_ddb_literal(tbname)}))['colDefs']"
        start = time.perf_counter()
        schema = session.run(script)
        self._log_timing(f"DDB_COLDEFS_{tbname}", start)
        serialized = _serialize_value(schema)
        return {
            "status": "ok",
            "message": "coldefs completed",
            "dbName": dbname,
            "tbName": tbname,
            "data": serialized,
        }

    def _handle_testsql(self, payload: dict[str, Any]) -> dict[str, Any]:
        dbname = self._require_tool_string(payload, "dbname")
        tbname = self._require_tool_string(payload, "tbname")
        limit = payload.get("limit", 10)
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("'limit' must be a positive integer")
        self.selected_source = {"dbName": dbname, "tbName": tbname}

        session = self._ensure_session()
        script = (
            f"select top {limit} * from loadTable({_ddb_literal(dbname)}, {_ddb_literal(tbname)})"
        )
        start = time.perf_counter()
        rows = session.run(script)
        self._log_timing(f"DDB_TESTSQL_{tbname}", start)
        serialized = _serialize_value(rows)
        return {
            "status": "ok",
            "message": "testsql completed",
            "dbName": dbname,
            "tbName": tbname,
            "data": serialized,
        }

    def _handle_mr_eligible(self, payload: dict[str, Any]) -> dict[str, Any]:
        db_name = self._require_tool_string(payload, "dbName")
        tb_name = self._require_tool_string(payload, "tbName")
        target_freq = _normalize_freq(
            self._require_tool_string(payload, "targetFreq"),
            "targetFreq",
        )
        is_cs = self._require_tool_bool(payload, "isCS")
        is_slide = self._require_tool_bool(payload, "isSlide")
        compute_freq = _normalize_freq(
            self._require_optional_tool_string(payload, "computeFreq"),
            "computeFreq",
        )
        window_size = self._require_optional_tool_int(payload, "windowSize")
        force_mr = self._require_optional_tool_bool(payload, "forceMR")
        if not is_slide:
            compute_freq = None
            window_size = None
        self.selected_source = {"dbName": db_name, "tbName": tb_name}

        session = self._ensure_session()
        use_start = time.perf_counter()
        session.run("use starfish::facplfRun")
        self._log_timing("DDB_USE_FACPLFRUN", use_start)
        script = (
            "starfish::facplfRun::MREligible("
            f"{_ddb_literal(db_name)}, "
            f"{_ddb_literal(tb_name)}, "
            f"{_ddb_literal(target_freq)}, "
            f"{_ddb_literal(is_cs)}, "
            f"{_ddb_literal(is_slide)}, "
            f"{_ddb_literal(compute_freq)}, "
            f"{_ddb_literal(window_size)}, "
            f"{_ddb_literal(force_mr)})"
        )
        start = time.perf_counter()
        outcome = session.run(script)
        self._log_timing(f"DDB_MR_ELIGIBLE_{tb_name}", start)
        serialized = _serialize_value(outcome)
        return {
            "status": "ok",
            "message": "mrEligible completed",
            "dbName": db_name,
            "tbName": tb_name,
            "data": serialized,
        }

    def _handle_mr_execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        code = self._require_tool_string(payload, "mapFuncString")
        db_name = self._require_tool_string(payload, "dbName")
        tb_name = self._require_tool_string(payload, "tbName")
        self.selected_source = {"dbName": db_name, "tbName": tb_name}

        session = self._ensure_session()
        use_start = time.perf_counter()
        session.run("use starfish::facplfRun")
        self._log_timing("DDB_USE_FACPLFRUN", use_start)
        script = (
            "starfish::facplfRun::facplf_mr_execute("
            f"{_ddb_literal(code)}, {_ddb_literal(db_name)}, {_ddb_literal(tb_name)})"
        )
        start = time.perf_counter()
        result = session.run(script)
        self._log_timing(f"DDB_MR_EXECUTE_{tb_name}", start)
        snapshot = self._capture_execution_result(result, "mrExecute", code)
        return {
            "status": "ok",
            "message": "mrExecute completed",
            "executeType": "mrExecute",
            "resultMeta": snapshot.to_dict(),
        }

    def _handle_execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        code = self._require_tool_string(payload, "code")
        session = self._ensure_session()
        wrapped_script = (
            "def reportAnalysisAgentRun() {\n"
            f"{code}\n"
            "return result\n"
            "}\n"
            "reportAnalysisAgentRun()"
        )
        start = time.perf_counter()
        result = session.run(wrapped_script)
        self._log_timing("DDB_EXECUTE_SCRIPT", start)
        snapshot = self._capture_execution_result(result, "execute", code)
        return {
            "status": "ok",
            "message": "execute completed",
            "executeType": "execute",
            "resultMeta": snapshot.to_dict(),
        }

    def _handle_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.last_execution is None:
            raise RuntimeError("result cannot be called before execute or mrExecute succeeds")

        result = ResultPayload.from_dict(payload)
        if result.executeType != self.last_execution.executeType:
            raise ValueError(
                "result.executeType must match the last successful execution "
                f"({self.last_execution.executeType})"
            )

        self.final_result = result
        self.task.outputDir.mkdir(parents=True, exist_ok=True)

        code_path = self.task.outputDir / target_filename(self.task.factor, result.executeType)
        result_path = self.task.outputDir / "result.json"
        evaluation_path = self.task.outputDir / "evaluation.json"
        summary_path = self.task.outputDir / "summary.json"

        code_path.write_text(result.code, encoding="utf-8")
        result_path.write_text(
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        evaluation_start = time.perf_counter()
        evaluation_payload = self._build_evaluation_payload(result)
        self._log_timing("BUILD_EVALUATION", evaluation_start)
        evaluation_path.write_text(
            json.dumps(evaluation_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        summary_payload = {
            "task": self.task.to_prompt_payload(),
            "preResult": self.pre_result.to_dict() if self.pre_result else {},
            "finalResult": result.to_dict(),
            "execution": self.last_execution.to_dict(),
            "artifacts": {
                "codeFile": str(code_path),
                "resultFile": str(result_path),
                "evaluationFile": str(evaluation_path),
            },
            "connections": {
                "dolphindb": {
                    "host": DDB_HOST,
                    "port": DDB_PORT,
                    "user": DDB_USER,
                },
                "dolphinx": {
                    "host": DOLPHINX_HOST,
                    "port": DOLPHINX_PORT,
                    "user": DOLPHINX_USER,
                },
            },
        }
        summary_path.write_text(
            json.dumps(summary_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self.artifacts = {
            "codeFile": str(code_path),
            "resultFile": str(result_path),
            "evaluationFile": str(evaluation_path),
            "summaryFile": str(summary_path),
        }
        return {
            "status": "ok",
            "message": "result persisted",
            "artifacts": self.artifacts,
        }

    def _build_evaluation_payload(self, result: ResultPayload) -> dict[str, Any]:
        try:
            return self._run_real_evaluation(result)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "message": str(exc),
                "fallback": build_placeholder_evaluation(
                    task=self.task,
                    execute_type=result.executeType,
                    selected_source=self.selected_source,
                    execution_meta=self.last_execution.to_dict() if self.last_execution else {},
                    tool_trace=self.tool_trace,
                ),
            }

    def _run_real_evaluation(self, result: ResultPayload) -> dict[str, Any]:
        if self.selected_source is None:
            raise RuntimeError("Cannot run evaluation without a selected factor data source")

        self._ensure_core_dos_loaded()
        session = self._ensure_session()
        use_start = time.perf_counter()
        session.run("use starfish::facplfRun")
        self._log_timing("DDB_USE_FACPLFRUN", use_start)

        source_list = self.task.dataSources
        has_daily_source = False
        for source in source_list:
            inspect = self._inspect_source(source.dbName, source.tbName)
            if self._infer_frequency(inspect) == "daily":
                has_daily_source = True
                break

        if has_daily_source and source_list:
            db_names_expr = "[" + ",".join(_ddb_literal(source.dbName) for source in source_list) + "]"
            tb_names_expr = "[" + ",".join(_ddb_literal(source.tbName) for source in source_list) + "]"
            discovered_expr = f"createSourceTable({db_names_expr}, {tb_names_expr})"
        else:
            discovered_expr = "getAllAvailableData()"

        selected_db = self.selected_source["dbName"]
        selected_tb = self.selected_source["tbName"]
        factor_code = _ddb_literal(result.code)
        execute_type = _ddb_literal(result.executeType)
        script = f"""
selectedSourceInfo = inspectSource({_ddb_literal(selected_db)}, {_ddb_literal(selected_tb)});
discoveredSources = {discovered_expr};
priceSource = choosePriceSource(selectedSourceInfo, discoveredSources);
generateAnalysisReport(
    {factor_code},
    {execute_type},
    string(priceSource["priceDbname"]),
    string(priceSource["priceTbname"]),
    string(priceSource["priceDate"]),
    string(priceSource["priceSymbol"]),
    string(priceSource["priceCol"]),
    {_ddb_literal(selected_db)},
    {_ddb_literal(selected_tb)}
)
"""
        start = time.perf_counter()
        evaluation = session.run(script)
        self._log_timing("DDB_GENERATE_ANALYSIS_REPORT", start)
        serialized = _serialize_value(evaluation)
        return {
            "status": "ok",
            "message": "generateAnalysisReport completed",
            "data": serialized,
        }

    def _inspect_source(self, db_name: str, tb_name: str) -> dict[str, Any]:
        self._ensure_core_dos_loaded()
        session = self._ensure_session()
        start = time.perf_counter()
        inspect = session.run(
            f"inspectSource({_ddb_literal(db_name)}, {_ddb_literal(tb_name)})"
        )
        self._log_timing(f"DDB_INSPECT_SOURCE_{tb_name}", start)
        serialized = _serialize_value(inspect)
        if not isinstance(serialized, dict):
            raise RuntimeError("inspectSource returned an unexpected payload")
        return serialized

    def _infer_frequency(self, inspect_payload: dict[str, Any]) -> str:
        time_field = str(inspect_payload.get("timeField", "") or "")
        if not time_field:
            return "other"

        col_defs = inspect_payload.get("colDefs", [])
        if isinstance(col_defs, list):
            for item in col_defs:
                if not isinstance(item, dict):
                    continue
                if str(item.get("name", "")).lower() == time_field.lower():
                    if str(item.get("typeString", "")).upper() == "DATE":
                        return "daily"

        tb_name = str(inspect_payload.get("tbName", "")).lower()
        if "min" in tb_name:
            return "minute"
        return "other"

    def _capture_execution_result(self, result: Any, execute_type: str, code: str) -> ExecutionSnapshot:
        columns = self._extract_columns(result)
        if columns != RESULT_COLUMNS:
            raise RuntimeError(
                "EXECUTE_ERROR: result table columns must be exactly "
                f"{RESULT_COLUMNS}, got {columns}"
            )

        preview_rows = self._extract_preview_rows(result)
        self._validate_factorname_values(preview_rows, result)
        row_count = self._extract_row_count(result)

        snapshot = ExecutionSnapshot(
            executeType=execute_type,
            code=code,
            columns=columns,
            rowCount=row_count,
            preview=preview_rows,
            selectedDataSource=self.selected_source,
        )
        self.last_execution = snapshot
        return snapshot

    def _extract_columns(self, result: Any) -> list[str]:
        columns = getattr(result, "columns", None)
        if columns is not None:
            return [str(item) for item in columns]
        serialized = _serialize_value(result)
        if isinstance(serialized, dict) and "columns" in serialized:
            return [str(item) for item in serialized["columns"]]
        raise RuntimeError("EXECUTE_ERROR: could not inspect result table columns")

    def _extract_preview_rows(self, result: Any, limit: int = 10) -> list[dict[str, Any]]:
        head = getattr(result, "head", None)
        to_dict = getattr(result, "to_dict", None)
        if callable(head) and callable(to_dict):
            return _serialize_value(head(limit).to_dict(orient="records"))
        serialized = _serialize_value(result)
        if isinstance(serialized, dict) and isinstance(serialized.get("rows"), list):
            return serialized["rows"][:limit]
        if isinstance(serialized, list):
            return serialized[:limit]
        raise RuntimeError("EXECUTE_ERROR: could not preview result rows")

    def _extract_row_count(self, result: Any) -> int:
        try:
            return int(len(result))
        except Exception:  # noqa: BLE001
            serialized = _serialize_value(result)
            if isinstance(serialized, dict) and "rowCount" in serialized:
                return int(serialized["rowCount"])
            if isinstance(serialized, list):
                return len(serialized)
            return 0

    def _validate_factorname_values(self, preview_rows: list[dict[str, Any]], result: Any) -> None:
        factor_values: set[str] = set()
        for row in preview_rows:
            value = row.get("factorname")
            if value is not None:
                factor_values.add(str(value))

        if not factor_values and hasattr(result, "__getitem__"):
            try:
                factor_series = result["factorname"]
                unique_values = getattr(factor_series, "dropna", lambda: factor_series)()
                tolist = getattr(unique_values, "tolist", None)
                if callable(tolist):
                    factor_values = {str(item) for item in tolist()[:10]}
            except Exception:  # noqa: BLE001
                factor_values = set()

        if factor_values and factor_values != {self.task.factor}:
            raise RuntimeError(
                "EXECUTE_ERROR: factorname values must equal the input factor "
                f"{self.task.factor}, got {sorted(factor_values)}"
            )

    def _require_tool_string(self, payload: dict[str, Any], key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"'{key}' must be a non-empty string")
        return value.strip()

    def _require_optional_tool_string(self, payload: dict[str, Any], key: str) -> str | None:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"'{key}' must be a string or null")
        stripped = value.strip()
        return stripped or None

    def _require_tool_bool(self, payload: dict[str, Any], key: str) -> bool:
        value = payload.get(key)
        if not isinstance(value, bool):
            raise ValueError(f"'{key}' must be a boolean")
        return value

    def _require_optional_tool_bool(self, payload: dict[str, Any], key: str) -> bool | None:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, bool):
            raise ValueError(f"'{key}' must be a boolean or null")
        return value

    def _require_optional_tool_int(self, payload: dict[str, Any], key: str) -> int | None:
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, int):
            raise ValueError(f"'{key}' must be an integer or null")
        return value

