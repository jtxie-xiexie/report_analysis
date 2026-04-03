from __future__ import annotations

import json
import queue
import threading
import time
from pathlib import Path
from typing import Any

import gradio as gr

try:
    from .agent_main import (
        AGENT_NAME,
        RUNTIME_BASE_URL,
        RUNTIME_PASSWORD,
        RUNTIME_USER,
        run_task_session,
        setup_runtime,
    )
    from .contracts import DEFAULT_OUTPUT_ROOT, TaskInput
except ImportError:
    from agent_main import (
        AGENT_NAME,
        RUNTIME_BASE_URL,
        RUNTIME_PASSWORD,
        RUNTIME_USER,
        run_task_session,
        setup_runtime,
    )
    from contracts import DEFAULT_OUTPUT_ROOT, TaskInput

from agent_client import AgentHttpClient


def _load_json(path_text: str) -> str:
    path = Path(path_text)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _ui_state(
    summary_text: str = "",
    result_json: str = "",
    evaluation_json: str = "",
    code_text: str = "",
    logs: list[str] | None = None,
) -> tuple[str, str, str, str, str]:
    return (
        summary_text,
        result_json,
        evaluation_json,
        code_text,
        "\n".join(logs or []),
    )


def run_from_form(
    content: str,
    factor: str,
    db_name: str,
    tb_name: str,
    force_mr_choice: str,
    output_root: str,
) -> Any:
    logs: list[str] = []
    log_queue: queue.Queue[str] = queue.Queue()
    outcome: dict[str, Any] = {}

    def logger(message: str) -> None:
        log_queue.put(str(message))

    logs.append("[STATUS] Task received")
    yield _ui_state(logs=logs)

    force_mr: bool | None
    if force_mr_choice == "true":
        force_mr = True
    elif force_mr_choice == "false":
        force_mr = False
    else:
        force_mr = None

    task_payload: dict[str, Any] = {
        "content": content,
        "factor": factor,
        "dataSources": [],
        "forceMR": force_mr,
    }
    if db_name.strip() and tb_name.strip():
        task_payload["dataSources"] = [{"dbName": db_name.strip(), "tbName": tb_name.strip()}]
    if output_root.strip():
        task_payload["outputDir"] = str(Path(output_root.strip()) / factor)

    logs.append("[STATUS] Building task payload")
    yield _ui_state(logs=logs)

    task = TaskInput.from_json(task_payload, DEFAULT_OUTPUT_ROOT)

    def worker() -> None:
        try:
            log_queue.put("[STATUS] Connecting to Agent Runtime")
            client = AgentHttpClient(
                base_url=RUNTIME_BASE_URL,
                username=RUNTIME_USER,
                password=RUNTIME_PASSWORD,
            )
            log_queue.put("[STATUS] Registering runtime resources")
            ids = setup_runtime(client, AGENT_NAME, "Report Analysis Web User")
            log_queue.put("[STATUS] Running analysis session")
            outcome["run_result"] = run_task_session(
                client,
                task,
                ids,
                max_rounds=24,
                logger=logger,
            )
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = str(exc)
        finally:
            outcome["done"] = True

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    last_log_text = ""
    while not outcome.get("done") or not log_queue.empty():
        updated = False
        while True:
            try:
                logs.append(log_queue.get_nowait())
                updated = True
            except queue.Empty:
                break
        if updated:
            last_log_text = "\n".join(logs)
            yield _ui_state(logs=logs)
        time.sleep(0.2)

    while True:
        try:
            logs.append(log_queue.get_nowait())
        except queue.Empty:
            break

    if "error" in outcome:
        logs.append(f"\n[ERROR]\n{outcome['error']}")
        yield _ui_state(logs=logs)
        return

    run_result = outcome["run_result"]
    artifacts = run_result["artifacts"]
    result_json = _load_json(artifacts.get("resultFile", ""))
    evaluation_json = _load_json(artifacts.get("evaluationFile", ""))
    code_text = _load_json(artifacts.get("codeFile", ""))
    summary_text = run_result["summary"]
    artifact_text = json.dumps(artifacts, ensure_ascii=False, indent=2)
    logs.append("[STATUS] Completed")
    yield _ui_state(
        summary_text + "\n\nArtifacts:\n" + artifact_text,
        result_json,
        evaluation_json,
        code_text,
        logs,
    )


def build_app() -> gr.Blocks:
    with gr.Blocks(title="DolphinDB Report Analysis Agent") as app:
        gr.Markdown("# DolphinDB Report Analysis Agent")
        gr.Markdown("粘贴研报文本，填写因子名和数据源，页面会自动跑完整流程并返回 result。")

        with gr.Row():
            factor = gr.Textbox(label="Factor", value="Minute_Return_1")
            force_mr = gr.Dropdown(
                label="forceMR",
                choices=["null", "true", "false"],
                value="null",
            )

        with gr.Row():
            db_name = gr.Textbox(label="DB Name", value="dfs://stockMinKSH")
            tb_name = gr.Textbox(label="Table Name", value="stockMinKSH_v2")

        output_root = gr.Textbox(
            label="Output Root",
            value=str(DEFAULT_OUTPUT_ROOT),
        )
        content = gr.Textbox(
            label="Report Content",
            lines=12,
            value=(
                "请构造一个最简单的分钟频因子：1分钟收益率。"
                "使用分钟行情表中的 LastPx 作为价格字段，按照每个 SecurityID 的 DateTime 升序计算"
                "当前分钟相对上一分钟的收益率，即 LastPx / prev(LastPx) - 1。"
            ),
        )

        run_button = gr.Button("Run", variant="primary")

        summary = gr.Textbox(label="Summary", lines=10)
        result_json = gr.Code(label="result.json", language="json")
        evaluation_json = gr.Code(label="evaluation.json", language="json")
        code_text = gr.Code(label="Generated Code", language="python")
        logs = gr.Textbox(label="Logs", lines=24)

        run_button.click(
            run_from_form,
            inputs=[content, factor, db_name, tb_name, force_mr, output_root],
            outputs=[summary, result_json, evaluation_json, code_text, logs],
        )

    return app


def main() -> None:
    app = build_app()
    app.queue()
    app.launch(server_name="0.0.0.0", server_port=7861)


if __name__ == "__main__":
    main()
