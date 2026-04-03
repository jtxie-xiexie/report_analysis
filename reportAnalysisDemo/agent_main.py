from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from .contracts import DEFAULT_OUTPUT_ROOT, TaskInput
    from .prompt import SYSTEM_PROMPT, WORKFLOW_PROMPT, build_task_message
    from .tools import ReportAnalysisToolExecutor, build_tool_schemas
except ImportError:
    from contracts import DEFAULT_OUTPUT_ROOT, TaskInput
    from prompt import SYSTEM_PROMPT, WORKFLOW_PROMPT, build_task_message
    from tools import ReportAnalysisToolExecutor, build_tool_schemas


CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent
DEMO_DIR = REPO_ROOT / "demo"

if str(DEMO_DIR) not in sys.path:
    sys.path.insert(0, str(DEMO_DIR))

from agent_client import AgentHttpClient  # noqa: E402


AGENT_NAME = "Report Analysis Agent"
LLM_PROVIDER = "openai-compatible"
LLM_MODEL = "deepseek-v3.2"
LLM_ENDPOINT = "https://api.n1n.ai"
LLM_API_KEY = "sk-A7cbB9D6aDRYX8hmsMCQQj3L7YDn4172SFi3aIjtV865oQ0D"

RUNTIME_BASE_URL = "http://192.168.100.208:8985"
RUNTIME_USER = "admin"
RUNTIME_PASSWORD = "123456"

CONTEXT_CONFIG = {
    "maxContextTokens": 128000,
    "reservedForOutput": 4096,
    "maxHistoryMessages": 40,
    "includeMemory": False,
    "memoryTopK": 0,
    "memoryTokenBudget": 0,
    "includeSkills": False,
    "includeSkillCatalog": False,
    "summaryConfig": {
        "enabled": False,
    },
}

CONTINUE_MESSAGE = (
    "Continue with the next step. "
    "If the final result tool has not been called yet, follow the workflow exactly. "
    "Call exactly one function and output only a valid tool call."
)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DolphinDB report analysis agent task")
    parser.add_argument("--task-json", required=True, help="Path to the task JSON file")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Default output root when the task JSON omits outputDir",
    )
    parser.add_argument("--runtime-base-url", default=RUNTIME_BASE_URL)
    parser.add_argument("--runtime-user", default=RUNTIME_USER)
    parser.add_argument("--runtime-password", default=RUNTIME_PASSWORD)
    parser.add_argument("--agent-name", default=AGENT_NAME)
    parser.add_argument("--biz-user-name", default="Report Analysis Demo User")
    parser.add_argument("--max-rounds", type=int, default=24)
    return parser.parse_args()


def setup_runtime(client: AgentHttpClient, agent_name: str, biz_user_name: str) -> dict[str, str]:
    if not LLM_API_KEY:
        raise RuntimeError(
            "Missing REPORT_ANALYSIS_LLM_API_KEY or LLM_API_KEY. "
            "Set one of them before running the agent."
        )

    llm = client.register_llm_config(
        name="report-analysis-llm",
        provider=LLM_PROVIDER,
        model=LLM_MODEL,
        endpoint=LLM_ENDPOINT,
        api_key=LLM_API_KEY,
        default_params={
            "temperature": 0.2,
            "maxTokens": 4096,
        },
        contextWindow=128000,
    )
    llm_config_id = llm["llmConfigId"]

    agent = client.register_agent(
        agent_name=agent_name,
        agent_type="chat",
        system_prompt=f"{SYSTEM_PROMPT}\n\n{WORKFLOW_PROMPT}",
        llm_config_id=llm_config_id,
        context_config=CONTEXT_CONFIG,
        description="Task-oriented DolphinDB report analysis agent",
    )
    agent_id = agent["agentId"]
    client.activate_agent(agent_id)

    biz_user = client.register_bizuser(display_name=biz_user_name)
    biz_user_id = biz_user["bizUserId"]
    client.grant_bizuser_access(agent_id, biz_user_id)
    return {
        "agentId": agent_id,
        "bizUserId": biz_user_id,
        "llmConfigId": llm_config_id,
    }


def print_model_response(tag: str, response: dict[str, Any], logger=print) -> None:
    logger(f"\n[{tag}]")
    logger(json.dumps(response, ensure_ascii=False, indent=2, default=str))


def print_tool_result(round_index: int, tool_name: str, result_text: str, logger=print) -> None:
    logger(f"\n[TOOL_RESULT][Round {round_index}][{tool_name}]")
    logger(result_text)


def print_timing(tag: str, elapsed_seconds: float, logger=print) -> None:
    logger(f"[TIMING][{tag}] {elapsed_seconds:.2f}s")


def _default_logger(message: str) -> None:
    print(message)


def run_task_session(
    client: AgentHttpClient,
    task: TaskInput,
    ids: dict[str, str],
    max_rounds: int,
    logger=_default_logger,
) -> dict[str, Any]:
    session_create_start = time.perf_counter()
    session = client.create_session(ids["agentId"], ids["bizUserId"], title=task.factor)
    print_timing("SESSION_CREATE", time.perf_counter() - session_create_start, logger=logger)
    session_id = session["sessionId"]
    executor = ReportAnalysisToolExecutor(task, logger=logger)
    tools = build_tool_schemas()

    try:
        initial_chat_start = time.perf_counter()
        response = client.chat(
            session_id,
            build_task_message(task),
            stream=False,
            tools=tools,
        )
        print_timing("CHAT_INITIAL", time.perf_counter() - initial_chat_start, logger=logger)
        print_model_response("MODEL_RESPONSE_INITIAL", response, logger=logger)

        for round_index in range(1, max_rounds + 1):
            tool_calls = response.get("toolCalls") or []
            if not tool_calls:
                content = (response.get("content") or "").strip()
                if executor.final_result is not None:
                    break
                if not content:
                    raise RuntimeError("The model returned neither tool calls nor content.")
                continue_chat_start = time.perf_counter()
                response = client.chat(
                    session_id,
                    CONTINUE_MESSAGE,
                    stream=False,
                    tools=tools,
                )
                print_timing(
                    f"CHAT_CONTINUE_{round_index}",
                    time.perf_counter() - continue_chat_start,
                    logger=logger,
                )
                print_model_response(
                    f"MODEL_RESPONSE_CONTINUE_{round_index}",
                    response,
                    logger=logger,
                )
                continue

            tool_results: list[dict[str, Any]] = []
            terminal = False
            for tool_call in tool_calls:
                function_info = tool_call["function"]
                tool_start = time.perf_counter()
                result_text, is_terminal = executor.execute_tool_call(
                    function_info["name"],
                    function_info.get("arguments", "{}"),
                )
                print_timing(
                    f"TOOL_{round_index}_{function_info['name']}",
                    time.perf_counter() - tool_start,
                    logger=logger,
                )
                print_tool_result(
                    round_index,
                    function_info["name"],
                    result_text,
                    logger=logger,
                )
                tool_results.append(
                    {
                        "toolCallId": tool_call["id"],
                        "content": result_text,
                    }
                )
                terminal = terminal or is_terminal

            if terminal:
                break

            tool_chat_start = time.perf_counter()
            response = client.chat(
                session_id,
                stream=False,
                tools=tools,
                tool_results=tool_results,
            )
            print_timing(
                f"CHAT_AFTER_TOOLS_{round_index}",
                time.perf_counter() - tool_chat_start,
                logger=logger,
            )
            print_model_response(
                f"MODEL_RESPONSE_AFTER_TOOLS_{round_index}",
                response,
                logger=logger,
            )
        else:
            raise RuntimeError(f"Exceeded max rounds ({max_rounds}) before producing a final result")

        if executor.final_result is None:
            raise RuntimeError("The task ended without a final result artifact")
        return {
            "summary": executor.final_summary(),
            "artifacts": executor.artifacts,
            "result": executor.final_result.to_dict(),
            "preResult": executor.pre_result.to_dict() if executor.pre_result else {},
            "toolTrace": executor.tool_trace,
            "sessionId": session_id,
        }
    finally:
        executor.close()


def main() -> int:
    args = parse_args()
    task_payload = Path(args.task_json).read_text(encoding="utf-8")
    task = TaskInput.from_json(task_payload, Path(args.output_root))

    client = AgentHttpClient(
        base_url=args.runtime_base_url,
        username=args.runtime_user,
        password=args.runtime_password,
    )
    ids = setup_runtime(client, args.agent_name, args.biz_user_name)
    run_result = run_task_session(client, task, ids, args.max_rounds)
    print(run_result["summary"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
