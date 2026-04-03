# DolphinDB Agent Runtime - Python Demo

A complete demo showing how to build an agent application on the DolphinDB Agent Runtime framework.

## Files

| File | Description |
|------|-------------|
| `agent_client.py` | **Core reference** - HTTP + WebSocket client library wrapping all bus APIs |
| `main.py` | Single entry point: setup -> API walkthrough -> launch Gradio chat UI |

## Quick Start

```bash
pip install -r requirements.txt
export LLM_API_KEY='your-api-key'
python main.py
```

One command does everything:
1. Registers LLM config, Agent, BizUser, seeds memories
2. Runs through key APIs (chat, streaming, tool calling, memory, context preview, summary)
3. Launches a Gradio chat UI at http://localhost:7860

## What's Covered

| API | Where |
|-----|-------|
| LLM config register | `main.py` Phase 1 |
| Agent register + activate | `main.py` Phase 1 |
| BizUser register + grant access | `main.py` Phase 1 |
| Memory store + search | `main.py` Phase 1 & 2 |
| Session create / list / archive | `main.py` Phase 2 |
| Chat completions (non-streaming) | `main.py` Phase 2 |
| Chat completions (SSE streaming) | `main.py` Phase 2 & Gradio UI |
| Tool calling loop | `main.py` Phase 2 & Gradio UI |
| Context preview | `main.py` Phase 2 |
| Session summary | `main.py` Phase 2 |

## Architecture

```
main.py          (setup + walkthrough + Gradio UI)
    |
    v
agent_client.py  (HTTP + WS client library)
    |
    v
DolphinDB Agent Runtime  (the framework, bus API)
    |
    v
LLM API  (OpenAI-compatible)
```

Tool execution happens in application code (`execute_tool()`), not in the framework.
Tool results are submitted back via `chat/completions` with the `toolResults` field — the framework
appends them to the session and automatically continues the LLM conversation.
`context/preview` follows the same input contract: provide `message`, `toolResults`, or both to inspect the
next assembled turn without persisting those inputs.

## Customization

- **LLM provider**: Edit `LLM_PROVIDER`, `LLM_MODEL`, `LLM_ENDPOINT` at top of `main.py`
- **System prompt**: Edit `SYSTEM_PROMPT` in `main.py`
- **Add tools**: Extend `TOOLS` list and `execute_tool()` function
- **DolphinDB address**: Edit `DDB_HTTP_BASE` / `DDB_WS_URL` in `agent_client.py`
- **Use WebSocket instead of HTTP**: Replace `AgentHttpClient` with `AgentWsClient` in `main.py`
