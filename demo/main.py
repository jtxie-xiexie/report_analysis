#!/usr/bin/env python3
"""
DolphinDB Agent Runtime Demo - Single Entry Point

Usage:
    export LLM_API_KEY='your-api-key'
    python main.py

This script does everything in one run:
  Phase 1 - Setup: register LLM config, create Agent, BizUser, seed memories
  Phase 2 - API walkthrough: exercise key APIs (non-streaming chat, tool calling,
            memory search, context preview, session summary)
  Phase 3 - Launch Gradio chat UI for interactive use

Modify the constants below to switch LLM provider or DolphinDB address.
"""

import json
import os
import sys
from datetime import datetime

import gradio as gr

from agent_client import AgentHttpClient
from agent_client import AgentWsClient

# ============================================================================
# Configuration - edit these to match your environment
# ============================================================================

# LLM
LLM_PROVIDER = "openai-compatible"
LLM_MODEL = "deepseek-v3.2"
LLM_ENDPOINT = "https://api.n1n.ai"
# LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_API_KEY = "sk-A7cbB9D6aDRYX8hmsMCQQj3L7YDn4172SFi3aIjtV865oQ0D"
# Agent system prompt
SYSTEM_PROMPT = """\
You are a helpful knowledge assistant powered by DolphinDB Agent Runtime.
Your capabilities:
- Answer questions based on your knowledge and conversation context
- Use tools when the user asks for real-time data (e.g., calculations, current time)
- Remember user preferences and facts from previous conversations

Guidelines:
- Be concise and direct
- When you use a tool, explain what you're doing
- If memories are provided in context, use them naturally
"""

# Context assembly config
CONTEXT_CONFIG = {
    "maxContextTokens": 128000,
    "reservedForOutput": 4096,
    "maxHistoryMessages": 30,
    "includeMemory": True,
    "memoryTopK": 5,
    "memoryTokenBudget": 2000,
    "includeSkills": False,
    "includeSkillCatalog": False,
    "summaryConfig": {
        "enabled": True,
        "autoTriggerOnTruncate": True,
        "minMessagesSinceLastSummary": 10,
    },
}

# Seed memories
SEED_MEMORIES = [
    ("DolphinDB is a high-performance distributed time-series database system.",
     "fact", 0.9),
    ("The agent runtime framework provides session management, memory management, "
     "context assembly, skill management, and LLM integration.",
     "fact", 0.85),
    ("The demo user prefers Chinese responses when asked in Chinese, "
     "and English responses when asked in English.",
     "preference", 0.7),
]

# Tool definitions - application layer defines and executes tools,
# the framework only passes tool_call/tool_result between LLM and application
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "calculate",
            "description": "Evaluate a mathematical expression and return the result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": "The math expression, e.g. '2 + 3 * 4'",
                    }
                },
                "required": ["expression"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": "Get the current date and time.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def execute_tool(name: str, arguments: str) -> str:
    """Execute a tool call locally. Framework does NOT execute tools."""
    args = json.loads(arguments) if arguments else {}
    if name == "calculate":
        expr = args.get("expression", "0")
        try:
            return str(eval(expr, {"__builtins__": {}}))
        except Exception as e:
            return f"Error: {e}"
    elif name == "get_current_time":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Unknown tool: {name}"


# ============================================================================
# Phase 1: Setup
# ============================================================================

def setup(client: AgentHttpClient) -> dict:
    """Register LLM config, Agent, BizUser, seed memories. Returns IDs."""
    print("\n" + "=" * 60)
    print("Phase 1: Setup")
    print("=" * 60)

    # 1. Register LLM config
    print("\n[1/5] Register LLM config...")
    llm = client.register_llm_config(
        name="demo-llm",
        provider=LLM_PROVIDER,
        model=LLM_MODEL,
        endpoint=LLM_ENDPOINT,
        api_key=LLM_API_KEY,
        default_params={"temperature": 0.7, "maxTokens": 2048},
        contextWindow=128000,
    )
    llm_config_id = llm["llmConfigId"]
    print(f"      LLM Config: {llm_config_id}")

    # 2. Register and activate Agent
    print("[2/5] Register & activate Agent...")
    agent = client.register_agent(
        agent_name="Knowledge Assistant Demo",
        agent_type="chat",
        system_prompt=SYSTEM_PROMPT,
        llm_config_id=llm_config_id,
        context_config=CONTEXT_CONFIG,
        description="Demo agent for the DolphinDB Agent Runtime framework",
    )
    agent_id = agent["agentId"]
    client.activate_agent(agent_id)
    print(f"      Agent: {agent_id} (ACTIVE)")

    # 3. Register BizUser and grant access
    print("[3/5] Register BizUser & grant access...")
    biz_user = client.register_bizuser(display_name="Demo User")
    biz_user_id = biz_user["bizUserId"]
    client.grant_bizuser_access(agent_id, biz_user_id)
    print(f"      BizUser: {biz_user_id}")

    # 4. Store seed memories (requires embedding config, skip if unavailable)
    print("[4/5] Store seed memories...")
    memory_available = True
    for content, category, importance in SEED_MEMORIES:
        try:
            client.store_memory(agent_id=agent_id, content=content,
                                category=category, importance=importance)
            print(f"      [{category}] {content[:50]}...")
        except RuntimeError as e:
            if "embedding" in str(e).lower():
                print(f"      SKIP: Memory requires embedding config (not configured).")
                print(f"      To enable memory, register an embedding LLM config:")
                print(f"        capability='embedding', embeddingDimension=<dim>")
                memory_available = False
                break
            raise

    print("[5/5] Setup complete.\n")
    return {"agentId": agent_id, "bizUserId": biz_user_id,
            "llmConfigId": llm_config_id}


# ============================================================================
# Phase 2: API Walkthrough
# ============================================================================

def walkthrough(client: AgentWsClient, ids: dict):
    """Exercise key APIs with printed output."""
    print("=" * 60)
    print("Phase 2: API Walkthrough")
    print("=" * 60)
    agent_id = ids["agentId"]
    biz_user_id = ids["bizUserId"]

    # --- Create session ---
    print("\n--- Create Session ---")
    session = client.create_session(agent_id, biz_user_id, title="Walkthrough")
    session_id = session["sessionId"]
    print(f"Session: {session_id}")

    # --- Non-streaming chat ---
    print("\n--- Chat (non-streaming) ---")
    print("User: Hello! What can you help me with?")
    resp = client.chat(session_id, "Hello! What can you help me with?", stream=False)
    print(f"Assistant: {resp.get('content', '')}")
    usage = resp.get("usage", {})
    print(f"  (tokens: {usage.get('totalTokens', '?')})")

    # --- Streaming chat ---
    print("\n--- Chat (streaming SSE) ---")
    print("User: Tell me briefly about DolphinDB.")
    print("Assistant: ", end="", flush=True)
    for event in client.chat(session_id, "Tell me briefly about DolphinDB.", stream=True):
        stream_data = event.get("data") or event.get("payload") or {}
        if event.get("type") == "STREAM_CHUNK":
            chunk = stream_data.get("content") or stream_data.get("delta", {}).get("content", "")
            if chunk:
                print(chunk, end="", flush=True)
        elif event.get("type") == "STREAM_END":
            print()

    # --- Tool calling loop ---
    print("\n--- Tool Calling Loop ---")
    user_msg = "What is 123 * 456 + 789? Also what time is it?"
    print(f"User: {user_msg}")
    resp = client.chat(session_id, user_msg, stream=False, tools=TOOLS)

    round_num = 0
    while resp.get("toolCalls"):
        round_num += 1
        print(f"  [Round {round_num}]")
        # Execute all tools and collect results
        results = []
        for tc in resp["toolCalls"]:
            fn = tc["function"]
            name, args_str = fn["name"], fn.get("arguments", "{}")
            result = execute_tool(name, args_str)
            print(f"    {name}({args_str}) -> {result}")
            results.append({"toolCallId": tc["id"], "content": result})
        # Submit tool results via chat/completions; the framework appends them
        # to session and continues the LLM conversation automatically
        resp = client.chat(session_id, stream=False, tools=TOOLS,
                           tool_results=results)
    print(f"Assistant: {resp.get('content', '')}")

    # --- Memory search ---
    print("\n--- Memory Search ---")
    try:
        mems = client.search_memory(agent_id, text_query="DolphinDB", top_k=3)
        print(f"Query 'DolphinDB' -> {len(mems)} results:")
        for m in mems:
            print(f"  [{m.get('category')}] {m.get('content', '')[:70]}...")
    except RuntimeError as e:
        print(f"  SKIP: {e}")

    # --- Context preview ---
    print("\n--- Context Preview ---")
    preview = client.context_preview(session_id, "Summarize our chat.", tools=TOOLS)
    detail = preview.get("detail", {})
    print(f"Messages: {len(preview.get('messages', []))}  "
          f"Memories: {detail.get('memoriesIncluded', 0)}  "
          f"History: {detail.get('messagesIncluded', 0)}")

    # --- Session summary ---
    print("\n--- Session Summary ---")
    summary = client.generate_summary(session_id, force=True)
    if summary.get("generated"):
        print(f"Summary: {summary.get('content', '')[:150]}...")
    else:
        print(f"Not generated: {summary.get('reason', '?')}")

    # --- Session list ---
    print("\n--- Session List ---")
    sessions = client.list_sessions(agent_id, biz_user_id)
    for s in sessions[:5]:
        print(f"  {s['sessionId']}: {s.get('title', '-')} ({s.get('statusName', '?')})")

    print("\nPhase 2 complete.\n")
    return session_id


# ============================================================================
# Phase 3: Gradio Chat UI
# ============================================================================

class GradioAgent:
    """Wraps agent_client for Gradio's streaming chat interface."""

    def __init__(self, client: AgentWsClient, agent_id: str, biz_user_id: str):
        self.client = client
        self.agent_id = agent_id
        self.biz_user_id = biz_user_id
        self.session_id = None

    def new_session(self):
        session = self.client.create_session(
            self.agent_id, self.biz_user_id, title="Gradio Chat")
        self.session_id = session["sessionId"]
        return self.session_id

    def stream_reply(self, message: str, history: list):
        """Streaming handler: yields partial text as SSE chunks arrive."""
        if not self.session_id:
            self.new_session()

        partial = ""
        end_payload = None

        # Stream the LLM response
        for event in self.client.chat(
            self.session_id, message, stream=True, tools=TOOLS,
        ):
            t = event.get("type")
            stream_data = event.get("data") or event.get("payload") or {}
            if t == "STREAM_CHUNK":
                chunk = stream_data.get("content") or stream_data.get("delta", {}).get("content", "")
                if chunk:
                    partial += chunk
                    yield partial
            elif t == "STREAM_END":
                end_payload = stream_data

        # Handle tool calls
        if end_payload and end_payload.get("toolCalls"):
            partial += "\n\n*[Using tools...]*\n"
            yield partial

            results = []
            for tc in end_payload["toolCalls"]:
                fn = tc["function"]
                name, args_str = fn["name"], fn.get("arguments", "{}")
                result = execute_tool(name, args_str)
                partial += f"\n`{name}({args_str})` -> `{result}`\n"
                yield partial
                results.append({"toolCallId": tc["id"], "content": result})

            # Submit tool results and stream the final answer
            partial += "\n"
            for event in self.client.chat(
                self.session_id, stream=True, tools=TOOLS,
                tool_results=results,
            ):
                t = event.get("type")
                stream_data = event.get("data") or event.get("payload") or {}
                if t == "STREAM_CHUNK":
                    chunk = stream_data.get("content") or stream_data.get("delta", {}).get("content", "")
                    if chunk:
                        partial += chunk
                        yield partial


def launch_ui(client: AgentHttpClient, agent_id: str, biz_user_id: str):
    agent = GradioAgent(client, agent_id, biz_user_id)

    with gr.Blocks(title="DolphinDB Agent Demo") as app:
        gr.Markdown("# DolphinDB Agent Runtime Demo")
        gr.Markdown(
            "Chat with the agent. Try asking calculations (`123*456+789`) "
            "or current time. The agent has memory and tool calling support."
        )

        chatbot = gr.Chatbot(height=500)
        session_status = gr.Textbox(
            value="No active session",
            label="Current Session",
            interactive=False,
        )
        msg = gr.Textbox(placeholder="Type a message...",
                         show_label=False, container=False)

        with gr.Row():
            send_btn = gr.Button("Send", variant="primary")
            new_btn = gr.Button("New Session", variant="secondary")
            clear_btn = gr.ClearButton([msg, chatbot], value="Clear")

        def respond(message, chat_history):
            chat_history = chat_history or []
            chat_history.append({"role": "user", "content": message})
            chat_history.append({"role": "assistant", "content": ""})
            if not agent.session_id:
                agent.new_session()
            yield "", chat_history, f"Current session: {agent.session_id}"
            for partial in agent.stream_reply(message, chat_history):
                chat_history[-1]["content"] = partial
                yield "", chat_history, f"Current session: {agent.session_id}"

        def on_new_session():
            sid = agent.new_session()
            return [], "", f"Current session: {sid}"

        msg.submit(respond, [msg, chatbot], [msg, chatbot, session_status])
        send_btn.click(respond, [msg, chatbot], [msg, chatbot, session_status])
        new_btn.click(on_new_session, outputs=[chatbot, msg, session_status])

    print("=" * 60)
    print("Phase 3: Launching Gradio UI at http://0.0.0.0:7860")
    print("=" * 60)
    app.launch(server_name="0.0.0.0", server_port=7860)


# ============================================================================
# Main
# ============================================================================

def main():
    if not LLM_API_KEY:
        print("ERROR: Set LLM_API_KEY environment variable first.")
        print("  export LLM_API_KEY='your-api-key'")
        sys.exit(1)

    client = AgentHttpClient()

    # Phase 1: setup
    ids = setup(client)

    # Phase 2: walkthrough
    walkthrough(client, ids)

    # Phase 3: launch Gradio
    launch_ui(client, ids["agentId"], ids["bizUserId"])


if __name__ == "__main__":
    main()

