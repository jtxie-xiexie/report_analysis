"""
DolphinDB Agent Runtime - Python Client Library

Encapsulates HTTP and WebSocket two transports for the agent-bus API.
This is the core reference for developers building agents on the framework.
"""

import json
import uuid
from typing import Any, Dict, Generator, List, Optional

import requests
import websocket

# ============================================================================
# Connection Config (hardcoded for demo)
# ============================================================================

DDB_HTTP_BASE = "http://192.168.100.208:8985"
DDB_WS_URL = "ws://192.168.100.208:8985/"
DDB_USERNAME = "admin"
DDB_PASSWORD = "123456"

BUS_PREFIX = "/agent-bus/v1"


# ============================================================================
# HTTP Client
# ============================================================================

class AgentHttpClient:
    """HTTP client for agent-bus REST API (JSON + SSE streaming)."""

    def __init__(
        self,
        base_url: str = DDB_HTTP_BASE,
        username: str = DDB_USERNAME,
        password: str = DDB_PASSWORD,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        if username:
            self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base_url}/api/login",
            json={"username": username, "password": password},
        )
        result = resp.json()
        if resp.status_code != 200 or result.get("code") != 0:
            raise RuntimeError(f"Login failed: {result}")
        # DolphinDB returns auth token in result[0], use it as Bearer token
        tokens = result.get("result")
        if tokens and isinstance(tokens, list) and tokens[0]:
            self.session.headers["Authorization"] = f"Bearer {tokens[0]}"
        print(f"[HTTP] Logged in as '{username}'")

    def _url(self, path: str) -> str:
        return f"{self.base_url}{BUS_PREFIX}/{path.lstrip('/')}"

    def _extract(self, resp: requests.Response) -> Any:
        """Extract payload from the standard bus envelope."""
        envelope = resp.json()
        if envelope.get("type") != "RESPONSE":
            error = envelope.get("error", {})
            raise RuntimeError(
                f"Bus error: code={error.get('code')} message={error.get('message')}"
            )
        return envelope.get("data")

    def _iter_sse_events(self, resp: requests.Response) -> Generator[Dict, None, None]:
        """SSE is UTF-8 text; decode bytes explicitly before JSON parsing."""
        for raw_line in resp.iter_lines(decode_unicode=False):
            if not raw_line:
                continue
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise RuntimeError("Invalid UTF-8 in SSE stream.") from exc
            if not line.startswith("data: "):
                continue
            data_str = line[len("data: "):]
            if not data_str.strip():
                continue
            try:
                yield json.loads(data_str)
            except json.JSONDecodeError:
                continue

    # -- Agent Management --

    def register_agent(self, agent_name: str, agent_type: str = "chat",
                       system_prompt: str = "", llm_config_id: str = "",
                       context_config: Optional[Dict] = None,
                       **kwargs) -> Dict:
        body = {
            "agentName": agent_name,
            "agentType": agent_type,
            "systemPrompt": system_prompt,
            **kwargs,
        }
        if llm_config_id:
            body["llmConfigId"] = llm_config_id
        if context_config:
            body["contextConfig"] = context_config
        return self._extract(self.session.post(self._url("agent/register"), json=body))

    def activate_agent(self, agent_id: str) -> Dict:
        return self._extract(self.session.post(self._url(f"agent/{agent_id}/activate")))

    def get_agent(self, agent_id: str) -> Dict:
        return self._extract(self.session.get(self._url(f"agent/{agent_id}")))

    def list_agents(self) -> List[Dict]:
        data = self._extract(self.session.get(self._url("agent/list")))
        return data.get("items", [])

    # -- LLM Config --

    def register_llm_config(self, name: str, provider: str, model: str,
                            endpoint: str = "", api_key: str = "",
                            default_params: Optional[Dict] = None,
                            **kwargs) -> Dict:
        body = {
            "name": name,
            "provider": provider,
            "model": model,
            "endpoint": endpoint,
            "apiKey": api_key,
            "defaultParams": default_params or {},
            **kwargs,
        }
        return self._extract(self.session.post(self._url("llm/config/register"), json=body))

    def bind_llm(self, agent_id: str, llm_config_id: str,
                 is_default: bool = True) -> Dict:
        body = {
            "agentId": agent_id,
            "llmConfigId":  llm_config_id,
            "isDefault": is_default,
            "priority": 0,
        }
        return self._extract(
            self.session.post(self._url(f"agent/{agent_id}/llm/access"), json=body)
        )

    # -- BizUser --

    def register_bizuser(self, display_name: str) -> Dict:
        return self._extract(
            self.session.post(self._url("bizuser/register"), json={"displayName": display_name})
        )

    def grant_bizuser_access(self, agent_id: str, biz_user_id: str) -> Dict:
        body = {"agentId": agent_id, "bizUserId": biz_user_id}
        return self._extract(
            self.session.post(self._url(f"agent/{agent_id}/bizuser/access"), json=body)
        )

    # -- Session --

    def create_session(self, agent_id: str, biz_user_id: str,
                       title: str = "") -> Dict:
        body = {"agentId": agent_id, "bizUserId": biz_user_id, "title": title}
        return self._extract(self.session.post(self._url("session/create"), json=body))

    def list_sessions(self, agent_id: str, biz_user_id: str = "") -> List[Dict]:
        params = {"agentId": agent_id}
        if biz_user_id:
            params["bizUserId"] = biz_user_id
        data = self._extract(self.session.get(self._url("session/list"), params=params))
        return data.get("items", [])

    def get_messages(self, session_id: str) -> List[Dict]:
        data = self._extract(
            self.session.get(self._url(f"session/{session_id}/messages"))
        )
        return data.get("items", [])

    def archive_session(self, session_id: str) -> Dict:
        return self._extract(
            self.session.post(self._url(f"session/{session_id}/archive"))
        )

    # -- Chat Completions (core API) --

    def chat(self, session_id: str, message: str = "", stream: bool = True,
             tools: Optional[List[Dict]] = None,
             tool_results: Optional[List[Dict]] = None,
             active_skill_names: Optional[List[str]] = None,
             **kwargs) -> Any:
        """
        Send a message and/or tool results, get LLM response.
        At least one of `message` or `tool_results` must be provided.
        - stream=False: returns the full response dict
        - stream=True: returns a generator yielding SSE chunks
        """
        body: Dict[str, Any] = {
            "sessionId": session_id,
            "stream": stream,
            **kwargs,
        }
        if message:
            body["message"] = message
        if tools:
            body["tools"] = tools
        if tool_results:
            body["toolResults"] = tool_results
        if active_skill_names:
            body["activeSkillNames"] = active_skill_names

        if not stream:
            return self._extract(
                self.session.post(self._url("chat/completions"), json=body)
            )
        else:
            return self._chat_stream(body)

    def _chat_stream(self, body: Dict) -> Generator[Dict, None, None]:
        """Parse SSE stream from chat/completions."""
        resp = self.session.post(
            self._url("chat/completions"),
            json=body,
            stream=True,
            headers={"Accept": "text/event-stream"},
        )
        resp.raise_for_status()
        for event in self._iter_sse_events(resp):
            yield event

    # -- Append Message (for tool_result callback) --

    def append_message(self, session_id: str, role: str, content: str,
                       content_type: str = "text",
                       tool_call_id: str = "", **kwargs) -> Dict:
        body: Dict[str, Any] = {
            "role": role,
            "contentType": content_type,
            "content": content,
            **kwargs,
        }
        if tool_call_id:
            body["toolCallId"] = tool_call_id
        return self._extract(
            self.session.post(self._url(f"session/{session_id}/message"), json=body)
        )

    # -- Memory --

    def store_memory(self, agent_id: str, content: str, category: str = "fact",
                     importance: float = 0.8, scope: str = "agent",
                     biz_user_id: str = "", **kwargs) -> Dict:
        body: Dict[str, Any] = {
            "agentId": agent_id,
            "content": content,
            "category": category,
            "importance": importance,
            "scope": scope,
            **kwargs,
        }
        if biz_user_id:
            body["bizUserId"] = biz_user_id
        return self._extract(self.session.post(self._url("memory/store"), json=body))

    def search_memory(self, agent_id: str, text_query: str = "",
                      top_k: int = 5, **kwargs) -> List[Dict]:
        body: Dict[str, Any] = {
            "agentId": agent_id,
            "textQuery": text_query,
            "topK": top_k,
            **kwargs,
        }
        data = self._extract(self.session.post(self._url("memory/search"), json=body))
        return data.get("items", [])

    # -- Context Preview --

    def context_preview(self, session_id: str, message: str = "",
                        tools: Optional[List[Dict]] = None,
                        tool_results: Optional[List[Dict]] = None,
                        active_skill_names: Optional[List[str]] = None,
                        **kwargs) -> Dict:
        body: Dict[str, Any] = {"sessionId": session_id, **kwargs}
        if message:
            body["message"] = message
        if tools:
            body["tools"] = tools
        if tool_results:
            body["toolResults"] = tool_results
        if active_skill_names:
            body["activeSkillNames"] = active_skill_names
        return self._extract(
            self.session.post(self._url("context/preview"), json=body)
        )

    # -- Session Summary --

    def generate_summary(self, session_id: str, force: bool = False) -> Dict:
        body: Dict[str, Any] = {"force": force}
        return self._extract(
            self.session.post(self._url(f"session/{session_id}/summary"), json=body)
        )

    # -- LLM Complete (raw, bypass context assembly) --

    def llm_complete(self, agent_id: str, messages: List[Dict],
                     stream: bool = False, session_id: str = "",
                     **kwargs) -> Any:
        body: Dict[str, Any] = {
            "agentId": agent_id,
            "messages": messages,
            "stream": stream,
            **kwargs,
        }
        if session_id:
            body["sessionId"] = session_id
        if not stream:
            return self._extract(
                self.session.post(self._url("llm/complete"), json=body)
            )
        else:
            return self._llm_stream(body)

    def _llm_stream(self, body: Dict) -> Generator[Dict, None, None]:
        resp = self.session.post(
            self._url("llm/complete"),
            json=body,
            stream=True,
            headers={"Accept": "text/event-stream"},
        )
        resp.raise_for_status()
        for event in self._iter_sse_events(resp):
            yield event


# ============================================================================
# WebSocket Client
# ============================================================================

class AgentWsClient:
    """WebSocket client for agent-bus protocol."""

    def __init__(
        self,
        ws_url: str = DDB_WS_URL,
        http_base: str = DDB_HTTP_BASE,
        username: str = DDB_USERNAME,
        password: str = DDB_PASSWORD,
    ):
        self.ws_url = ws_url
        self.http_base = http_base.rstrip("/")
        # Login via HTTP first to get auth token
        self.token = ""
        if username:
            self.token = self._login(username, password)
        self.ws: Optional[websocket.WebSocket] = None

    def _login(self, username: str, password: str) -> str:
        resp = requests.post(
            f"{self.http_base}/api/login",
            json={"username": username, "password": password},
        )
        result = resp.json()
        if resp.status_code != 200 or result.get("code") != 0:
            raise RuntimeError(f"Login failed: {result}")
        tokens = result.get("result")
        token = tokens[0] if tokens and isinstance(tokens, list) else ""
        print(f"[WS] Logged in as '{username}'")
        return token

    def connect(self) -> None:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self.ws = websocket.create_connection(
            self.ws_url,
            subprotocols=["agent-bus"],
            header=headers,
            timeout=60,
        )
        print(f"[WS] Connected to {self.ws_url}")

    def close(self) -> None:
        if self.ws:
            self.ws.close()
            self.ws = None

    def _send(self, action: str, payload: Dict, agent_id: str = "",
              session_id: str = "", msg_id: str = "") -> str:
        """Send a request frame and return the msgId."""
        if not msg_id:
            msg_id = f"req-{uuid.uuid4().hex[:8]}"
        frame: Dict[str, Any] = {
            "type": "REQUEST",
            "msgId": msg_id,
            "action": action,
            "payload": payload,
        }
        if agent_id:
            frame["agentId"] = agent_id
        if session_id:
            frame["sessionId"] = session_id
        self.ws.send(json.dumps(frame, ensure_ascii=False))
        return msg_id

    def _recv(self) -> Dict:
        """Receive one frame (handles both TEXT and BINARY)."""
        data = self.ws.recv()
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def _request(self, action: str, payload: Dict, **kwargs) -> Any:
        """Send request and wait for RESPONSE, return data."""
        self._send(action, payload, **kwargs)
        resp = self._recv()
        if resp.get("type") == "ERROR":
            error = resp.get("error", {})
            raise RuntimeError(
                f"WS error: code={error.get('code')} message={error.get('message')}"
            )
        if resp.get("type") != "RESPONSE":
            raise RuntimeError(f"Unexpected WS frame type: {resp.get('type')}")
        return resp.get("data")

    # -- Agent Management --

    def register_agent(self, agent_name: str, agent_type: str = "chat",
                       system_prompt: str = "", llm_config_id: str = "",
                       context_config: Optional[Dict] = None,
                       **kwargs) -> Dict:
        payload = {
            "agentName": agent_name,
            "agentType": agent_type,
            "systemPrompt": system_prompt,
            **kwargs,
        }
        if llm_config_id:
            payload["llmConfigId"] = llm_config_id
        if context_config:
            payload["contextConfig"] = context_config
        return self._request("agent.register", payload)

    def activate_agent(self, agent_id: str) -> Dict:
        return self._request("agent.activate", {}, agent_id=agent_id)

    def get_agent(self, agent_id: str) -> Dict:
        return self._request("agent.get", {}, agent_id=agent_id)

    # -- LLM Config --

    def register_llm_config(self, name: str, provider: str, model: str,
                            endpoint: str = "", api_key: str = "",
                            default_params: Optional[Dict] = None,
                            **kwargs) -> Dict:
        payload = {
            "name": name,
            "provider": provider,
            "model": model,
            "endpoint": endpoint,
            "apiKey": api_key,
            "defaultParams": default_params or {},
            **kwargs,
        }
        return self._request("llm.config.register", payload)

    def bind_llm(self, agent_id: str, llm_config_id: str,
                 is_default: bool = True) -> Dict:
        payload = {
            "llmConfigId": llm_config_id,
            "isDefault": is_default,
            "priority": 0,
        }
        return self._request("agent.llm.grant", payload, agent_id=agent_id)

    # -- BizUser --

    def register_bizuser(self, display_name: str) -> Dict:
        return self._request("bizuser.register", {"displayName": display_name})

    def grant_bizuser_access(self, agent_id: str, biz_user_id: str) -> Dict:
        payload = {"bizUserId": biz_user_id}
        return self._request("agent.bizuser.grant", payload, agent_id=agent_id)

    # -- Session --

    def create_session(self, agent_id: str, biz_user_id: str,
                       title: str = "") -> Dict:
        payload = {"agentId": agent_id, "bizUserId": biz_user_id, "title": title}
        return self._request("session.create", payload, agent_id=agent_id)

    def get_messages(self, session_id: str) -> List[Dict]:
        data = self._request("session.getMessages", {}, session_id=session_id)
        return data.get("items", [])

    # -- Chat Completions --

    def chat(self, session_id: str, message: str = "", stream: bool = True,
             tools: Optional[List[Dict]] = None,
             tool_results: Optional[List[Dict]] = None,
             agent_id: str = "",
             **kwargs) -> Any:
        """
        - stream=False: send and receive a single RESPONSE
        - stream=True: send request, then yield STREAM_START / STREAM_CHUNK / STREAM_END
        """
        payload: Dict[str, Any] = {
            "stream": stream,
            **kwargs,
        }
        if message:
            payload["message"] = message
        if tools:
            payload["tools"] = tools
        if tool_results:
            payload["toolResults"] = tool_results

        self._send("chat.completions", payload,
                    agent_id=agent_id, session_id=session_id)

        if not stream:
            resp = self._recv()
            if resp.get("type") == "ERROR":
                error = resp.get("error", {})
                raise RuntimeError(f"Chat error: {error}")
            return resp.get("data")
        else:
            return self._recv_stream()

    def _recv_stream(self) -> Generator[Dict, None, None]:
        """Yield frames until STREAM_END or ERROR."""
        while True:
            frame = self._recv()
            frame_type = frame.get("type")
            yield frame
            if frame_type in ("STREAM_END", "ERROR"):
                break

    # -- Append Message --

    def append_message(self, session_id: str, role: str, content: str,
                       content_type: str = "text",
                       tool_call_id: str = "", **kwargs) -> Dict:
        payload: Dict[str, Any] = {
            "role": role,
            "contentType": content_type,
            "content": content,
            **kwargs,
        }
        if tool_call_id:
            payload["toolCallId"] = tool_call_id
        return self._request("session.appendMessage", payload,
                             session_id=session_id)

    # -- Memory --

    def store_memory(self, agent_id: str, content: str, category: str = "fact",
                     importance: float = 0.8, scope: str = "agent",
                     **kwargs) -> Dict:
        payload: Dict[str, Any] = {
            "agentId": agent_id,
            "content": content,
            "category": category,
            "importance": importance,
            "scope": scope,
            **kwargs,
        }
        return self._request("memory.store", payload, agent_id=agent_id)

    def search_memory(self, agent_id: str, text_query: str = "",
                      top_k: int = 5, **kwargs) -> List[Dict]:
        payload: Dict[str, Any] = {
            "agentId": agent_id,
            "textQuery": text_query,
            "topK": top_k,
            **kwargs,
        }
        data = self._request("memory.search", payload, agent_id=agent_id)
        return data.get("items", [])

    # -- Context Preview --

    def context_preview(self, session_id: str, message: str = "",
                        tools: Optional[List[Dict]] = None,
                        tool_results: Optional[List[Dict]] = None,
                        active_skill_names: Optional[List[str]] = None,
                        **kwargs) -> Dict:
        payload: Dict[str, Any] = {**kwargs}
        if message:
            payload["message"] = message
        if tools:
            payload["tools"] = tools
        if tool_results:
            payload["toolResults"] = tool_results
        if active_skill_names:
            payload["activeSkillNames"] = active_skill_names
        return self._request("context.preview", payload, session_id=session_id)

    # -- Session Summary --

    def generate_summary(self, session_id: str, force: bool = False) -> Dict:
        payload = {"force": force}
        return self._request("session.generateSummary", payload,
                             session_id=session_id)
