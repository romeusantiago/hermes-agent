from __future__ import annotations

import asyncio
from typing import Any, cast

from hermes_cli.web_routers import chat_ws
import tui_gateway.ws as ws_mod


class _QueryParams:
    def __init__(self, values: dict[str, str]):
        self._values = values

    def get(self, key: str, default: str = "") -> str:
        return self._values.get(key, default)


class _FakeWebSocket:
    def __init__(self, query: dict[str, str]):
        self.query_params = _QueryParams(query)
        self.close_codes: list[int] = []

    async def close(self, code: int = 1000) -> None:
        self.close_codes.append(code)


def test_gateway_ws_propagates_explicit_read_only_mode(monkeypatch):
    observed: list[bool | None] = []

    async def allow(_ws):
        return True

    async def fake_handle(_ws, **kwargs):
        observed.append(kwargs.get("read_only"))

    monkeypatch.setattr(chat_ws, "_close_unless_sidecar_allowed", allow)
    monkeypatch.setattr(ws_mod, "handle_ws", fake_handle)

    asyncio.run(chat_ws.gateway_ws(cast(Any, _FakeWebSocket({"mode": "read-only"}))))
    asyncio.run(chat_ws.gateway_ws(cast(Any, _FakeWebSocket({}))))

    assert observed == [True, False]


def test_gateway_ws_rejects_unknown_mode_before_authentication(monkeypatch):
    auth_called = False

    async def allow(_ws):
        nonlocal auth_called
        auth_called = True
        return True

    async def fake_handle(_ws, **_kwargs):
        raise AssertionError("invalid mode reached handler")

    monkeypatch.setattr(chat_ws, "_close_unless_sidecar_allowed", allow)
    monkeypatch.setattr(ws_mod, "handle_ws", fake_handle)
    ws = _FakeWebSocket({"mode": "READ_ONLY"})

    asyncio.run(chat_ws.gateway_ws(cast(Any, ws)))

    assert auth_called is False
    assert ws.close_codes == [4403]
