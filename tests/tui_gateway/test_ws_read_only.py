from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace

import pytest

from gateway import scale_to_zero
from tui_gateway import server
import tui_gateway.ws as ws_mod


class _FakeWebSocket:
    def __init__(
        self,
        frames: list[str],
        *,
        close_reason: str = "",
        fail_send_at: int | None = None,
        on_receive=None,
    ):
        self._frames = iter(frames)
        self._close_reason = close_reason
        self._fail_send_at = fail_send_at
        self._on_receive = on_receive
        self._send_count = 0
        self.client = SimpleNamespace(host="127.0.0.1", port=41337)
        self.scope = {}
        self.accepted = False
        self.closed = False
        self.sent: list[dict] = []

    async def accept(self, **_kwargs) -> None:
        self.accepted = True

    async def send_text(self, line: str) -> None:
        self._send_count += 1
        if self._send_count == self._fail_send_at:
            raise RuntimeError("synthetic send failure")
        self.sent.append(json.loads(line))

    async def receive_text(self) -> str:
        try:
            if self._on_receive is not None:
                self._on_receive()
                self._on_receive = None
            return next(self._frames)
        except StopIteration:
            raise ws_mod._WebSocketDisconnect(code=1000, reason=self._close_reason)

    async def close(self) -> None:
        self.closed = True


def _patch_effects(monkeypatch) -> list[str]:
    effects: list[str] = []

    def record(name: str, result=None):
        def inner(*_args, **_kwargs):
            effects.append(name)
            return result

        return inner

    monkeypatch.setattr(ws_mod, "_disable_nagle", lambda _ws: None)
    monkeypatch.setattr(ws_mod, "_note_dashboard_client_activity", record("dashboard_marker"))
    monkeypatch.setattr(ws_mod, "replay_epoch", lambda: "test-epoch")
    monkeypatch.setattr(server, "resolve_skin", record("resolve_skin", {"name": "test"}))
    monkeypatch.setattr(server, "_ensure_skin_watcher", record("skin_watcher"))
    monkeypatch.setattr(server, "register_live_transport", record("register_transport"))
    monkeypatch.setattr(server, "_start_backend_heartbeat_refresher", record("backend_heartbeat"))
    monkeypatch.setattr(server, "_schedule_startup_orphan_sweep", record("orphan_sweep"))
    monkeypatch.setattr(server, "dispatch", record("dispatch", None))
    monkeypatch.setattr(server, "unregister_live_transport", record("unregister_transport"))
    monkeypatch.setattr(server, "_release_wake_for_transport", record("release_wake"))
    monkeypatch.setattr(server, "_close_sessions_for_transport", record("close_sessions", (0, 0)))
    return effects


def test_read_only_ws_allows_only_exact_ping_and_skips_mutating_lifecycle(monkeypatch):
    effects = _patch_effects(monkeypatch)
    frames = [
        json.dumps({"jsonrpc": "2.0", "id": "ping-1", "method": "gateway.ping", "params": {}}),
        json.dumps({"jsonrpc": "2.0", "id": "session-1", "method": "session.create", "params": {}}),
        json.dumps({
            "jsonrpc": "2.0",
            "id": "ping-extra",
            "method": "gateway.ping",
            "params": {},
            "extra": True,
        }),
    ]
    ws = _FakeWebSocket(frames)

    asyncio.run(ws_mod.handle_ws(ws, read_only=True))

    assert ws.accepted is True
    assert ws.closed is True
    assert ws.sent[0]["params"] == {
        "type": "gateway.ready",
        "payload": {
            "skin": {"name": "test"},
            "change_events": False,
            "heartbeat": True,
            "replay_epoch": "test-epoch",
            "read_only": True,
        },
    }
    assert ws.sent[1] == {"jsonrpc": "2.0", "result": {"ok": True}, "id": "ping-1"}
    assert ws.sent[2]["error"] == {"code": -32601, "message": "method not allowed"}
    assert ws.sent[3]["error"] == {"code": -32600, "message": "invalid request"}
    assert effects == ["resolve_skin"]


def test_read_only_ws_never_logs_client_controlled_values(monkeypatch, caplog):
    _patch_effects(monkeypatch)
    payload_secret = "PAYLOAD-SECRET-SENTINEL"
    id_secret = "ID-SECRET-SENTINEL"
    method_secret = "METHOD-SECRET-SENTINEL"
    close_secret = "CLOSE-SECRET-SENTINEL"
    frames = [
        '{"broken":"' + payload_secret + '"',
        json.dumps({"jsonrpc": "2.0", "id": id_secret, "method": method_secret, "params": {}}),
    ]
    ws = _FakeWebSocket(frames, close_reason=close_secret)
    caplog.set_level(logging.INFO, logger=ws_mod._log.name)

    asyncio.run(ws_mod.handle_ws(ws, read_only=True))

    send_secret = "SEND-ID-SECRET-SENTINEL"
    failing_ws = _FakeWebSocket(
        [json.dumps({"jsonrpc": "2.0", "id": send_secret, "method": "gateway.ping", "params": {}})],
        fail_send_at=2,
    )
    asyncio.run(ws_mod.handle_ws(failing_ws, read_only=True))

    assert payload_secret not in caplog.text
    assert id_secret not in caplog.text
    assert method_secret not in caplog.text
    assert close_secret not in caplog.text
    assert send_secret not in caplog.text


def test_ws_never_logs_client_controlled_values_in_normal_mode(monkeypatch, caplog):
    _patch_effects(monkeypatch)
    payload_secret = "NORMAL-PAYLOAD-SECRET-SENTINEL"
    id_secret = "NORMAL-ID-SECRET-SENTINEL"
    method_secret = "NORMAL-METHOD-SECRET-SENTINEL"
    close_secret = "NORMAL-CLOSE-SECRET-SENTINEL"

    def crash(*_args, **_kwargs):
        raise RuntimeError("synthetic dispatch failure " + method_secret)

    monkeypatch.setattr(server, "dispatch", crash)
    ws = _FakeWebSocket(
        [
            '{"broken":"' + payload_secret + '"',
            json.dumps({"jsonrpc": "2.0", "id": id_secret, "method": method_secret, "params": {}}),
        ],
        close_reason=close_secret,
    )
    caplog.set_level(logging.INFO, logger=ws_mod._log.name)

    asyncio.run(ws_mod.handle_ws(ws))

    ping_fail_ws = _FakeWebSocket(
        [json.dumps({"jsonrpc": "2.0", "id": id_secret, "method": "gateway.ping", "params": {}})],
        fail_send_at=2,
    )
    asyncio.run(ws_mod.handle_ws(ping_fail_ws))

    monkeypatch.setattr(server, "dispatch", lambda *_args, **_kwargs: {"jsonrpc": "2.0", "result": {"ok": True}})
    response_fail_ws = _FakeWebSocket(
        [json.dumps({"jsonrpc": "2.0", "id": id_secret, "method": method_secret, "params": {}})],
        fail_send_at=2,
    )
    asyncio.run(ws_mod.handle_ws(response_fail_ws))

    assert payload_secret not in caplog.text
    assert id_secret not in caplog.text
    assert method_secret not in caplog.text
    assert close_secret not in caplog.text


@pytest.mark.parametrize("marker_state", ["regular", "symlink", "swap"])
def test_read_only_ws_never_touches_dashboard_marker(monkeypatch, tmp_path, marker_state):
    effects = _patch_effects(monkeypatch)
    marker = tmp_path / "dashboard_clients.heartbeat"
    target = tmp_path / "sentinel"
    target.write_text("unchanged", encoding="utf-8")
    marker.write_text("marker", encoding="utf-8")
    on_receive = None

    def swap_to_symlink():
        marker.unlink()
        marker.symlink_to(target)

    if marker_state == "symlink":
        swap_to_symlink()
    elif marker_state == "swap":
        on_receive = swap_to_symlink

    target_before = target.stat().st_mtime_ns

    def touch_marker(*, force=False):
        effects.append(f"dashboard_marker:{force}")
        scale_to_zero.touch_dashboard_client_heartbeat(marker)

    monkeypatch.setattr(ws_mod, "_note_dashboard_client_activity", touch_marker)
    ws = _FakeWebSocket(
        [json.dumps({"jsonrpc": "2.0", "id": "ping", "method": "gateway.ping", "params": {}})],
        on_receive=on_receive,
    )

    asyncio.run(ws_mod.handle_ws(ws, read_only=True))

    assert not any(effect.startswith("dashboard_marker") for effect in effects)
    assert target.read_text(encoding="utf-8") == "unchanged"
    assert target.stat().st_mtime_ns == target_before
