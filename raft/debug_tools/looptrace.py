"""Трассировка event loop'а в формате Chrome Trace (https://ui.perfetto.dev).

Сохраняются два файла:
- trace.json — полная техническая трасса, где у каждой корутины своя дорожка;
- trace_readable.json — прикладная трасса с постоянными логическими дорожками.

В читаемой версии вложенные вызовы остаются на одной дорожке, а новые задачи
RPC получают дорожку своего peer. Время await входит в интервал корутины.

Включение: RAFT_LOOPTRACE=trace.json python -m raft ...   Остановка: Ctrl+C.
По умолчанию служебные шаги asyncio/aiohttp без кода Raft скрыты;
RAFT_LOOPTRACE_ALL=1 показывает все шаги в полной трассе.
"""

import asyncio
import atexit
import contextvars
import functools
import inspect
import json
import os
import time
from asyncio import events
from pathlib import Path
from typing import Any


# Полная техническая трассировка.
_events: list[dict] = []
_pid = os.getpid()
_raft_calls = 0
_next_tid = 2
_current_coroutine: contextvars.ContextVar[tuple[int, str] | None] = (
    contextvars.ContextVar("raft_trace_coroutine", default=None)
)

# Читаемая трассировка. Контекст содержит Task и Handle, чтобы отличать
# настоящий вложенный вызов от новой задачи, скопировавшей контекст родителя.
_readable_events: list[dict] = []
_readable_lanes: dict[str, list[int]] = {}
_readable_busy: set[int] = set()
_next_readable_tid = 1
_readable_initialized = False

_ReadableContext = tuple[asyncio.Task | None, events.Handle | None, int]
_current_readable: contextvars.ContextVar[_ReadableContext | None] = (
    contextvars.ContextVar("raft_trace_readable", default=None)
)
_current_handle: contextvars.ContextVar[events.Handle | None] = (
    contextvars.ContextVar("raft_trace_handle", default=None)
)


def _now_us() -> float:
    return time.perf_counter() * 1e6


def _label(handle: events.Handle) -> str:
    cb = handle._callback
    owner = getattr(cb, "__self__", None)
    if isinstance(owner, asyncio.Task):
        coro = owner.get_coro()
        return f"task: {getattr(coro, '__qualname__', type(coro).__name__)}"
    return f"callback: {getattr(cb, '__qualname__', type(cb).__name__)}"


def _emit(
    name: str,
    start: float,
    end: float,
    cat: str,
    tid: int = 1,
    args: dict[str, Any] | None = None,
    target: list[dict] | None = None,
) -> None:
    if target is None:
        target = _events

    event = {
        "name": name,
        "cat": cat,
        "ph": "X",
        "ts": start,
        "dur": max(0.0, end - start),
        "pid": _pid,
        "tid": tid,
    }
    if args:
        event["args"] = args
    target.append(event)


def _new_track(name: str) -> int:
    """Создать дорожку полной трассы для одного вызова корутины."""
    global _next_tid
    tid = _next_tid
    _next_tid += 1
    _events.append({
        "name": "thread_name",
        "ph": "M",
        "pid": _pid,
        "tid": tid,
        "args": {"name": name},
    })
    _events.append({
        "name": "thread_sort_index",
        "ph": "M",
        "pid": _pid,
        "tid": tid,
        "args": {"sort_index": tid},
    })
    return tid


def _new_readable_track(name: str, key: str) -> int:
    global _next_readable_tid
    tid = _next_readable_tid
    _next_readable_tid += 1
    _readable_lanes.setdefault(key, []).append(tid)
    _readable_events.append({
        "name": "thread_name",
        "ph": "M",
        "pid": _pid,
        "tid": tid,
        "args": {"name": name},
    })
    _readable_events.append({
        "name": "thread_sort_index",
        "ph": "M",
        "pid": _pid,
        "tid": tid,
        "args": {"sort_index": tid},
    })
    return tid


def _init_readable_tracks() -> None:
    """Создать основные дорожки заранее, чтобы их порядок был стабильным."""
    global _readable_initialized
    if _readable_initialized:
        return
    _new_readable_track("Main / lifecycle", "lifecycle")
    _new_readable_track("Raft / timers", "timers")
    _new_readable_track("HTTP server", "http")
    _new_readable_track("Client API", "client")
    _readable_initialized = True


def _base_readable_tid(key: str) -> int:
    return _readable_lanes[key][0]


def _acquire_readable_track(key: str, label: str) -> int:
    """Занять свободную дорожку, создав новую только при перекрытии."""
    lanes = _readable_lanes.setdefault(key, [])
    for tid in lanes:
        if tid not in _readable_busy:
            _readable_busy.add(tid)
            return tid

    name = label if not lanes else f"{label} #{len(lanes) + 1}"
    tid = _new_readable_track(name, key)
    _readable_busy.add(tid)
    return tid


def _release_readable_track(tid: int) -> None:
    _readable_busy.discard(tid)


def _set_readable_context(tid: int):
    return _current_readable.set((
        asyncio.current_task(),
        _current_handle.get(),
        tid,
    ))


def _active_readable_tid() -> int | None:
    """Вернуть дорожку только для настоящего вложенного выполнения."""
    current = _current_readable.get()
    if current is None:
        return None

    owner_task, owner_handle, tid = current
    task = asyncio.current_task()
    if task is not None:
        return tid if owner_task is task else None

    # Вне Task выполняется callback Handle. Новый callback не должен
    # унаследовать дорожку того места, где он был зарегистрирован.
    if owner_task is None and owner_handle is _current_handle.get():
        return tid
    return None


def _track_group(fn, details: dict[str, Any]) -> tuple[str, str]:
    qualname = fn.__qualname__
    owner, _, method = qualname.rpartition(".")

    if owner == "HttpPeerClient" and method in ("start", "close"):
        return "lifecycle", "Main / lifecycle"
    if owner == "HttpServer" and method in ("start", "stop"):
        return "lifecycle", "Main / lifecycle"
    if owner == "HttpServer" and method in ("status", "add_entry", "show_entry"):
        return "client", "Client API"
    if owner == "HttpServer":
        return "http", "HTTP server"

    peer_id = details.get("peer_id")
    if peer_id is not None and (
        owner == "HttpPeerClient"
        or (owner == "Node" and method.startswith("send_"))
    ):
        return f"rpc:{peer_id}", f"RPC → node {peer_id}"

    return "timers", "Raft / timers"


def _call_details(fn, args, kwargs) -> tuple[str, dict[str, Any]]:
    """Добавить peer_id/path и понятное имя spawn, не сохраняя HTTP-объекты."""
    name = fn.__qualname__
    details: dict[str, Any] = {}

    try:
        bound = inspect.signature(fn).bind_partial(*args, **kwargs)
        for key in ("peer_id", "path"):
            value = bound.arguments.get(key)
            if isinstance(value, (str, int, float, bool)):
                details[key] = value

        if fn.__qualname__ == "Node.spawn":
            coro = bound.arguments.get("coro")
            frame = getattr(coro, "cr_frame", None)
            frame_locals = frame.f_locals if frame is not None else {}

            # _trace_async — async-обёртка, поэтому coroutine ссылается на её
            # frame. Оригинальная функция и её аргументы лежат в closure/frame.
            spawned_fn = frame_locals.get("fn")
            spawned_name = getattr(
                spawned_fn,
                "__qualname__",
                getattr(coro, "__qualname__", type(coro).__name__),
            )
            short_name = spawned_name.rsplit(".", 1)[-1]
            details["coroutine"] = spawned_name

            spawned_args = frame_locals.get("args", ())
            spawned_kwargs = frame_locals.get("kwargs", {})
            if callable(spawned_fn):
                try:
                    spawned_bound = inspect.signature(spawned_fn).bind_partial(
                        *spawned_args,
                        **spawned_kwargs,
                    )
                    peer_id = spawned_bound.arguments.get("peer_id")
                    if isinstance(peer_id, (str, int)):
                        details["peer_id"] = peer_id
                except (TypeError, ValueError):
                    pass

            name = f"spawn → {short_name}"

    except (TypeError, ValueError):
        pass

    if "peer_id" in details:
        name = f"{name}(peer={details['peer_id']})"
    return name, details


def _trace_sync(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _raft_calls
        name, details = _call_details(fn, args, kwargs)

        readable_tid = _active_readable_tid()
        readable_token = None
        if readable_tid is None:
            key = (
                "lifecycle"
                if fn.__qualname__ in ("Node.start", "Node.stop")
                else "timers"
            )
            readable_tid = _base_readable_tid(key)
            readable_token = _set_readable_context(readable_tid)

        start = _now_us()
        try:
            return fn(*args, **kwargs)
        finally:
            end = _now_us()
            _raft_calls += 1

            current = _current_coroutine.get()
            raw_tid = current[0] if current is not None else 1
            _emit(name, start, end, "raft", raw_tid, details)
            _emit(
                name,
                start,
                end,
                "raft",
                readable_tid,
                details,
                target=_readable_events,
            )

            if readable_token is not None:
                _current_readable.reset(readable_token)

    return wrapper


def _trace_async(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        global _raft_calls
        name, details = _call_details(fn, args, kwargs)

        raw_tid = _new_track(name)
        raw_token = _current_coroutine.set((raw_tid, name))

        readable_tid = _active_readable_tid()
        readable_token = None
        acquired_track = False
        if readable_tid is None:
            key, label = _track_group(fn, details)
            readable_tid = _acquire_readable_track(key, label)
            readable_token = _set_readable_context(readable_tid)
            acquired_track = True

        start = _now_us()
        try:
            return await fn(*args, **kwargs)
        finally:
            end = _now_us()
            _raft_calls += 1

            _emit(name, start, end, "coroutine", raw_tid, details)
            _emit(
                name,
                start,
                end,
                "coroutine",
                readable_tid,
                details,
                target=_readable_events,
            )

            if readable_token is not None:
                _current_readable.reset(readable_token)
            if acquired_track:
                _release_readable_track(readable_tid)
            _current_coroutine.reset(raw_token)

    return wrapper


def trace_methods(cls: type) -> None:
    """Обернуть синхронные и асинхронные методы класса."""
    for name, fn in list(vars(cls).items()):
        if name.startswith("__") or not inspect.isfunction(fn):
            continue
        if getattr(fn, "__raft_traced__", False):
            continue
        wrapped = _trace_async(fn) if inspect.iscoroutinefunction(fn) else _trace_sync(fn)
        wrapped.__raft_traced__ = True
        setattr(cls, name, wrapped)


def _write_trace(path: str | Path, events_list: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"traceEvents": events_list, "displayTimeUnit": "ms"}, f)


def enable(path: str | Path, *classes: type) -> None:
    _init_readable_tracks()
    original_run = events.Handle._run
    show_all = bool(os.environ.get("RAFT_LOOPTRACE_ALL"))

    def _run(self):
        current = _current_coroutine.get()
        tid = current[0] if current is not None else 1
        label = current[1] if current is not None else _label(self)
        calls_before = _raft_calls
        start = _now_us()
        handle_token = _current_handle.set(self)
        try:
            original_run(self)
        finally:
            if show_all or current is not None or _raft_calls != calls_before:
                _emit(label, start, _now_us(), "loop", tid)
            _current_handle.reset(handle_token)

    events.Handle._run = _run
    for cls in classes:
        trace_methods(cls)

    def save() -> None:
        source = Path(path)
        readable_path = source.with_name(f"{source.stem}_readable{source.suffix}")
        _write_trace(source, _events)
        _write_trace(readable_path, _readable_events)

    atexit.register(save)