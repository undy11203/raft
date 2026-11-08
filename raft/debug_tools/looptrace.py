"""Трассировка event loop'а в формате Chrome Trace (открывать в https://ui.perfetto.dev).

Каждый шаг loop'а (колбэк таймера или шаг корутины) — один слайс на общей дорожке.
Методы Node рисуются вложенными слайсами внутри шага, в котором они выполнились.
Включение: RAFT_LOOPTRACE=trace.json python -m raft ...   Остановка: Ctrl+C.
По умолчанию служебные шаги asyncio/aiohttp, в которых не выполнялся код Raft,
скрыты; RAFT_LOOPTRACE_ALL=1 показывает все шаги.
"""
import asyncio
import atexit
import functools
import inspect
import json
import os
import time
from asyncio import events

_events: list[dict] = []
_pid = os.getpid()
_raft_calls = 0  # сколько вызовов Raft было за всё время; по разнице видно, был ли он в шаге


def _now_us() -> float:
    return time.perf_counter() * 1e6


def _label(handle: events.Handle) -> str:
    cb = handle._callback
    owner = getattr(cb, "__self__", None)
    if isinstance(owner, asyncio.Task):          # шаг корутины: показываем её имя
        coro = owner.get_coro()
        return f"task: {getattr(coro, '__qualname__', type(coro).__name__)}"
    return f"callback: {getattr(cb, '__qualname__', type(cb).__name__)}"


def _emit(name: str, start: float, end: float, cat: str) -> None:
    _events.append({"name": name, "cat": cat, "ph": "X", "ts": start,
                    "dur": end - start, "pid": _pid, "tid": 1})


def trace_methods(cls: type) -> None:
    """Обернуть синхронные методы класса: они появятся внутри шагов loop'а."""
    for name, fn in list(vars(cls).items()):
        if name.startswith("__") or not inspect.isfunction(fn) or inspect.iscoroutinefunction(fn):
            continue

        def make(fn=fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                start = _now_us()
                try:
                    return fn(*args, **kwargs)
                finally:
                    global _raft_calls
                    _raft_calls += 1
                    _emit(fn.__name__, start, _now_us(), "raft")
            return wrapper
        setattr(cls, name, make())


def enable(path: str, *classes: type) -> None:
    original_run = events.Handle._run           # через него loop вызывает любой колбэк
    show_all = bool(os.environ.get("RAFT_LOOPTRACE_ALL"))

    def _run(self):
        label = _label(self)                    # до запуска: колбэк может отменить сам себя
        calls_before = _raft_calls
        start = _now_us()
        try:
            original_run(self)
        finally:
            if show_all or _raft_calls != calls_before:
                _emit(label, start, _now_us(), "loop")

    events.Handle._run = _run
    for cls in classes:
        trace_methods(cls)

    def save() -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"traceEvents": _events, "displayTimeUnit": "ms"}, f)

    atexit.register(save)