"""Проверка окружения: всё, что понадобится для Raft, установлено и работает."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

import aiohttp
from aiohttp import web


def check(ok: bool, name: str) -> None:
    print(f"[ OK ] {name}" if ok else f"[FAIL] {name}")
    if not ok:
        raise SystemExit(1)


@dataclass
class Ping:
    term: int
    node_id: int


def check_argparse() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0, help="HTTP port (0 = any free)")
    args = parser.parse_args()
    check(isinstance(args.port, int), "argparse")
    return args.port


def check_files() -> None:
    with tempfile.TemporaryDirectory(prefix="raft-check-") as tmp:
        path = Path(tmp) / "state.json"
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps({"term": 1}), encoding="utf-8")
        os.replace(tmp_path, path)  # атомарная замена файла
        data = json.loads(path.read_text(encoding="utf-8"))
    check(data == {"term": 1}, "pathlib + json + os.replace")


def check_dataclass_json() -> None:
    original = Ping(term=3, node_id=1)
    restored = Ping(**json.loads(json.dumps(asdict(original))))
    check(restored == original, "dataclass <-> JSON")


# Поднимает HTTP-сервер на aiohttp, шлёт ему запрос клиентом aiohttp,
# проверяет таймер asyncio и ошибку при обращении к мёртвому узлу.
async def check_aiohttp(port: int) -> None:
    # asyncio: таймер
    fired = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, fired.set)

    # aiohttp: сервер
    async def handle_ping(request: web.Request) -> web.Response:
        ping = Ping(**await request.json())
        return web.json_response(asdict(Ping(term=ping.term + 1, node_id=ping.node_id)))

    app = web.Application()
    app.router.add_post("/ping", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    real_port = runner.addresses[0][1]
    url = f"http://127.0.0.1:{real_port}/ping"

    timeout = aiohttp.ClientTimeout(total=1)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # aiohttp: клиент
        async with session.post(url, json=asdict(Ping(term=5, node_id=2))) as resp:
            status = resp.status
            reply = Ping(**await resp.json())
        await asyncio.wait_for(fired.wait(), timeout=1)

        # узел «упал» — клиент должен получить ошибку, а не зависнуть
        await runner.cleanup()
        try:
            async with session.post(url, json={}):
                dead_node_error = False
        except aiohttp.ClientError:
            dead_node_error = True

    check(fired.is_set(), "asyncio (call_later)")
    check(status == 200 and reply == Ping(term=6, node_id=2),
          f"aiohttp server + client (127.0.0.1:{real_port})")
    check(dead_node_error, "aiohttp client error on dead node")


def main() -> None:
    print(f"Python {sys.version.split()[0]}, aiohttp {aiohttp.__version__}")
    port = check_argparse()
    check_files()
    check_dataclass_json()
    asyncio.run(check_aiohttp(port))
    print("Environment OK.")


if __name__ == "__main__":
    main()
