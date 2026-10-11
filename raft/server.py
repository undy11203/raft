
from dataclasses import asdict
from typing import Callable

from aiohttp import web

from raft.messages import AppendEntriesRequest, RequestVoteRequest
from raft.node import Node


class HttpServer:
    node: Node
    host: str
    port: int
    runner: web.AppRunner

    def __init__(self, node: Node, host: str, port: int) -> None:
        self.node = node
        self.host = host
        self.port = port

        app = web.Application()
        app.add_routes([
            web.post("/raft/request-vote", self.request_vote),
            web.post("/raft/append-entries", self.append_entries),
            # web.get("/status", self.status),
        ])
        self.runner = web.AppRunner(app, access_log=None)

    async def start(self) -> None:
        await self.runner.setup()
        await web.TCPSite(self.runner, self.host, self.port).start()

    async def stop(self) -> None:
        await self.runner.cleanup()

    async def request_vote(self, request: web.Request) -> web.Response:
        req = await self.read_message(request, RequestVoteRequest)
        return web.json_response(asdict(self.node.handle_request_vote(req)))

    async def append_entries(self, request: web.Request) -> web.Response:
        req = await self.read_message(request, AppendEntriesRequest)
        return web.json_response(asdict(self.node.handle_append_entries(req)))

    # async def status(self, request: web.Request) -> web.Response:
    #     return web.json_response(self.node.status())

    async def read_message[T](self, request: web.Request, message: Callable[..., T]) -> T:
        try:
            return message(**await request.json())
        except (ValueError, TypeError) as e:
            raise web.HTTPBadRequest(text=f"bad request: {e}") from e

