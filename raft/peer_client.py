from dataclasses import asdict
from typing import Any, Callable

import aiohttp

from .messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)

class HttpPeerClient:
    peers: dict[int, str]
    timeout: aiohttp.ClientTimeout
    session: aiohttp.ClientSession | None

    def __init__(self, peers: dict[int, str], timeout: float):
        self.peers = peers
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(timeout=self.timeout)

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()

    async def request_vote(self, peer_id: int, req: RequestVoteRequest) -> RequestVoteResponse | None:
        return await self.call(peer_id, "/raft/request-vote", req, RequestVoteResponse)

    async def append_entries(self, peer_id: int, req: AppendEntriesRequest) -> AppendEntriesResponse | None:
        return await self.call(peer_id, "/raft/append-entries", req, AppendEntriesResponse)

    # async def call[T](self, peer_id: int, path: str, req: Any, response: Callable[..., T]) -> T | None:
    async def call(self, peer_id: int, path: str, req: Any, response):
        assert self.session is not None
        url = f"http://{self.peers[peer_id]}{path}"
        try:
            async with self.session.post(url, json=asdict(req)) as resp:
                if resp.status != 200:
                    return None
                return response(**await resp.json())
        except (aiohttp.ClientError, TimeoutError, ValueError, TypeError):
            return None
