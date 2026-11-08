"""Точка входа: python -m raft --id 1 --port 8001 --peers 2=127.0.0.1:8002 3=127.0.0.1:8003"""

import asyncio
import atexit
import contextlib
import logging
import os
import sys
from pathlib import Path

from raft.config import Config, parse_args
from raft.debug_tools import compress_trace, looptrace
from raft.node import Node
from raft.peer_client import HttpPeerClient
from raft.server import HttpServer

log = logging.getLogger("raft")

async def run(config: Config) -> int:
    peerClient = HttpPeerClient(config.peers, config.rpc_timeout)
    node = Node(config, peerClient)
    server = HttpServer(node, config.host, config.port)

    await peerClient.start()
    try:
        node.start()
        try:
            await server.start()
        except OSError as e:
            log.error("cannot listen on %s:%s : %s", config.host, config.port, e)
            return 1
        log.info("listening on http://%s:%s", config.host, config.port)
        await asyncio.Event().wait()
        return 0
    finally:
        node.stop()
        await server.stop()
        await peerClient.close()


def main() -> None:
    config = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s.%(msecs)03d [node {config.node_id}] %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("raft").setLevel(config.log_level)
    # logging.getLogger("raft").setLevel("DEBUG")
    trace_path = os.environ.get("RAFT_LOOPTRACE")
    if trace_path:
        source = Path(trace_path)
        compact_path = source.with_name(f"{source.stem}_compact{source.suffix}")
        # atexit выполняет обработчики в обратном порядке: сначала сохранить, потом сжать.
        atexit.register(compress_trace.compress_file, source, compact_path)
        looptrace.enable(trace_path, Node)
    with contextlib.suppress(KeyboardInterrupt):
        sys.exit(asyncio.run(run(config)))


if __name__ == "__main__":
    main()
