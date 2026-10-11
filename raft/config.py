import argparse
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    node_id: int
    host: str
    port: int
    peers: dict[int, str] # host:port всех узлов кластера
    data_dir: Path # путь до директории с журналами и всяким
    election_timeout: tuple[float, float] # диапозон когда стоит проводить выборы
    heartbeat_interval: float # heartbeat
    rpc_timeout: float # end response time
    log_level: str = "INFO"

def parse_args(argv: list[str] | None = None) -> Config:
    parser = argparse.ArgumentParser(prog="Raft'овый узел", description="Один из узлов")
    parser.add_argument("--id", type=int, required=True, help="id этого узла")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", nargs="*", default=[], metavar="ID=HOST:PORT",
                        help="остальные узлы кластера, например 2=127.0.0.1:8002 3=127.0.0.1:8003")
    parser.add_argument("--data-dir", type=Path,
                        help="папка для данных (по умолчанию data/node<ID>)")
    parser.add_argument("--election-timeout", type=int, nargs=2, default=[150, 300],
                        metavar=("MIN_MS", "MAX_MS"), help="Временной диапозон выбора лидера")
    parser.add_argument("--heartbeat", type=int, default=50, metavar="MS", help="Частота проверки связи")
    parser.add_argument("--rpc-timeout", type=int, default=100, metavar="MS", help="Через сколько решать что узел упал")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    args = parser.parse_args(argv)

    peers: dict[int, str] = {}
    for peer in args.peers:
        try:
            peer_id_str, address = peer.split("=", 1)
            peer_id = int(peer_id_str)
            _, port = address.rsplit(":", 1)
            int(port)
        except ValueError:
            parser.error(f"неверный пир {peer}, нужен формат ID=HOST:PORT")
        if peer_id == args.id:
            parser.error(f"в --peers указан собственный id {peer_id}")
        peers[peer_id] = address

    min_ms, max_ms = args.election_timeout
    if not 0 < min_ms <= max_ms:
        parser.error("--election-timeout: нужно 0 < MIN_MS <= MAX_MS")

    return Config(
        node_id=args.id,
        host=args.host,
        port=args.port,
        peers=peers,
        data_dir=args.data_dir or Path("data") / f"node{args.id}",
        election_timeout=(min_ms / 1000, max_ms / 1000),
        heartbeat_interval=args.heartbeat / 1000,
        rpc_timeout=args.rpc_timeout / 1000,
        log_level=args.log_level,
    )
