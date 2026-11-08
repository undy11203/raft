"""Сжать простои event loop'а в трассе от looptrace.py.

Каждая пауза между шагами loop'а длиннее порога укорачивается до порога,
а на её место ставится слайс «простой N мс» с настоящей длительностью в подписи.
Вызовы Raft внутри шага сдвигаются вместе со своим шагом.

    python compress_trace.py trace1.json trace1_compact.json [--gap-us 300]
"""
import argparse
import json
from collections import defaultdict
from pathlib import Path


def compress(events: list[dict], gap_us: float) -> list[dict]:
    by_proc = defaultdict(list)
    for e in events:
        by_proc[(e.get("pid"), e.get("tid"))].append(e)

    out = []
    for (pid, tid), evs in by_proc.items():
        steps = sorted((e for e in evs if e.get("cat") != "raft"), key=lambda e: e["ts"])
        calls = sorted((e for e in evs if e.get("cat") == "raft"), key=lambda e: e["ts"])
        shift, prev_end, ci = 0.0, None, 0
        for s in steps:
            if prev_end is not None and s["ts"] - prev_end > gap_us:
                idle = s["ts"] - prev_end
                out.append({"name": f"простой {idle / 1000:.1f} мс", "cat": "idle", "ph": "X",
                            "ts": prev_end - shift, "dur": gap_us, "pid": pid, "tid": tid})
                shift += idle - gap_us
            end = s["ts"] + s["dur"]
            out.append({**s, "ts": s["ts"] - shift})
            while ci < len(calls) and calls[ci]["ts"] <= end:   # вызовы внутри этого шага
                out.append({**calls[ci], "ts": calls[ci]["ts"] - shift})
                ci += 1
            prev_end = end
        out.extend({**c, "ts": c["ts"] - shift} for c in calls[ci:])
    return out


def compress_file(src: str | Path, dst: str | Path, gap_us: float = 300) -> None:
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
    data["traceEvents"] = compress(data["traceEvents"], gap_us)
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(data, f)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--gap-us", type=float, default=300, help="ширина сжатого простоя, мкс")
    args = ap.parse_args()
    compress_file(args.src, args.dst, args.gap_us)


if __name__ == "__main__":
    main()