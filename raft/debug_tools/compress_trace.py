"""Сжатие простоев event loop без изменения общей временной шкалы."""

import argparse
import json
from collections import defaultdict
from pathlib import Path


def _compress_process(events: list[dict], gap_us: float) -> list[dict]:
    steps = sorted(
        (
            event
            for event in events
            if event.get("ph") == "X"
            and event.get("cat") == "loop"
        ),
        key=lambda event: event["ts"],
    )

    metadata = [
        event
        for event in events
        if event.get("ph") != "X"
    ]

    if not steps:
        return events.copy()

    gaps = []
    prev_end = None
    shift = 0.0

    # Ищем промежутки между шагами event loop.
    for step in steps:
        start = step["ts"]
        end = start + step["dur"]

        if prev_end is not None and start - prev_end > gap_us:
            raw_start = prev_end
            raw_end = start
            removed = raw_end - raw_start - gap_us

            gaps.append((
                raw_start,
                raw_end,
                shift,
                removed,
            ))

            shift += removed

        prev_end = (
            end
            if prev_end is None
            else max(prev_end, end)
        )

    def map_time(value: float) -> float:
        """Перевести исходное время на сжатую шкалу."""

        for raw_start, raw_end, shift_before, removed in gaps:
            if value <= raw_start:
                return value - shift_before

            if value < raw_end:
                fraction = (
                    (value - raw_start)
                    / (raw_end - raw_start)
                )

                return (
                    raw_start
                    - shift_before
                    + fraction * gap_us
                )

        return value - shift

    out = metadata.copy()

    # Оставляем короткие маркеры вместо длинных простоев.
    for raw_start, raw_end, shift_before, _ in gaps:
        idle_ms = (raw_end - raw_start) / 1000

        out.append({
            "name": f"простой {idle_ms:.1f} мс",
            "cat": "idle",
            "ph": "X",
            "ts": raw_start - shift_before,
            "dur": gap_us,
            "pid": steps[0]["pid"],
            "tid": 1,
        })

    # Начало и конец каждого события переводим по одной шкале.
    # Поэтому корутина, пересекающая простой, остаётся целым событием.
    for event in events:
        if event.get("ph") != "X":
            continue

        start = map_time(event["ts"])
        end = map_time(event["ts"] + event["dur"])

        out.append({
            **event,
            "ts": start,
            "dur": max(0.0, end - start),
        })

    return sorted(
        out,
        key=lambda event: (
            event.get("ts", 0),
            event.get("tid", 0),
        ),
    )


def compress(events: list[dict], gap_us: float) -> list[dict]:
    # У каждого процесса своя временная шкала.
    by_proc = defaultdict(list)

    for event in events:
        by_proc[event.get("pid")].append(event)

    out = []

    for process_events in by_proc.values():
        out.extend(
            _compress_process(process_events, gap_us)
        )

    return out


def compress_file(
    src: str | Path,
    dst: str | Path,
    gap_us: float = 300,
) -> None:
    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    data["traceEvents"] = compress(
        data["traceEvents"],
        gap_us,
    )

    with open(dst, "w", encoding="utf-8") as f:
        json.dump(data, f)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument(
        "--gap-us",
        type=float,
        default=300,
        help="Ширина сжатого простоя в микросекундах",
    )

    args = parser.parse_args()

    compress_file(
        args.src,
        args.dst,
        args.gap_us,
    )


if __name__ == "__main__":
    main()