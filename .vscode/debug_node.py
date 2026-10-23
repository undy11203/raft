"""Узел под debugpy в режиме listen — VS Code подключается к нему через attach (см. launch.json).

python .vscode/debug_node.py 5678 --id 1 --port 8001 --peers 2=127.0.0.1:8002 3=127.0.0.1:8003
Первый аргумент — порт отладчика, остальные уходят в raft как есть.
"""

import runpy
import sys
from pathlib import Path

import debugpy

port = int(sys.argv.pop(1))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

debugpy.listen(("127.0.0.1", port))
# эту строку ждёт задача в tasks.json (background.endsPattern): после неё VS Code делает attach
print(f"debugpy: waiting for VS Code on 127.0.0.1:{port}", flush=True)
debugpy.wait_for_client()

runpy.run_module("raft", run_name="__main__", alter_sys=True)
