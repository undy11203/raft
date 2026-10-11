

from dataclasses import asdict, dataclass
from pathlib import Path
import json
import os

@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: int | None = None

class Storage:
    data_dir: Path
    state_file: Path

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.state_file = data_dir / "state.json"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> PersistentState:
        if not self.state_file.exists():
            return PersistentState()
        data = json.loads(self.state_file.read_text(encoding="utf-8"))
        return PersistentState(**data)


    def save(self, state: PersistentState) -> None:
        with self.state_file.open("w", encoding="utf-8") as f:
            json.dump(asdict(state), f)
            f.flush()
            os.fsync(f.fileno())
