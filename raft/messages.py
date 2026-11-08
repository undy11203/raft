from dataclasses import dataclass
from enum import StrEnum

# =====
# RPC
# =====

@dataclass
class LogEntry:
    term: int
    command: str


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int = 0

    def __post_init__(self):
        self.entries = [
            LogEntry(**entry) if isinstance(entry, dict) else entry
            for entry in self.entries
        ]

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool

# =====
# Rest
# =====

class StatusCommand(StrEnum):
    OK = 'ok'
    NOT_OK = 'not ok'

@dataclass
class Status:
    term: int
    leader_id: int | None
    role: str

@dataclass
class AddEntry:
    command: str

@dataclass
class AddEntryResponse:
    status: StatusCommand
    