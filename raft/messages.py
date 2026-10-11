from dataclasses import dataclass


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

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
