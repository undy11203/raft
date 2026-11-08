

from ast import Assert
import asyncio
from enum import StrEnum
import logging
import random
from typing import Any, Coroutine

from raft.config import Config
from raft.messages import AddEntry, AddEntryResponse, AppendEntriesRequest, AppendEntriesResponse, LogEntry, RequestVoteRequest, RequestVoteResponse, Status, StatusCommand
from raft.peer_client import HttpPeerClient

log = logging.getLogger(__name__)

class Role(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class Node:
    #step 1
    config: Config
    id: int
    peers: dict[int, str]
    peerClient: HttpPeerClient
    loop: asyncio.AbstractEventLoop
    current_term: int
    voted_for: int | None
    role: Role
    leader_id: int | None
    votes: set[int]
    election_timer: asyncio.TimerHandle | None
    heartbeat_timer: asyncio.TimerHandle | None
    tasks: set[asyncio.Task[None]]

    #step2
    log: list[LogEntry]
    commit_index: int
    last_applied: int
    next_index: dict[int, int]
    match_index: dict[int, int]

    def __init__(self, config: Config, peerClient: HttpPeerClient) -> None:
        self.config = config
        self.id = config.node_id
        self.peers = config.peers
        self.peerClient = peerClient
        self.loop = asyncio.get_running_loop()

        self.current_term = 0
        self.voted_for: int | None = None

        # Изменчивое состояние
        self.role = Role.FOLLOWER
        self.leader_id: int | None = None
        self.votes: set[int] = set()

        self.election_timer: asyncio.TimerHandle | None = None
        self.heartbeat_timer: asyncio.TimerHandle | None = None
        self.tasks: set[asyncio.Task[None]] = set()

        self.log: list[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0

        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

    def start(self) -> None:
        self.current_term = 0
        self.voted_for = None
        self.role = Role.FOLLOWER
        log.info("started as follower: term %d, voted_for %s, peers %s",
                 self.current_term, self.voted_for, sorted(self.peers))
        self.reset_election_timer()


    def stop(self) -> None:
        log.debug("stopping: term %d, %s", self.current_term, self.role)
        self.cancel_election_timer()
        self.cancel_heartbeat_timer()
        for task in self.tasks:
            task.cancel()

    def handle_request_vote(self, req: RequestVoteRequest):
        log.debug("RequestVote from %d: term %d (my term %d, voted_for %s)",
                  req.candidate_id, req.term, self.current_term, self.voted_for)
        if (req.term < self.current_term):
            log.debug("term %d: reject vote for %d: stale term %d",
                      self.current_term, req.candidate_id, req.term)
            return RequestVoteResponse(term=self.current_term, vote_granted=False)
        
        if (req.term > self.current_term):
            self.become_follower(req.term)        

        can_vote = self.voted_for is None
        if can_vote:
            self.voted_for = req.candidate_id
            self.reset_election_timer()
            log.info("term %d: voted for %d", self.current_term, req.candidate_id)
            return RequestVoteResponse(term=self.current_term, vote_granted=True)
        log.debug("term %d: reject vote for %d: already voted for %s",
                  self.current_term, req.candidate_id, self.voted_for)
        return RequestVoteResponse(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, req: AppendEntriesRequest):
        log.debug("AppendEntries from %d: term %d (my term %d)",
                  req.leader_id, req.term, self.current_term)
        if req.term < self.current_term:
            log.debug("term %d: reject AppendEntries from %d: stale term %d",
                      self.current_term, req.leader_id, req.term)
            return AppendEntriesResponse(term=self.current_term, success=False)

        print(req)
        
        self.become_follower(req.term)

        if self.leader_id != req.leader_id:
            log.info("term %d: leader is %d", self.current_term, req.leader_id)
        self.leader_id = req.leader_id

        self.reset_election_timer()

        if req.prev_log_index > len(self.log):
            return AppendEntriesResponse(
                term=self.current_term,
                success=False,
            )

        if self.term_at(req.prev_log_index) != req.prev_log_term:
            return AppendEntriesResponse(
                term=self.current_term,
                success=False,
            )

        for offset, entry in enumerate(req.entries):
            index = req.prev_log_index + offset + 1

            if index <= len(self.log):
                if self.term_at(index) == entry.term:
                    continue

                del self.log[index - 1:]

            self.log.append(entry)

        if req.leader_commit > self.commit_index:
            last_index_in_request = (
                req.prev_log_index + len(req.entries)
            )
            self.commit_index = min(
                req.leader_commit,
                last_index_in_request,
            )
            self.apply_committed_entries()

        return AppendEntriesResponse(term=self.current_term, success=True)

    def term_at(self, i: int):
        if i == 0:
            return 0
        return self.log[i - 1].term

    def handle_add_entry(self, req: AddEntry):
        if self.role != Role.LEADER:
            return AddEntryResponse(StatusCommand.NOT_OK)
        
        entry = LogEntry(
            term=self.current_term,
            command=req.command,
        )
        self.log.append(entry)
        
        for peer_id in self.peers:
            rpc_req = self.make_append_entries_request(peer_id)
            self.spawn(self.send_append_entries(peer_id, rpc_req))
        return AddEntryResponse(StatusCommand.OK)

    def become_follower(self, term: int):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.leader_id = None 
        if self.role == Role.LEADER:
            self.cancel_heartbeat_timer()
            self.reset_election_timer()
        self.role = Role.FOLLOWER
        self.votes = set()

        log.debug("%s: %s", self.current_term, self.role)

    def become_candidate(self):
        old_role = self.role
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        log.debug("term %d: %s -> candidate, requesting votes from %s",
                  self.current_term, old_role, sorted(self.peers))
        self.leader_id = None
        self.reset_election_timer()

        last_log_index = len(self.log)
        last_log_term = self.term_at(last_log_index)
        req = RequestVoteRequest(term=self.current_term, 
                                 candidate_id=self.id, 
                                 last_log_index=last_log_index, 
                                 last_log_term=last_log_term)
        for peer_id in self.peers:
            self.spawn(self.send_request_vote(peer_id, req))
        self.check_majority()

    def reset_election_timer(self):
        self.cancel_election_timer()
        timeout = random.uniform(self.config.election_timeout[0],
                                 self.config.election_timeout[1])
        log.debug("election timer reset: %.0f ms", timeout * 1000)
        self.election_timer = self.loop.call_later(timeout, self.on_election_timeout)

    def cancel_election_timer(self) -> None:
        if self.election_timer is not None:
            self.election_timer.cancel()
            self.election_timer = None

    def cancel_heartbeat_timer(self) -> None:
        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

    def on_election_timeout(self) -> None:
        if self.role != Role.LEADER:
            log.debug("term %d: election timeout, no leader heard (%s)", self.current_term, self.role)
            self.become_candidate()

    def spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = self.loop.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.on_task_done)

    def on_task_done(self, task: asyncio.Task[None]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("background task failed", exc_info=task.exception())

    async def send_request_vote(self, peer_id: int, req: RequestVoteRequest) -> None:
        resp = await self.peerClient.request_vote(peer_id, req)
        log.info("request_vote: %d, %s", peer_id, req)
        if resp is not None:
            self.handle_request_vote_response(peer_id, req, resp)
        else:
            log.debug("RequestVote to %d: no response", peer_id)

    def handle_request_vote_response(self, peer_id: int, req: RequestVoteRequest,
                                     resp: RequestVoteResponse) -> None:
        log.debug("vote response from %d: term %d, granted %s",
                  peer_id, resp.term, resp.vote_granted)
        if resp.term > self.current_term:
            log.debug("term %d: higher term %d from %d in vote response",
                      self.current_term, resp.term, peer_id)
            self.become_follower(resp.term)
            return
        if self.role != Role.CANDIDATE or req.term != self.current_term:
            log.debug("ignore stale vote response from %d (req term %d, now term %d, %s)",
                      peer_id, req.term, self.current_term, self.role)
            return
        if resp.vote_granted:
            self.votes.add(peer_id)
            log.debug("term %d: vote from %d granted (%d/%d)",
                      self.current_term, peer_id, len(self.votes), len(self.peers) + 1)
            self.check_majority()
        else:
            log.debug("term %d: vote from %d denied", self.current_term, peer_id)

    def check_majority(self) -> None:
        cluster_size = len(self.peers)
        if self.role == Role.CANDIDATE and (len(self.votes) > cluster_size // 2):
            self.become_leader()

    def become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.cancel_election_timer()
        log.info("term %d: became leader with votes %s",
                 self.current_term, sorted(self.votes))
        last_index = len(self.log)
        for peer in self.peers:
            self.next_index[peer] = last_index + 1
            self.match_index[peer] = 0
        self.send_heartbeats()

    def send_heartbeats(self) -> None:
        if self.role != Role.LEADER:
            return
        log.debug("-> heartbeat to %s, term %d", sorted(self.peers), self.current_term)
        for peer_id in self.peers:
            req = self.make_append_entries_request(peer_id)
            self.spawn(self.send_append_entries(peer_id, req))
        log.info("send heartbeat: %s, %s", self.peers, req)
        self.heartbeat_timer = self.loop.call_later(self.config.heartbeat_interval,
                                                    self.send_heartbeats)

    async def send_append_entries(self, peer_id: int, req: AppendEntriesRequest) -> None:
        resp = await self.peerClient.append_entries(peer_id, req)
        if resp is None:
            log.debug("AppendEntries to %d: no response", peer_id)
            return
        if resp.term > self.current_term:
            log.debug("term %d: higher term %d from %d, stepping down",
                      self.current_term, resp.term, peer_id)
            self.become_follower(resp.term)
            return
        self.handle_append_entries_response(peer_id, req, resp)

    def handle_append_entries_response(
        self,
        peer_id: int,
        req: AppendEntriesRequest,
        resp: AppendEntriesResponse,
    ) -> None:
        if resp.term > self.current_term:
            self.become_follower(resp.term)
            return

        if self.role != Role.LEADER or req.term != self.current_term:
            return

        if resp.success:
            matched = req.prev_log_index + len(req.entries)

            self.match_index[peer_id] = max(
                self.match_index[peer_id],
                matched,
            )
            self.next_index[peer_id] = (
                self.match_index[peer_id] + 1
            )

            self.check_commit()
            return

        self.next_index[peer_id] = max(
            1,
            self.next_index[peer_id] - 1,
        )

        retry_req = self.make_append_entries_request(peer_id)
        self.spawn(self.send_append_entries(peer_id, retry_req))

    def check_commit(self) -> None:
        cluster_size = len(self.peers) + 1

        for index in range(
            len(self.log),
            self.commit_index,
            -1,
        ):
            if self.log[index - 1].term != self.current_term:
                return

            replicas = 1

            for matched in self.match_index.values():
                if matched >= index:
                    replicas += 1

            if replicas * 2 > cluster_size:
                self.commit_index = index
                self.apply_committed_entries()
                return

    def apply_committed_entries(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]

            log.info(
                "apply index=%d term=%d command=%s",
                self.last_applied,
                entry.term,
                entry.command,
            )

    def status(self):
        return Status(term=self.current_term, leader_id=self.leader_id, role=self.role)

    def make_append_entries_request(
        self,
        peer_id: int,
    ) -> AppendEntriesRequest:
        next_i = self.next_index[peer_id]
        prev_log_index = next_i - 1

        return AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=self.term_at(prev_log_index),
            entries=self.log[next_i - 1:],
            leader_commit=self.commit_index,
        )

    def handle_show_entry(self):
        entries = []

        for index, entry in enumerate(self.log, start=1):
            entries.append({
                "index": index,
                "term": entry.term,
                "command": entry.command,
                "is_committed": index <= self.commit_index,
                "is_applied": index <= self.last_applied,
            })

        return { 
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "entries": entries,
        }