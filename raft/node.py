

import asyncio
from enum import StrEnum
import logging
import random
from typing import Any, Coroutine

from raft.config import Config
from raft.messages import AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, Status
from raft.peer_client import HttpPeerClient

log = logging.getLogger(__name__)

class Role(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class Node:
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
        
        self.become_follower(req.term)

        if self.leader_id != req.leader_id:
            log.info("term %d: leader is %d", self.current_term, req.leader_id)
        self.leader_id = req.leader_id

        self.reset_election_timer()

        return AppendEntriesResponse(term=self.current_term, success=True)

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

        req = RequestVoteRequest(term=self.current_term, 
                                 candidate_id=self.id, 
                                 last_log_index=0, 
                                 last_log_term=0)
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
        self.send_heartbeats()

    def send_heartbeats(self) -> None:
        if self.role != Role.LEADER:
            return
        log.debug("-> heartbeat to %s, term %d", sorted(self.peers), self.current_term)
        req = AppendEntriesRequest(term=self.current_term, 
                                   leader_id=self.id, 
                                   prev_log_index=0,
                                   prev_log_term=0,
                                   entries=[],
                                   leader_commit= 0)
        for peer_id in self.peers:
            self.spawn(self.send_append_entries(peer_id, req))
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

    def status(self):
        return Status(term=self.current_term, leader_id=self.leader_id, role=self.role)



