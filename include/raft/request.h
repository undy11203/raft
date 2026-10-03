#pragma once

#include "raft/types.h"
#include "raft/logEntry.h"

#include <vector>

struct AppendEntriesRequest {
    Term term;                     // term лидера
    NodeId leaderId;               // кто лидер
    Index prevLogIndex;            // индекс записи перед новыми
    Term prevLogTerm;              // её term
    std::vector<LogEntry> entries; // новые записи (пусто = heartbeat)
    Index leaderCommit;            // commitIndex лидера
};

struct RequestVoteRequest {
    Term term;            // term кандидата (он только что сделал currentTerm++)
    NodeId candidateId;   // кто просит голос
    Index lastLogIndex;   // индекс последней записи в журнале кандидата
    Term lastLogTerm;     // term этой записи
};