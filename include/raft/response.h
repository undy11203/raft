#pragma once

#include "raft/types.h"

struct AppendEntriesResponse {
    Term term;      // currentTerm получателя, чтобы лидер обновил свою
    bool success;   // true, если у follower'а совпала запись prevLogIndex/prevLogTerm
};

struct RequestVoteResponse {
    Term term;          // currentTerm голосующего, чтобы кандидат узнал, если устарел
    bool voteGranted;   // true = голос отдан
};