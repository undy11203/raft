#pragma once

#include "role.h"
#include "storage.h"
#include "logEntry.h"
#include <vector>
#include <optional>

class Node {
private:
    // Persistent state(all Server)
    Term currentTerm = 0;
    std::optional<NodeId> votedFor = std::nullopt;   // nullopt = ещё не голосовал, а при запуске он и не может уже быть проголосовавшим
    
    std::vector<LogEntry> logs; // при инициализации считаю из файла
    
    // Volatile state(all Server)
    Index commitIndex = 0;
    Index lastApplied = 0;

    // Volatile state(Leader)
    std::unordered_map<NodeId, Index> nextIndex;
    std::unordered_map<NodeId, Index> matchIndex; //как-то должно быть проинициализировнно 0
    // ...
    Role role = Role::FOLLOWER; // думаю начинать с фолловера всегда
    std::optional<NodeId> leaderId;
    
    Storage& storage;                 // через него пишем на диск

public:
    explicit Node(Storage& storage) : storage(storage) {}
};