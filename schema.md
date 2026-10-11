# Схема реализации: каркас + выбор лидера + status (REST API)

## Общая схема

```
                ┌──────────── __main__.py ────────────┐
                │ argparse → Config, logging,          │
                │ создаёт всё и запускает asyncio.run  │
                └─────────────────┬────────────────────┘
                                  │
  другие узлы / curl              ▼
  ── HTTP ──► server.py ──вызывает──► node.py ──использует──► storage.py (state.json)
                                       │
                                       └──отправляет RPC──► transport.py ── HTTP ──► другие узлы
  messages.py — dataclass'ы запросов/ответов, их используют все
```

Главные правила:
- **один поток asyncio**, блокировки не нужны;
- **обработчики RPC и колбэки таймеров — обычные `def`** без `await`, поэтому никто не вклинится посередине изменения состояния;
- **`node.py` ничего не знает про HTTP:** он вызывает `transport`, а его вызывает `server`.

---

## 1. `config.py`

**`Config`** — `@dataclass(frozen=True)`:
- `node_id: int`, `host: str`, `port: int`;
- `peers: dict[int, str]` — `id → "host:port"` остальных узлов;
- `data_dir: Path`;
- `election_timeout: tuple[float, float]` — в секундах;
- `heartbeat_interval: float`, `rpc_timeout: float` — в секундах;
- свойство `address` возвращает `"host:port"`.

**`parse_args(argv=None) -> Config`:**
1. Аргументы argparse:
   - `--id` (обязательный), `--host` (по умолчанию 127.0.0.1), `--port` (обязательный);
   - `--peers` с `nargs="*"`, формат `ID=HOST:PORT`;
   - `--data-dir`, по умолчанию `data/node<ID>`;
   - `--election-timeout MIN MAX` в мс, по умолчанию 150 300;
   - `--heartbeat` в мс (50), `--rpc-timeout` в мс (100).
2. Каждую строку пира разбить по `=` и по последнему `:`. При неверном формате или собственном id — `parser.error(...)`.
3. Проверить, что `0 < min <= max` и `heartbeat < min` (§5.6).
4. Перевести мс в секунды и вернуть `Config`.

---

## 2. `messages.py`

Пять `@dataclass` с полями из Figure 2 (имена в snake_case):
- `LogEntry`: `term`, `command`;
- `RequestVoteRequest`: `term`, `candidate_id`, `last_log_index`, `last_log_term`;
- `RequestVoteResponse`: `term`, `vote_granted`;
- `AppendEntriesRequest`: `term`, `leader_id`, `prev_log_index`, `prev_log_term`, `entries: list[LogEntry]`, `leader_commit`;
- `AppendEntriesResponse`: `term`, `success`.

Преобразование в JSON: `dataclasses.asdict(msg)`. Обратно: `Cls(**data)`.

Есть одна тонкость: в `AppendEntriesRequest` записи `entries` из JSON приходят словарями. Их нужно превратить в `LogEntry`, удобнее всего в `__post_init__`.

---

## 3. `storage.py`

- `PersistentState` — dataclass с полями `current_term: int = 0` и `voted_for: int | None = None`.
- `Storage(data_dir)`:
  - в конструкторе создать папку (`mkdir(parents=True, exist_ok=True)`);
  - `load() -> PersistentState`: если файла `state.json` нет, вернуть значения по умолчанию; иначе прочитать JSON и создать `PersistentState(**data)`;
  - `save(state)`: записать JSON в `state.json.tmp`, сделать `flush()` и `os.fsync()`, затем `os.replace(tmp, state.json)`. Так запись атомарная.

---

## 4. `transport.py`

**`Transport`** — интерфейс (`typing.Protocol`) из двух методов:
- `async request_vote(peer_id, req) -> RequestVoteResponse | None`;
- `async append_entries(peer_id, req) -> AppendEntriesResponse | None`.

`None` означает «узел не ответил».

**`HttpTransport(peers, timeout)`:**
- `async start()` — создать `aiohttp.ClientSession(timeout=ClientTimeout(total=timeout))`. Создавать **внутри** запущенного цикла событий.
- `async close()` — закрыть сессию.
- Оба метода RPC вызывают общий `_call`:
  1. собрать URL `http://{peers[peer_id]}/raft/request-vote` (или `/append-entries`);
  2. `session.post(url, json=asdict(req))`;
  3. если статус не 200, вернуть `None`, иначе `ResponseCls(**await resp.json())`;
  4. при `aiohttp.ClientError`, `TimeoutError` и `ValueError` вернуть `None`.

---

## 5. `server.py`

**`HttpServer(node, host, port)`:**
- в конструкторе создать `web.Application()` и три маршрута:
  - `POST /raft/request-vote`
  - `POST /raft/append-entries`
  - `GET /status`
- `web.AppRunner(app, access_log=None)`, иначе каждый heartbeat попадёт в лог;
- `async start()`: `await runner.setup()`, затем `await web.TCPSite(runner, host, port).start()`;
- `async stop()`: `await runner.cleanup()`.

Каждый обработчик RPC делает одно и то же:
1. `data = await request.json()`, затем `Cls(**data)`. При ошибке — `web.HTTPBadRequest`.
2. Вызвать **синхронный** метод узла: `node.handle_request_vote(req)` или `node.handle_append_entries(req)`.
3. Вернуть `web.json_response(asdict(resp))`.

`/status` возвращает `web.json_response(node.status())`.

---

## 6. `node.py` — основная часть

### Поля

```
Role — StrEnum: FOLLOWER / CANDIDATE / LEADER

Node(config, storage, transport):
    # постоянное (Figure 2) — копия того, что на диске
    current_term, voted_for
    # изменчивое
    role, leader_id
    votes: set[int]                    — кто проголосовал за нас в этих выборах
    last_contact: dict[int, float]     — для /status (по желанию)
    # техническое
    loop                               — asyncio.get_running_loop()
    election_timer, heartbeat_timer    — asyncio.TimerHandle | None
    tasks: set[Task]                   — исходящие RPC, которые ещё в пути
```

### Жизненный цикл

- `start()`: `storage.load()` → `current_term`, `voted_for`; `role = FOLLOWER`; записать в лог; `reset_election_timer()`.
- `stop()`: отменить оба таймера и все задачи из `tasks`.

### Входящие RPC (Figure 2, «Receiver implementation»)

`handle_request_vote(req) -> RequestVoteResponse`:
1. Если `req.term > current_term`, вызвать `become_follower(req.term)`.
2. Если `req.term < current_term`, вернуть `(current_term, False)`.
3. Если `voted_for` равен `None` или `req.candidate_id`, **и** `log_is_up_to_date(...)`:
   - `voted_for = req.candidate_id`;
   - `persist()` — **до** ответа;
   - `reset_election_timer()`;
   - вернуть `(current_term, True)`.
4. Иначе вернуть `(current_term, False)`.

`handle_append_entries(req) -> AppendEntriesResponse`:
1. Если `req.term < current_term`, вернуть `(current_term, False)`.
2. `become_follower(req.term)` — кандидат с тем же term тоже уступает.
3. `leader_id = req.leader_id`.
4. `reset_election_timer()`.
5. Вернуть `(current_term, True)`. На этапе 2 сюда добавится работа с журналом.

### Смена ролей (Figure 4)

Роль меняется **только** через эти три функции, и каждая пишет в лог.

`become_follower(term)`:
1. Если `term > current_term`: обновить `current_term`, `voted_for = None`, `leader_id = None`, `persist()`.
2. Если узел был лидером: отменить heartbeat и `reset_election_timer()`, потому что у лидера таймер выборов выключен.
3. `role = FOLLOWER`, очистить `votes`.

`become_candidate()`:
1. `role = CANDIDATE`; `current_term += 1`; `voted_for = self.id`; `leader_id = None`; `votes = {self.id}`.
2. `persist()`.
3. `reset_election_timer()`: если за таймаут не победим, начнутся новые выборы.
4. Если `len(votes) >= majority()` (кластер из одного узла), вызвать `become_leader()`. Иначе `broadcast_request_vote()`.

`become_leader()`:
1. `role = LEADER`; `leader_id = self.id`.
2. Отменить таймер выборов.
3. Сразу `send_heartbeats()`.

### Таймеры

- `reset_election_timer()`: отменить старый таймер, затем `loop.call_later(random.uniform(min, max), on_election_timeout)`.
- `on_election_timeout()`: если узел не лидер, вызвать `become_candidate()`.
- `send_heartbeats()`: `broadcast_append_entries()`, затем `loop.call_later(heartbeat_interval, on_heartbeat_timer)`.
- `on_heartbeat_timer()`: если узел всё ещё лидер, снова вызвать `send_heartbeats()`.

### Исходящие RPC

`broadcast_request_vote()`: один раз собрать `RequestVoteRequest(current_term, id, 0, 0)` и для каждого пира вызвать `spawn(request_vote_from(peer, req))`.

`async request_vote_from(peer, req)`:
1. `resp = await transport.request_vote(peer, req)`.
2. Если `resp is None`, ничего не делать.
3. Если `resp.term > current_term`, вызвать `become_follower(resp.term)` и выйти.
4. Если `role != CANDIDATE` или `req.term != current_term`, это **ответ на старые выборы** — игнорировать.
5. Если `resp.vote_granted`: `votes.add(peer)`; при `len(votes) >= majority()` вызвать `become_leader()`.

`broadcast_append_entries()`: собрать `AppendEntriesRequest(current_term, id, 0, 0, [], 0)` и запустить `spawn(append_entries_to(peer, req))` для каждого пира.

`async append_entries_to(peer, req)`: получить ответ; если `None`, выйти; если `resp.term > current_term`, вызвать `become_follower(resp.term)`.

`spawn(coro)`:
1. `task = loop.create_task(coro)`.
2. `tasks.add(task)` — иначе задачу может собрать сборщик мусора.
3. `task.add_done_callback(...)`: в нём убрать задачу из `tasks` и залогировать исключение, если оно было.

### Вспомогательное

- `persist()`: `storage.save(PersistentState(current_term, voted_for))`.
- `majority()`: `(len(peers) + 1) // 2 + 1`.
- `last_log_index()` и `last_log_term()`: пока возвращают 0.
- `log_is_up_to_date(term, index)` (§5.4.1): если term'ы разные, у кого term больше, тот новее; если одинаковые — у кого index больше.
- `status() -> dict`: `id`, `address`, `role`, `term`, `voted_for`, `leader_id`, `leader_address`, `peers` (можно с `last_contact_ms_ago`).

---

## 7. `__main__.py`

`async run(config) -> int`:
1. Создать `HttpTransport`, затем `Node(config, Storage(config.data_dir), transport)`, затем `HttpServer`. Node создаётся **внутри** `run`, потому что ему нужен запущенный цикл событий.
2. `await transport.start()`.
3. Дальше всё внутри `try:`
   - `node.start()`;
   - `await server.start()`. При `OSError` (порт занят) залогировать ошибку и вернуть 1;
   - `await asyncio.Event().wait()` — работать, пока не остановят.
4. В `finally:` `node.stop()`, `await server.stop()`, `await transport.close()`.

`main()`:
1. `config = parse_args()`.
2. `logging.basicConfig(...)` с форматом, в котором есть `[node {id}]`.
3. `sys.exit(asyncio.run(run(config)))` внутри `contextlib.suppress(KeyboardInterrupt)`.

Порядок запуска важен: transport, потом node, потом server. Таймер выборов может сработать сразу, и ему нужен транспорт. Входящий RPC может прийти сразу, и ему нужен уже загруженный узел.

---

## Ловушки

1. **Ответ на старые выборы.** Ответ пришёл, когда узел уже в другом term или другой роли. Отсюда проверка в шаге 4 `request_vote_from`.
2. **Двойной подсчёт голоса.** Голоса храни в `set`, а не в счётчике.
3. **`persist()` после ответа** вместо «до». Голос должен оказаться на диске раньше, чем уйдёт ответ.
4. **Таймер выборов у лидера** не отменён, и лидер сам себя свергает новыми выборами.
5. **Сброс таймера выборов при каждом RequestVote.** Сбрасывать нужно только при отданном голосе и при AppendEntries от лидера.
6. **`await` внутри `handle_*` или `become_*`.** В этот момент может вклиниться другой обработчик.

---

## Порядок реализации и проверки

| Шаг | Что сделать | Как проверить |
|---|---|---|
| 1 | `config.py` + `storage.py` | `python -m raft --id 1 --port 8001` печатает конфиг; `state.json` переживает перезапуск |
| 2 | `messages.py` | в REPL: `asdict(...)`, затем `Cls(**...)` возвращает равный объект |
| 3 | `server.py` с заглушкой `/status` | `Invoke-RestMethod http://127.0.0.1:8001/status` |
| 4 | `node.py`: `start`, `handle_*`, `status`, `become_follower` | вызвать RPC руками (term 100 → follower, term 5 → `success:false`) |
| 5 | таймеры + `become_candidate/leader` | один узел без пиров сам становится лидером через ~0.2 с |
| 6 | `transport.py` + рассылки | 3 узла: ровно один лидер, остальные знают его `leader_id` |
| 7 | отказ | убить лидера → за <1 с новый лидер с бо́льшим term; вернуть узел → он follower |
