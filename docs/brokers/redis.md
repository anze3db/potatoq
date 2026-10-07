# Redis and Valkey

```python
app = Potatoq("proj", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0")
```

Requires `potatoq[redis]`. Works with Redis 6.2+ and Valkey. `rediss://` (TLS),
`valkey://` and `unix://` URLs are supported.

## Why it's good

- **Atomic Lua scripts** for every state change. The ack, the result and the follow-up
  tasks happen in one step.
- **Leases instead of a visibility timeout**: running tasks are kept alive by the
  worker, so long tasks are never re-run, and a crashed worker's tasks come back within
  60 seconds.
- **Real priorities** (sorted set) with FIFO order within each priority.
- **Blocking wake-ups** with `BZPOPMIN` on a marker key, so idle workers don't poll.

## Options

```python
app.conf.broker_transport_options = {
    "global_keyprefix": "potatoq",   # all keys live under this prefix
    "socket_timeout": 10,
}
```

## Operations

!!! warning "Configure Redis for a queue, not a cache"
    - `maxmemory-policy noeviction`: anything else lets Redis silently delete queued
      tasks. potatoq warns at startup if this isn't set.
    - `appendonly yes` with `appendfsync everysec`: at most about a second of enqueued
      tasks can be lost if Redis crashes.
    - Don't share the instance with a cache that uses LRU eviction.

Results are opt-in on Redis, because they use memory: set `result_backend` to `"broker"`
or a URL. They expire after `result_expires` (1 day).

Redis Cluster is not supported yet.

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#redis-valkey)
