"""Redis / Valkey broker.

Design (BullMQ-style; research in docs/backends.md):

* Every state change is one atomic Lua script: enqueue, claim, complete (+ result +
  follow-up tasks), retry, requeue, dead-letter. No "popped but not yet pushed" gaps.
* Ready tasks live in a sorted set scored by (priority, sequence): real priorities
  with FIFO order inside a priority, no emulation with N lists.
* Delayed tasks (ETA, countdown, retry backoff) live in a per-queue sorted set and are
  promoted atomically when due. Workers never hold future tasks in memory, so the
  infamous ``visibility_timeout`` duplicate-execution problem cannot happen.
* A claimed task gets a *lease* (in an "active" sorted set scored by deadline) and a
  fencing token. The worker supervisor extends the leases of running tasks every few
  seconds, so long tasks are never redelivered while they run; tasks of a crashed
  node are recovered once their lease expires (``worker_dead_after``, 60s).
* Idle workers block on a per-queue "marker" key with ``BZPOPMIN`` instead of
  polling; enqueue sets the marker, and each claim re-arms it while work remains.
* Configure Redis with ``maxmemory-policy noeviction`` (checked on startup) and AOF.
  Redis Cluster is not supported yet (keys of different queues are combined in scripts).
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any

from .. import serialization, states
from ..message import Message
from .base import Broker, Consumer, Delivery, ResultRecord

try:
    import redis
except ImportError as exc:  # pragma: no cover
    raise ImportError("Redis support requires redis-py: pip install 'potatoq[redis]'") from exc

logger = logging.getLogger("potatoq.redis")

LUA = r"""
local cmd = ARGV[1]
local P = ARGV[2]
local PRIO = 1099511627776  -- 2^40: priority dominates the sequence number

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function qkey(q) return P .. ':q:' .. q end
local function jkey(id) return P .. ':job:' .. id end

local function push_ready(q, id, prio, front)
  local score
  if front then score = -prio * PRIO else score = -prio * PRIO + redis.call('INCR', P .. ':seq') end
  redis.call('ZADD', qkey(q) .. ':ready', score, id)
  redis.call('ZADD', qkey(q) .. ':marker', 0, '0')
end

-- enqueue n messages starting at ARGV[i]: id, queue, priority, delay_ms, payload
local function enqueue(i, n, now)
  local added = 0
  for k = 0, n - 1 do
    local b = i + k * 5
    local id, q, prio, delay, payload = ARGV[b], ARGV[b + 1], tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3]), ARGV[b + 4]
    local jk = jkey(id)
    if redis.call('EXISTS', jk) == 0 then
      redis.call('SADD', P .. ':queues', q)
      if delay > 0 then
        redis.call('HSET', jk, 'q', q, 'p', payload, 'pr', prio, 'd', 0, 'st', 'delayed')
        redis.call('ZADD', qkey(q) .. ':delayed', now + delay, id)
      else
        redis.call('HSET', jk, 'q', q, 'p', payload, 'pr', prio, 'd', 0, 'st', 'ready')
        push_ready(q, id, prio, false)
      end
      added = added + 1
    end
  end
  return added
end

local function store_result(id, result, ttl)
  if result ~= '' then
    local rk = P .. ':result:' .. id
    if tonumber(ttl) > 0 then
      redis.call('SET', rk, result, 'PX', ttl)
    else
      redis.call('SET', rk, result)
    end
    if redis.call('EXISTS', P .. ':waiting:' .. id) == 1 then
      redis.call('RPUSH', P .. ':notify:' .. id, 1)
      redis.call('PEXPIRE', P .. ':notify:' .. id, 60000)
    end
  end
end

-- true if the caller still owns the job (token matches)
local function release(id, q, tok)
  local jk = jkey(id)
  if redis.call('HGET', jk, 'tok') ~= tok then return false end
  redis.call('ZREM', qkey(q) .. ':active', id)
  return true
end

if cmd == 'enqueue' then
  return enqueue(4, tonumber(ARGV[3]), now_ms())

elseif cmd == 'periodic' then
  -- name, fire_at, ttl, then one message
  local key = P .. ':periodic:' .. ARGV[3] .. ':' .. ARGV[4]
  if not redis.call('SET', key, 1, 'NX', 'PX', ARGV[5]) then return 0 end
  enqueue(6, 1, now_ms())
  return 1

elseif cmd == 'claim' then
  -- queue, worker, pid, lease_ms, token
  local q, worker, pid, lease, tok = ARGV[3], ARGV[4], ARGV[5], tonumber(ARGV[6]), ARGV[7]
  local now = now_ms()
  local qk = qkey(q)
  local due = redis.call('ZRANGE', qk .. ':delayed', '-inf', now, 'BYSCORE', 'LIMIT', 0, 1000)
  for _, id in ipairs(due) do
    redis.call('ZREM', qk .. ':delayed', id)
    local pr = redis.call('HGET', jkey(id), 'pr')
    if pr then
      redis.call('HSET', jkey(id), 'st', 'ready')
      push_ready(q, id, tonumber(pr), false)
    end
  end
  while true do
    local popped = redis.call('ZPOPMIN', qk .. ':ready')
    if #popped == 0 then break end
    local id = popped[1]
    local jk = jkey(id)
    if redis.call('EXISTS', jk) == 1 then  -- revoked jobs have no hash: skip them
      local d = redis.call('HINCRBY', jk, 'd', 1)
      redis.call('HSET', jk, 'st', 'active', 'tok', tok, 'w', worker, 'pid', pid)
      redis.call('ZADD', qk .. ':active', now + lease, id)
      if redis.call('ZCARD', qk .. ':ready') > 0 then redis.call('ZADD', qk .. ':marker', 0, '0') end
      return {id, redis.call('HGET', jk, 'p'), d}
    end
  end
  return {false, false, false}

elseif cmd == 'extend' then
  -- lease_ms, then (queue, id, token) triples
  local now = now_ms()
  local lease = tonumber(ARGV[3])
  local n = 0
  for i = 4, #ARGV, 3 do
    local q, id, tok = ARGV[i], ARGV[i + 1], ARGV[i + 2]
    if redis.call('HGET', jkey(id), 'tok') == tok then
      redis.call('ZADD', qkey(q) .. ':active', 'XX', now + lease, id)
      n = n + 1
    end
  end
  return n

elseif cmd == 'complete' then
  -- id, queue, token, result, ttl, n_followups, followups...
  local id, q, tok = ARGV[3], ARGV[4], ARGV[5]
  -- Lost the claim (lease expired and the task was recovered): write nothing.
  if not release(id, q, tok) then return 0 end
  redis.call('DEL', jkey(id))
  store_result(id, ARGV[6], ARGV[7])
  enqueue(9, tonumber(ARGV[8]), now_ms())
  return 1

elseif cmd == 'retry' then
  -- id, queue, token, new_queue, priority, delay_ms, payload, result, ttl
  local id, q, tok = ARGV[3], ARGV[4], ARGV[5]
  local nq, prio, delay, payload = ARGV[6], tonumber(ARGV[7]), tonumber(ARGV[8]), ARGV[9]
  if not release(id, q, tok) then return 0 end
  local jk = jkey(id)
  redis.call('HDEL', jk, 'tok', 'w', 'pid')
  redis.call('HSET', jk, 'q', nq, 'p', payload, 'pr', prio, 'd', 0)
  redis.call('SADD', P .. ':queues', nq)
  if delay > 0 then
    redis.call('HSET', jk, 'st', 'delayed')
    redis.call('ZADD', qkey(nq) .. ':delayed', now_ms() + delay, id)
  else
    redis.call('HSET', jk, 'st', 'ready')
    push_ready(nq, id, prio, false)
  end
  store_result(id, ARGV[10], ARGV[11])
  return 1

elseif cmd == 'requeue' then
  -- id, queue, token, count(0/1)
  local id, q, tok = ARGV[3], ARGV[4], ARGV[5]
  if not release(id, q, tok) then return 0 end
  local jk = jkey(id)
  if ARGV[6] == '0' then redis.call('HINCRBY', jk, 'd', -1) end
  redis.call('HDEL', jk, 'tok', 'w', 'pid')
  redis.call('HSET', jk, 'st', 'ready')
  push_ready(q, id, tonumber(redis.call('HGET', jk, 'pr') or 0), true)
  return 1

elseif cmd == 'dead' then
  -- id, queue, token, reason, result, ttl, n_followups, followups...
  local id, q, tok = ARGV[3], ARGV[4], ARGV[5]
  local now = now_ms()
  if not release(id, q, tok) then return 0 end
  local jk = jkey(id)
  redis.call('HDEL', jk, 'tok', 'w', 'pid')
  redis.call('HSET', jk, 'st', 'dead', 'r', ARGV[6], 'died', now)
  redis.call('ZADD', P .. ':dead', now, id)
  store_result(id, ARGV[7], ARGV[8])
  enqueue(10, tonumber(ARGV[9]), now)
  return 1

elseif cmd == 'reap' then
  -- queue, max_deliveries: recover expired leases. Returns dead-lettered ids.
  local q, maxd = ARGV[3], tonumber(ARGV[4])
  local now = now_ms()
  local qk = qkey(q)
  local dead = {}
  local expired = redis.call('ZRANGE', qk .. ':active', '-inf', now, 'BYSCORE', 'LIMIT', 0, 1000)
  for _, id in ipairs(expired) do
    redis.call('ZREM', qk .. ':active', id)
    local jk = jkey(id)
    local v = redis.call('HMGET', jk, 'd', 'pr', 'p')
    if v[1] then
      redis.call('HDEL', jk, 'tok', 'w', 'pid')
      if tonumber(v[1]) >= maxd then
        redis.call('HSET', jk, 'st', 'dead', 'r', 'worker lost too many times', 'died', now)
        redis.call('ZADD', P .. ':dead', now, id)
        table.insert(dead, id)
        table.insert(dead, v[1])
        table.insert(dead, v[3])
      else
        redis.call('HSET', jk, 'st', 'ready')
        push_ready(q, id, tonumber(v[2]), true)
      end
    end
  end
  return dead

elseif cmd == 'revoke' then
  -- ids...: delete waiting jobs; running ones are left alone
  local n = 0
  for i = 3, #ARGV do
    local jk = jkey(ARGV[i])
    local v = redis.call('HMGET', jk, 'q', 'st')
    if v[1] and v[2] ~= 'active' then
      redis.call('ZREM', qkey(v[1]) .. ':ready', ARGV[i])
      redis.call('ZREM', qkey(v[1]) .. ':delayed', ARGV[i])
      redis.call('DEL', jk)
      n = n + 1
    end
  end
  return n

elseif cmd == 'chord' then
  -- group_id, index, size, result
  -- Parts are kept (7 days): a part redelivered after completion (its worker died
  -- before acking) gets the results again, so the callback is never lost.
  local key = P .. ':chord:' .. ARGV[3]
  redis.call('HSETNX', key, ARGV[4], ARGV[6])
  redis.call('PEXPIRE', key, 604800000)
  local n = redis.call('HLEN', key)
  if n < tonumber(ARGV[5]) then return false end
  return redis.call('HGETALL', key)

elseif cmd == 'requeue_dead' then
  local id = ARGV[3]
  local jk = jkey(id)
  local v = redis.call('HMGET', jk, 'q', 'pr', 'st')
  if v[3] ~= 'dead' then return 0 end
  redis.call('ZREM', P .. ':dead', id)
  redis.call('HDEL', jk, 'r', 'died')
  redis.call('HSET', jk, 'd', 0, 'st', 'ready')
  redis.call('DEL', P .. ':result:' .. id)
  push_ready(v[1], id, tonumber(v[2]), false)
  return 1

elseif cmd == 'trim_dead' then
  -- max_entries, min_died_ms
  local key = P .. ':dead'
  local old = redis.call('ZRANGE', key, '-inf', ARGV[4], 'BYSCORE', 'LIMIT', 0, 1000)
  local over = redis.call('ZCARD', key) - tonumber(ARGV[3])
  if over > 0 then
    for _, id in ipairs(redis.call('ZRANGE', key, 0, over - 1)) do table.insert(old, id) end
  end
  for _, id in ipairs(old) do
    redis.call('ZREM', key, id)
    redis.call('DEL', jkey(id))
  end
  return #old
end
return redis.error_reply('unknown command ' .. tostring(cmd))
"""


def _client_from_url(url: str, timeout: float, **options: Any) -> redis.Redis:
    if url.startswith("valkey://"):
        url = "redis://" + url[len("valkey://") :]
    elif url.startswith("valkeys://"):
        url = "rediss://" + url[len("valkeys://") :]
    elif url.startswith("redis+socket://"):
        url = "unix://" + url[len("redis+socket://") :]
    kwargs: dict[str, Any] = {
        "socket_connect_timeout": timeout,
        # Blocking commands wait at most ~1-2s, so this only catches dead sockets.
        "socket_timeout": max(10.0, timeout),
        "socket_keepalive": True,
        "health_check_interval": 30,
        "retry_on_timeout": True,
    }
    for key in ("socket_timeout", "socket_connect_timeout", "max_connections", "ssl_cert_reqs", "username", "password"):
        if key in options:
            kwargs[key] = options[key]
    return redis.Redis.from_url(url, **kwargs)


class RedisBroker(Broker):
    schemes = ("redis", "rediss", "valkey", "unix")
    supports_results = True
    transactional = False

    def __init__(self, url: str, app: Any, **options: Any):
        super().__init__(url, app, **options)
        self.prefix = str(options.get("global_keyprefix") or options.get("prefix") or "potatoq").rstrip(":")
        self._timeout = float(app.conf.broker_connection_timeout)
        self._client: redis.Redis | None = None
        self._script: Any = None
        self._pid = os.getpid()

    @property
    def client(self) -> redis.Redis:
        if self._pid != os.getpid():
            self.after_fork()
        if self._client is None:
            self._client = _client_from_url(self.url, self._timeout, **self.options)
            self._script = self._client.register_script(LUA)
        return self._client

    def run(self, cmd: str, *args: Any, client: Any = None) -> Any:
        _ = self.client
        return self._script(args=[cmd, self.prefix, *args], client=client or self._client)

    def after_fork(self) -> None:
        # redis-py pools detect the pid change themselves; just start over.
        self._client = None
        self._script = None
        self._pid = os.getpid()

    def close(self) -> None:
        if self._client is not None and self._pid == os.getpid():
            self._client.close()
        self._client = None

    def setup(self) -> None:
        try:
            policy = self.client.config_get("maxmemory-policy").get("maxmemory-policy")
        except redis.ResponseError:
            return  # CONFIG may be disabled on managed services
        if policy and policy != "noeviction":
            logger.warning(
                "Redis maxmemory-policy is %r: Redis may silently evict queued tasks. Use 'noeviction' "
                "(and a separate Redis for caching).",
                policy,
            )

    # --- producing -----------------------------------------------------------------

    @staticmethod
    def _message_args(messages: list[Message]) -> list[Any]:
        now = time.time()
        args: list[Any] = []
        for m in messages:
            delay_ms = int((m.eta - now) * 1000) if m.eta is not None and m.eta > now else 0
            args += [m.id, m.queue, int(m.priority), delay_ms, m.encode()]
        return args

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        if not messages:
            return
        for start in range(0, len(messages), 500):
            batch = messages[start : start + 500]
            self.run("enqueue", len(batch), *self._message_args(batch))

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        return bool(self.run("periodic", name, repr(fire_at), 7 * 86400 * 1000, *self._message_args([message])))

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> RedisConsumer:
        return RedisConsumer(self, queues, worker_id, pid)

    # --- results -------------------------------------------------------------------

    def _ttl_ms(self) -> int:
        expires = self.app.conf.result_expires
        return int(float(expires) * 1000) if expires else 0

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        key = f"{self.prefix}:result:{record.task_id}"
        data = serialization.dumps(record.to_dict())
        pipe = self.client.pipeline(transaction=False)
        if expires:
            pipe.set(key, data, px=int(float(expires) * 1000))
        else:
            pipe.set(key, data)
        if record.ready:
            pipe.exists(f"{self.prefix}:waiting:{record.task_id}")
        results = pipe.execute()
        if record.ready and results[-1]:
            notify = f"{self.prefix}:notify:{record.task_id}"
            self.client.pipeline(transaction=False).rpush(notify, 1).pexpire(notify, 60000).execute()

    def get_result(self, task_id: str) -> ResultRecord | None:
        data = self.client.get(f"{self.prefix}:result:{task_id}")
        if data is not None:
            return ResultRecord.from_dict(serialization.loads(data))
        if self.client.hget(f"{self.prefix}:job:{task_id}", "st") == b"active":
            return ResultRecord(task_id=task_id, state=states.STARTED)
        return None

    def wait_for_result(self, task_id: str, timeout: float | None) -> ResultRecord | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        client = self.client
        waiting = f"{self.prefix}:waiting:{task_id}"
        notify = f"{self.prefix}:notify:{task_id}"
        while True:
            client.set(waiting, 1, px=int(((timeout or 3600) + 30) * 1000))
            record = self.get_result(task_id)
            if record is not None and record.ready:
                client.delete(waiting)
                return record
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return record
            client.blpop([notify], timeout=min(2.0, remaining) if remaining is not None else 2.0)

    def forget(self, task_id: str) -> None:
        self.client.delete(f"{self.prefix}:result:{task_id}")

    # --- coordination ----------------------------------------------------------------

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        self.client.hset(f"{self.prefix}:workers", worker_id, json.dumps({**info, "heartbeat": time.time()}))

    def unregister(self, worker_id: str) -> None:
        self.client.hdel(f"{self.prefix}:workers", worker_id)

    def workers(self) -> list[dict[str, Any]]:
        raw = self.client.hgetall(f"{self.prefix}:workers")
        return sorted(({"id": k.decode(), **json.loads(v)} for k, v in raw.items()), key=lambda w: w["id"])

    def lease_ms(self) -> int:
        return int(float(self.app.conf.worker_dead_after) * 1000)

    def extend(self, deliveries: list[Delivery]) -> None:
        args: list[Any] = []
        for d in deliveries:
            args += [d.message.queue, d.message.id, d.handle]
        if args:
            self.run("extend", self.lease_ms(), *args)

    def _queues(self) -> list[str]:
        return [q.decode() for q in self.client.smembers(f"{self.prefix}:queues")]

    def recover(self, worker_dead_after: float) -> list[Delivery]:  # type: ignore[override]
        dead: list[Delivery] = []
        limit = int(self.app.conf.task_max_deliveries)
        for queue in self._queues():
            flat = self.run("reap", queue, limit)
            for i in range(0, len(flat), 3):
                _, deliveries, payload = flat[i : i + 3]
                dead.append(Delivery(Message.decode(payload), delivery_count=int(deliveries), handle=None))
        # Forget workers that stopped heartbeating long ago.
        cutoff = time.time() - worker_dead_after - 3600
        stale = [w["id"] for w in self.workers() if float(w.get("heartbeat", 0)) < cutoff]
        if stale:
            self.client.hdel(f"{self.prefix}:workers", *stale)
        return dead

    def maintenance(self) -> None:
        conf = self.app.conf
        min_died = int((time.time() - float(conf.dead_letter_ttl)) * 1000)
        self.run("trim_dead", int(conf.dead_letter_max), min_died)

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        flat = self.run("chord", group_id, index, size, serialization.dumps(result))
        if not flat:
            return None
        parts = {int(flat[i]): flat[i + 1] for i in range(0, len(flat), 2)}
        return [serialization.loads(parts[i]) for i in sorted(parts)]

    def revoke(self, task_ids: list[str], expires: float) -> None:
        self.run("revoke", *task_ids)
        for task_id in task_ids:
            self.store_result(ResultRecord(task_id=task_id, state=states.REVOKED, date_done=time.time()), expires)

    # --- inspection --------------------------------------------------------------

    def queue_sizes(self) -> dict[str, int]:
        pipe = self.client.pipeline(transaction=False)
        queues = self._queues()
        for q in queues:
            pipe.zcard(f"{self.prefix}:q:{q}:ready")
            pipe.zcard(f"{self.prefix}:q:{q}:delayed")
        counts = pipe.execute()
        return {q: counts[2 * i] + counts[2 * i + 1] for i, q in enumerate(queues) if counts[2 * i] + counts[2 * i + 1]}

    def purge(self, queue: str) -> int:
        client = self.client
        n = 0
        for kind in ("ready", "delayed"):
            key = f"{self.prefix}:q:{queue}:{kind}"
            ids = client.zrange(key, 0, -1)
            if ids:
                pipe = client.pipeline()
                pipe.delete(key)
                pipe.delete(*[f"{self.prefix}:job:{i.decode()}" for i in ids])
                pipe.execute()
                n += len(ids)
        return n

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        ids = self.client.zrevrange(f"{self.prefix}:dead", 0, limit - 1)
        pipe = self.client.pipeline(transaction=False)
        for i in ids:
            pipe.hmget(f"{self.prefix}:job:{i.decode()}", "q", "p", "r", "died")
        out = []
        for job_id, (queue, payload, reason, died) in zip(ids, pipe.execute(), strict=True):
            if payload is None:
                continue
            message = serialization.loads(payload)
            out.append(
                {
                    "id": job_id.decode(),
                    "queue": (queue or b"").decode(),
                    "task": message.get("task"),
                    "reason": (reason or b"").decode(),
                    "died_at": int(died or 0) / 1000,
                    "message": message,
                }
            )
        return out

    def requeue_dead(self, task_id: str) -> bool:
        return bool(self.run("requeue_dead", task_id))


class RedisConsumer(Consumer):
    broker: RedisBroker

    def __init__(self, broker: RedisBroker, queues: list[str], worker_id: str, pid: int | None = None):
        super().__init__(broker, queues, worker_id, pid)
        self._rotation = 0
        self._interrupted = False
        self._markers = [f"{broker.prefix}:q:{q}:marker" for q in queues]

    def _claim(self) -> Delivery | None:
        b = self.broker
        n = len(self.queues)
        order = [self.queues[(self._rotation + i) % n] for i in range(n)]
        self._rotation = (self._rotation + 1) % max(n, 1)
        lease = b.lease_ms()
        for queue in order:
            token = uuid.uuid4().hex
            job_id, payload, deliveries = b.run("claim", queue, self.worker_id, self.pid, lease, token)
            if job_id:
                return Delivery(Message.decode(payload), delivery_count=int(deliveries), handle=token)
        return None

    def fetch(self, timeout: float) -> Delivery | None:
        deadline = time.monotonic() + timeout
        self._interrupted = False
        while True:
            try:
                delivery = self._claim()
                if delivery is not None:
                    return delivery
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._interrupted:
                    return None
                self.broker.client.bzpopmin(self._markers, timeout=max(0.01, min(remaining, 1.0)))
            except (redis.ConnectionError, redis.TimeoutError) as exc:
                logger.warning("Redis connection problem while fetching: %s", exc)
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                return None

    def interrupt(self) -> None:
        self._interrupted = True

    def _followup_args(self, followups: list[Message] | None) -> list[Any]:
        followups = followups or []
        return [len(followups), *self.broker._message_args(followups)]

    def _result_args(self, record: ResultRecord | None) -> list[Any]:
        if record is None:
            return ["", 0]
        return [serialization.dumps(record.to_dict()), self.broker._ttl_ms()]

    def _run_retrying(self, *args: Any) -> Any:
        for attempt in range(3):
            try:
                return self.broker.run(*args)
            except (redis.ConnectionError, redis.TimeoutError):
                if attempt == 2:
                    raise
                time.sleep(0.2 * (attempt + 1))

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        m = delivery.message
        self._run_retrying(
            "complete", m.id, m.queue, delivery.handle, *self._result_args(record), *self._followup_args(followups)
        )

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        m = delivery.message
        now = time.time()
        delay_ms = int((message.eta - now) * 1000) if message.eta and message.eta > now else 0
        self._run_retrying(
            "retry", m.id, m.queue, delivery.handle, message.queue, int(message.priority), delay_ms, message.encode(),
            *self._result_args(record),
        )  # fmt: skip

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        m = delivery.message
        self._run_retrying("requeue", m.id, m.queue, delivery.handle, 1 if count else 0)

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        m = delivery.message
        self._run_retrying(
            "dead",
            m.id,
            m.queue,
            delivery.handle,
            reason[-4000:],
            *self._result_args(record),
            *self._followup_args(followups),
        )
