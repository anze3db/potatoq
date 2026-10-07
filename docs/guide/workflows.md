# Workflows: chains, groups, chords

Workflows are built from **signatures**: a task, its arguments and its options, packed
into something you can send later or pass to another task.

```python
add.s(2, 2)                 # partial: the previous result is prepended to the args
add.si(2, 2)                # immutable: ignores the previous result
add.s(2).set(queue="fast", countdown=10)
```

## chain

Each task's result becomes the first argument of the next.

```python
from potatoq import chain

result = chain(fetch.s(url), parse.s(), store.s()).apply_async()
result = (fetch.s(url) | parse.s() | store.s()).delay()     # same thing
result.get()   # the last task's result
```

## group

Run tasks in parallel.

```python
from potatoq import group

result = group(resize.s(img, w) for w in (320, 640, 1280)).apply_async()
result.get()            # [r1, r2, r3], in order
result.completed_count()
```

## chord

A group followed by a callback that receives the list of results.

```python
from potatoq import chord

result = chord((count_words.s(p) for p in pages), total.s())()
result.get()   # total([...])

# a group piped into a task is a chord too
(group(count_words.s(p) for p in pages) | total.s() | report.s()).delay()
```

Chords are counted by the broker: each finished header task increments an atomic
per-group counter, and whoever finishes last enqueues the callback. There is no polling
"chord unlock" task. If a header task fails, the callback fails with `ChordError` and
its `link_error` callbacks run.

## Callbacks

```python
add.apply_async((2, 2), link=log_result.s(), link_error=alert.s())
```

`link` receives the result, and `link_error` receives the failed task's id. Callbacks
are enqueued atomically with the task's acknowledgement: in the same transaction on
Postgres and SQLite, in the same Lua script on Redis.

## Replacing a task

```python
@app.task(bind=True)
def process(self, item):
    if is_big(item):
        self.replace(process_in_chunks.s(item))   # inherits the id, callbacks and chord
    ...
```
