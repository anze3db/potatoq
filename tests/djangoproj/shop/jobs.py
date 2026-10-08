"""django.tasks tasks (deliberately not in tasks.py: the worker must find them anyway)."""

import asyncio

from django.tasks import task

from potatoq.contrib.django.tasks import task as potatoq_task


@task(queue_name="shop")
def total(order_id, items):
    return {"order": order_id, "total": sum(items)}


@task(queue_name="shop", priority=50, takes_context=True)
def with_context(context, x):
    return {"attempt": context.attempt, "id": context.task_result.id, "x": x}


@task(queue_name="shop")
async def async_double(x):
    await asyncio.sleep(0)
    return x * 2


@potatoq_task(queue_name="shop", max_retries=2, autoretry_for=(ConnectionError,), default_retry_delay=0)
def flaky(attempts_file):
    import pathlib

    path = pathlib.Path(attempts_file)
    n = int(path.read_text() or 0) + 1 if path.exists() else 1
    path.write_text(str(n))
    if n < 3:
        raise ConnectionError(f"attempt {n}")
    return n


@task(queue_name="shop")
def boom():
    raise ValueError("boom")


import django  # noqa: E402

if django.VERSION >= (6, 1):

    @task(queue_name="shop", time_limit=60, ignore_result=False)  # Django 6.1+ forwards extra options
    def with_options():
        return "ok"
