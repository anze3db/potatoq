"""Task results: ``AsyncResult``, ``GroupResult``, ``EagerResult``."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from . import serialization, states
from .exceptions import TimeoutError as PotatoqTimeoutError

if TYPE_CHECKING:
    from .app import Potatoq
    from .brokers.base import ResultRecord

__all__ = ["AsyncResult", "EagerResult", "GroupResult", "ResultSet", "result_from_tuple"]


def _assert_will_not_block(app: Potatoq) -> None:
    if app.current_task_request() is not None:
        raise RuntimeError(
            "Never call result.get() within a task: it can deadlock the worker pool. "
            "Use a chain or chord instead. Pass disable_sync_subtasks=False to override."
        )


class AsyncResult:
    """The result of a task that was sent to the broker."""

    def __init__(
        self,
        id: str,
        backend: Any = None,
        task_name: str | None = None,
        app: Potatoq | None = None,
        parent: Any = None,
        ignored: bool = False,
    ):
        if app is None:
            from .app import current_app

            app = current_app()
        self.id = id
        self.app = app
        self.task_name = task_name
        self.parent = parent
        self.ignored = ignored
        self._cache: ResultRecord | None = None

    task_id = property(lambda self: self.id)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.id}>"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AsyncResult):
            return other.id == self.id
        if isinstance(other, str):
            return other == self.id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return self.id

    # --- state -------------------------------------------------------------------

    def _get_record(self) -> ResultRecord | None:
        if self._cache is not None:
            return self._cache
        record = self.app.get_result(self.id)
        if record is not None and record.ready:
            self._cache = record
        return record

    @property
    def state(self) -> str:
        record = self._get_record()
        return record.state if record else states.PENDING

    status = state

    @property
    def info(self) -> Any:
        record = self._get_record()
        if record is None:
            return None
        return self._decode_value(record)

    result = info

    @property
    def traceback(self) -> str | None:
        record = self._get_record()
        return record.traceback if record else None

    @property
    def date_done(self) -> Any:
        record = self._get_record()
        if record is None or record.date_done is None:
            return None
        from datetime import UTC, datetime

        return datetime.fromtimestamp(record.date_done, tz=UTC)

    @property
    def name(self) -> str | None:
        record = self._get_record()
        return (record.task_name if record else None) or self.task_name

    @property
    def args(self) -> Any:
        record = self._get_record()
        return record.args if record else None

    @property
    def kwargs(self) -> Any:
        record = self._get_record()
        return record.kwargs if record else None

    @property
    def retries(self) -> int:
        record = self._get_record()
        return record.retries if record else 0

    @property
    def worker(self) -> str | None:
        record = self._get_record()
        return record.worker if record else None

    def _decode_value(self, record: ResultRecord) -> Any:
        if record.state in states.EXCEPTION_STATES and isinstance(record.result, dict) and "exc_type" in record.result:
            return serialization.exception_from_dict(record.result)
        return record.result

    def ready(self) -> bool:
        return self.state in states.READY_STATES

    def successful(self) -> bool:
        return self.state == states.SUCCESS

    def failed(self) -> bool:
        return self.state == states.FAILURE

    # --- waiting -----------------------------------------------------------------

    def get(
        self,
        timeout: float | None = None,
        propagate: bool = True,
        interval: float = 0.5,
        no_ack: bool = True,
        follow_parents: bool = True,
        callback: Any = None,
        on_message: Any = None,
        on_interval: Any = None,
        disable_sync_subtasks: bool = True,
        **kwargs: Any,
    ) -> Any:
        if disable_sync_subtasks:
            _assert_will_not_block(self.app)
        if self.ignored:
            from .exceptions import ResultBackendDisabled

            raise ResultBackendDisabled(
                f"Task {self.task_name or self.id} ignores its result, so there is nothing to wait for. "
                "Set ignore_result=False on the task or configure result_backend."
            )
        if self._cache is None:
            record = self.app.require_backend().wait_for_result(self.id, timeout)
            if record is None or not record.ready:
                raise PotatoqTimeoutError(f"The operation timed out waiting for task {self.id}")
            self._cache = record
        record = self._cache
        value = self._decode_value(record)
        if callback is not None:
            callback(self.id, value)
        if propagate and record.state in states.PROPAGATE_STATES:
            if isinstance(value, BaseException):
                raise value
            from .exceptions import TaskRevokedError

            raise TaskRevokedError(self.id)
        return value

    wait = get

    async def aget(self, timeout: float | None = None, propagate: bool = True, **kwargs: Any) -> Any:
        """``get`` for async code (waits in a thread, never blocks the event loop)."""
        import asyncio

        return await asyncio.to_thread(lambda: self.get(timeout=timeout, propagate=propagate, **kwargs))

    def then(self, callback: Any, on_error: Any = None) -> None:
        raise NotImplementedError("Use chains (task.s() | other.s()) instead of promises")

    # --- control -----------------------------------------------------------------

    def forget(self) -> None:
        self._cache = None
        self.app.require_backend().forget(self.id)

    def revoke(
        self,
        connection: Any = None,
        terminate: bool = False,
        signal: Any = None,
        wait: bool = False,
        timeout: float | None = None,
    ) -> None:
        self.app.control.revoke(self.id, terminate=terminate, signal=signal)

    def collect(self, intermediate: bool = False, **kwargs: Any) -> Iterator[tuple[AsyncResult, Any]]:
        yield self, self.get(**kwargs)

    @property
    def children(self) -> list[AsyncResult]:
        return []

    def build_graph(self, intermediate: bool = False, formatter: Any = None) -> Any:
        raise NotImplementedError

    def as_tuple(self) -> tuple[Any, ...]:
        parent = self.parent
        return ((self.id, parent.as_tuple() if parent else None), None)

    def __reduce__(self) -> Any:
        return (self.app.AsyncResult, (self.id,))


class EagerResult(AsyncResult):
    """Result of a task run locally with ``apply()`` / ``task_always_eager``."""

    def __init__(
        self,
        id: str,
        ret_value: Any,
        state: str,
        traceback: str | None = None,
        app: Potatoq | None = None,
        name: str | None = None,
    ):
        super().__init__(id, app=app, task_name=name)
        self._value = ret_value
        self._state = state
        self._traceback = traceback

    @property
    def state(self) -> str:
        return self._state

    status = state

    @property
    def info(self) -> Any:
        return self._value

    result = info

    @property
    def traceback(self) -> str | None:
        return self._traceback

    def get(
        self,
        timeout: float | None = None,
        propagate: bool = True,
        interval: float = 0.5,
        no_ack: bool = True,
        follow_parents: bool = True,
        callback: Any = None,
        on_message: Any = None,
        on_interval: Any = None,
        disable_sync_subtasks: bool = True,
        **kwargs: Any,
    ) -> Any:
        if propagate and self._state in states.PROPAGATE_STATES and isinstance(self._value, BaseException):
            raise self._value
        return self._value

    wait = get

    def forget(self) -> None:
        pass

    def revoke(self, *args: Any, **kwargs: Any) -> None:
        self._state = states.REVOKED

    def __repr__(self) -> str:
        return f"<EagerResult: {self.id}>"


class ResultSet:
    def __init__(self, results: list[AsyncResult], app: Potatoq | None = None, **kwargs: Any):
        self.results = list(results)
        if app is None:
            from .app import current_app

            app = current_app()
        self.app = app

    def __iter__(self) -> Iterator[AsyncResult]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    def __getitem__(self, index: int) -> AsyncResult:
        return self.results[index]

    def add(self, result: AsyncResult) -> None:
        if result not in self.results:
            self.results.append(result)

    def ready(self) -> bool:
        return all(r.ready() for r in self.results)

    def successful(self) -> bool:
        return all(r.successful() for r in self.results)

    def failed(self) -> bool:
        return any(r.failed() for r in self.results)

    def waiting(self) -> bool:
        return not self.ready()

    def completed_count(self) -> int:
        return sum(1 for r in self.results if r.successful())

    def revoke(self, terminate: bool = False, **kwargs: Any) -> None:
        self.app.control.revoke([r.id for r in self.results], terminate=terminate)

    def forget(self) -> None:
        for r in self.results:
            r.forget()

    def get(
        self,
        timeout: float | None = None,
        propagate: bool = True,
        interval: float = 0.5,
        disable_sync_subtasks: bool = True,
        **kwargs: Any,
    ) -> list[Any]:
        return self.join(timeout=timeout, propagate=propagate, disable_sync_subtasks=disable_sync_subtasks)

    def join(
        self,
        timeout: float | None = None,
        propagate: bool = True,
        interval: float = 0.5,
        disable_sync_subtasks: bool = True,
        **kwargs: Any,
    ) -> list[Any]:
        if disable_sync_subtasks:
            _assert_will_not_block(self.app)
        deadline = None if timeout is None else time.monotonic() + timeout
        values = []
        for r in self.results:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            values.append(r.get(timeout=remaining, propagate=propagate, disable_sync_subtasks=False))
        return values

    join_native = join

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {[r.id for r in self.results]}>"


class GroupResult(ResultSet):
    def __init__(
        self,
        id: str | None = None,
        results: list[AsyncResult] | None = None,
        parent: Any = None,
        app: Potatoq | None = None,
        **kwargs: Any,
    ):
        super().__init__(results or [], app=app)
        self.id = id
        self.parent = parent

    def save(self, backend: Any = None) -> GroupResult:
        from .brokers.base import ResultRecord

        record = ResultRecord(task_id=f"group:{self.id}", state=states.SUCCESS, result=[r.id for r in self.results])
        self.app.require_backend().store_result(record, expires=self.app.conf.result_expires)
        return self

    def delete(self, backend: Any = None) -> None:
        self.app.require_backend().forget(f"group:{self.id}")

    @classmethod
    def restore(cls, id: str, backend: Any = None, app: Potatoq | None = None) -> GroupResult | None:
        if app is None:
            from .app import current_app

            app = current_app()
        record = app.require_backend().get_result(f"group:{id}")
        if record is None:
            return None
        return cls(id, [app.AsyncResult(task_id) for task_id in record.result], app=app)

    def __repr__(self) -> str:
        return f"<GroupResult: {self.id} [{', '.join(r.id for r in self.results)}]>"


def result_from_tuple(r: Any, app: Potatoq | None = None) -> AsyncResult:
    (id_, parent), _ = r
    if app is None:
        from .app import current_app

        app = current_app()
    return AsyncResult(id_, app=app, parent=result_from_tuple(parent, app) if parent else None)
