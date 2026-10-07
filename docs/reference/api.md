# Python API

## Application

::: potatoq.Potatoq
    options:
      members:
        - task
        - send_task
        - config_from_object
        - autodiscover_tasks
        - add_periodic_task
        - AsyncResult
        - publish
        - on_commit
        - broker
        - backend

::: potatoq.shared_task

## Tasks

::: potatoq.Task
    options:
      members:
        - delay
        - apply_async
        - delay_on_commit
        - adelay
        - apply
        - s
        - si
        - signature
        - retry
        - replace
        - update_state
        - request
        - before_start
        - on_success
        - on_failure
        - on_retry
        - after_return

## Results

::: potatoq.result.AsyncResult
    options:
      members: [state, result, traceback, get, aget, ready, successful, failed, forget, revoke]

::: potatoq.result.GroupResult
    options:
      members: [get, join, ready, successful, failed, completed_count, save, restore]

## Workflows

::: potatoq.canvas.Signature
    options:
      members: [delay, apply_async, clone, set, link, link_error, freeze]

::: potatoq.canvas.chain

::: potatoq.canvas.group

::: potatoq.canvas.chord

## Schedules

::: potatoq.schedules.crontab
    options:
      members: [from_string, next_after]

::: potatoq.schedules.schedule

## Testing

::: potatoq.testing.drain

::: potatoq.testing.DrainedTask

## Exceptions

::: potatoq.exceptions
    options:
      show_root_heading: false
      members_order: source
