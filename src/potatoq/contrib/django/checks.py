"""Django system checks.

Tasks defined outside ``tasks.py`` (in ``views.py``, say) are registered in the web
process, which imports the views, but a worker only imports ``tasks.py`` modules and
``imports``: it dead-letters those tasks as unregistered, at runtime. The deploy check
boots Django in a fresh process the way a worker does and reports every task the web
process has that a worker couldn't run. Another check warns when the settings still
point Celery beat at django-celery-beat's database schedules, which potatoq doesn't read.
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from typing import Any

from django.conf import settings
from django.core import checks

from ...app import current_app
from ...cli import find_app

#: Long enough for a large project's startup; the check never blocks a deploy on a hang.
TIMEOUT = 120.0


@checks.register("potatoq", deploy=True)
def check_workers_can_run_tasks(app_configs: Any = None, **kwargs: Any) -> list[checks.CheckMessage]:
    app = current_app()
    urlconf = getattr(settings, "ROOT_URLCONF", None)
    if urlconf:
        # Tasks defined in views register when the URLconf imports them, as in a request.
        try:
            importlib.import_module(urlconf)
        except Exception:
            pass  # Django's own URL checks report it
    tasks = {name: task for name, task in app.tasks.items() if not name.startswith("potatoq.")}
    try:
        missing = _unresolvable_in_worker(sorted(tasks))
    except Exception as exc:
        return [
            checks.Warning(
                f"Couldn't start Django the way a worker does, to check which tasks it can run: {exc}",
                id="potatoq.W001",
            )
        ]
    errors = []
    for name in missing:
        module = type(tasks[name]).__module__
        errors.append(
            checks.Error(
                f"Task {name!r} is registered in this process, but workers don't import its module "
                f"{module!r}: they would dead-letter it as unregistered.",
                hint=(
                    "Move the task to the app's tasks.py, or add its module to the 'imports' setting "
                    f'(POTATOQ = {{"imports": [{module!r}]}}), or set "worker_import_urlconf": True.'
                ),
                id="potatoq.E001",
            )
        )
    return errors


@checks.register("potatoq")
def check_beat_scheduler(app_configs: Any = None, **kwargs: Any) -> list[checks.CheckMessage]:
    """django-celery-beat's schedules live in its tables, which potatoq doesn't read."""
    scheduler = current_app().conf.get("beat_scheduler")
    if "django_celery_beat" not in str(scheduler or ""):
        return []
    return [
        checks.Warning(
            f"beat_scheduler is {scheduler!r}: potatoq ignores it, so the periodic tasks stored in "
            "django-celery-beat's tables don't run.",
            hint="Move them to the beat_schedule setting and remove beat_scheduler; see "
            "https://anze3db.github.io/potatoq/migrating-from-celery/#django-celery-beat",
            id="potatoq.W002",
        )
    ]


def _unresolvable_in_worker(names: list[str]) -> list[str]:
    """The ``names`` that a freshly started worker process couldn't run."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    proc = subprocess.run(
        [sys.executable, "-m", "potatoq.contrib.django.checks"],
        input=json.dumps(names),
        capture_output=True,
        text=True,
        env=env,
        timeout=TIMEOUT,
        check=False,
    )
    lines = proc.stdout.strip().splitlines()
    if proc.returncode or not lines:
        raise RuntimeError((proc.stderr.strip().splitlines() or [f"exit code {proc.returncode}"])[-1])
    return json.loads(lines[-1])


def _worker_view() -> None:
    """In the fresh process: load the app and import task modules as ``potatoq worker``
    does, then print which of the names on stdin it can't resolve."""
    names = json.loads(sys.stdin.read())
    app = find_app(None)
    app.loader_import_default_modules()
    if app.conf.worker_import_urlconf and getattr(settings, "ROOT_URLCONF", None):
        importlib.import_module(settings.ROOT_URLCONF)
    print(json.dumps([name for name in names if app.resolve_task(name) is None]))


if __name__ == "__main__":
    _worker_view()
