"""Flask integration.

    from flask import Flask
    from potatoq import Potatoq
    from potatoq.contrib.flask import init_app

    flask_app = Flask(__name__)
    flask_app.config["POTATOQ_BROKER_URL"] = "redis://localhost"   # or CELERY_BROKER_URL
    potatoq = init_app(flask_app)                                  # or init_app(flask_app, existing_app)

* Settings are read from ``app.config`` (``POTATOQ_*``, ``CELERY_*`` or a ``POTATOQ`` dict).
* Every task runs inside ``flask_app.app_context()``, so ``current_app``, Flask-SQLAlchemy's
  ``db.session`` etc. just work in tasks (the pattern Flask's docs build by hand for Celery).
* With Flask-SQLAlchemy installed, ``.delay()`` inside a ``db.session`` transaction is
  sent when it commits.
* The app is available as ``flask_app.extensions["potatoq"]``.
"""

from __future__ import annotations

from typing import Any

from ..app import Potatoq
from ..task import Task


def init_app(flask_app: Any, app: Potatoq | None = None) -> Potatoq:
    if app is None:
        app = Potatoq(flask_app.import_name)
    config = flask_app.config
    app.conf.update_from_mapping({k: v for k, v in config.items() if k.startswith("CELERY_")}, namespace="CELERY")
    app.conf.update_from_mapping({k: v for k, v in config.items() if k.startswith("POTATOQ_")}, namespace="POTATOQ")
    app.conf.update_from_mapping(config.get("POTATOQ") or config.get("CELERY") or {})

    base = app.Task

    class FlaskTask(base):  # type: ignore[valid-type, misc]
        abstract = True

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            with flask_app.app_context():
                return Task.__call__(self, *args, **kwargs)

    app.Task = FlaskTask
    # Re-base tasks that were defined before init_app.
    for task in list(app.tasks.values()):
        cls = type(task)
        if issubclass(cls, Task) and not issubclass(cls, FlaskTask) and cls.__bases__ == (base,):
            cls.__bases__ = (FlaskTask,)

    try:
        from flask_sqlalchemy import SQLAlchemy  # noqa: F401
    except ImportError:
        pass
    else:
        from .sqlalchemy import install

        install(app)

    flask_app.extensions["potatoq"] = app
    app._reset_connections()
    return app
