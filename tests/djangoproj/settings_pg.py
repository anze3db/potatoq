import os
from urllib.parse import unquote, urlsplit

from .settings import *  # noqa: F403

# The same database as the rest of the suite (POTATOQ_TEST_POSTGRES in CI).
_url = urlsplit(os.environ.get("POTATOQ_TEST_POSTGRES", "postgresql://localhost/potatoq_test"))
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": unquote(_url.path.lstrip("/")),
        "HOST": _url.hostname or "localhost",
        "PORT": _url.port or "",
        "USER": unquote(_url.username or ""),
        "PASSWORD": unquote(_url.password or ""),
    }
}
POTATOQ = {
    "task_default_queue": "shop",
    "broker_transport_options": {"schema": os.environ["TEST_PG_SCHEMA"]},
}
