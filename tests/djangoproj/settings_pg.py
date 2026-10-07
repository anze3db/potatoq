from .settings import *  # noqa: F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "potatoq_test",
        "HOST": "localhost",
    }
}
POTATOQ = {
    "task_default_queue": "shop",
    "broker_transport_options": {"schema": __import__("os").environ["TEST_PG_SCHEMA"]},
}
