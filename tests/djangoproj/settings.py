import os
import tempfile

SECRET_KEY = "test"
DEBUG = True
USE_TZ = True
TIME_ZONE = "Europe/Ljubljana"
INSTALLED_APPS = ["django.contrib.contenttypes", "potatoq.contrib.django", "djangoproj.shop"]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.environ.get("TEST_DJANGO_DB") or os.path.join(tempfile.mkdtemp(), "db.sqlite3"),
    }
}
CELERY_TASK_ACKS_LATE = True  # old Celery settings are read (and this one is the default anyway)
POTATOQ = {"task_default_queue": "shop"}
