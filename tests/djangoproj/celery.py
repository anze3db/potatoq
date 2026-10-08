"""The documented "Keeping your celery.py" pattern, for ``potatoq -A djangoproj.celery:app``."""

import os

from potatoq import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djangoproj.settings")
app = Celery("djangoproj")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
