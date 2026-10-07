"""``python manage.py potatoq worker`` (any ``potatoq`` CLI command works)."""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run potatoq commands (worker, beat, status, queues, dead, ...) with Django loaded"

    def add_arguments(self, parser):  # type: ignore[no-untyped-def]
        parser.add_argument("args", nargs="*")

    def run_from_argv(self, argv):  # type: ignore[no-untyped-def]
        from ....app import current_app
        from ....cli import main

        raise SystemExit(main(["-A", current_app(), *argv[2:]]))

    def handle(self, *args, **options):  # type: ignore[no-untyped-def]
        from ....app import current_app
        from ....cli import main

        return str(main(["-A", current_app(), *args]) or "")
