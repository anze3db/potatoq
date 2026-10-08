"""``python manage.py potatoq worker`` (any ``potatoq`` CLI command works)."""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Run potatoq commands (worker, beat, status, queues, dead, ...) with Django loaded"

    def add_arguments(self, parser):
        parser.add_argument("args", nargs="*")

    def run_from_argv(self, argv):
        import os

        from potatoq.app import current_app
        from potatoq.cli import main

        raise SystemExit(main(["-A", current_app(), *argv[2:]], prog=f"{os.path.basename(argv[0])} potatoq"))

    def handle(self, *args, **options):
        from potatoq.app import current_app
        from potatoq.cli import main

        return str(main(["-A", current_app(), *args]) or "")
