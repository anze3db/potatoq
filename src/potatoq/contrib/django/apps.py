from django.apps import AppConfig


class PotatoqConfig(AppConfig):
    name = "potatoq.contrib.django"
    label = "potatoq"
    verbose_name = "Potatoq"

    def ready(self) -> None:
        from ...app import _get_default_app, current_app

        app = current_app()
        if app is _get_default_app():
            return  # configured by _get_default_app() already
        from . import configure_app

        configure_app(app)
