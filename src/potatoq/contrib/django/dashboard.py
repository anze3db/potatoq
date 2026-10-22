"""The dashboard inside a Django project, for staff users::

    urlpatterns = [
        path("potatoq/", include("potatoq.contrib.django.dashboard")),
        ...
    ]

Anonymous visitors are sent to ``LOGIN_URL``; signed-in users who aren't staff get a
403. ``POTATOQ_DASHBOARD_READ_ONLY = True`` turns off the retry and revoke buttons.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from django.conf import settings
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, HttpResponseRedirect
from django.shortcuts import resolve_url
from django.urls import re_path
from django.views.decorators.csrf import csrf_exempt

from ...app import current_app
from ...dashboard import Dashboard

_cached: list[Dashboard] = []


def _dashboard() -> Dashboard:
    app = current_app()
    if not _cached or _cached[0].app is not app:
        read_only = bool(getattr(settings, "POTATOQ_DASHBOARD_READ_ONLY", False))
        _cached[:] = [Dashboard(app, read_only=read_only)]
    return _cached[0]


# Django's CSRF token isn't needed: actions require the X-Potatoq-Dashboard header,
# which a cross-site form or script can't send (see potatoq.dashboard).
@csrf_exempt
def dashboard(request: HttpRequest, path: str = "") -> HttpResponse:
    user: Any = getattr(request, "user", None)
    if user is None or not user.is_authenticated:
        login = resolve_url(settings.LOGIN_URL)
        return HttpResponseRedirect(f"{login}?{urlencode({'next': request.get_full_path()})}")
    if not (user.is_active and user.is_staff):
        return HttpResponseForbidden("The potatoq dashboard is for staff users")
    headers = {name.lower(): value for name, value in request.headers.items()}
    # Django strips the include() prefix and its slash, so "" is the dashboard's root.
    result = _dashboard().handle(request.method or "GET", "/" + path, request.META.get("QUERY_STRING", ""), headers)
    response = HttpResponse(result.body, status=result.status)
    for name, value in result.headers:
        response[name] = value
    return response


urlpatterns = [re_path(r"^(?P<path>.*)$", dashboard, name="potatoq-dashboard")]
