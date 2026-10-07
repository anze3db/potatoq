# Flask

```python title="app.py"
from flask import Flask
from potatoq.contrib.flask import init_app

app = Flask(__name__)
app.config["POTATOQ_BROKER_URL"] = "redis://localhost:6379/0"   # CELERY_* keys work too
potatoq = init_app(app)


@potatoq.task
def send_email(user_id):
    user = db.session.get(User, user_id)   # runs inside app.app_context()
    ...
```

`init_app(flask_app, app=None)` (pass an existing `Potatoq` app, or one is created):

- reads settings from `app.config`: `POTATOQ_*`, `CELERY_*`, or a `POTATOQ` dict;
- runs every task inside `flask_app.app_context()`, the pattern Flask's docs make you
  build by hand for Celery;
- if Flask-SQLAlchemy is installed, defers `.delay()` inside a `db.session` transaction
  until it commits;
- stores the app as `app.extensions["potatoq"]`.

```console
$ potatoq -A app:potatoq worker
```
