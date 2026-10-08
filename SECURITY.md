# Security policy

## Supported versions

Security fixes go into the latest release (CalVer, e.g. `26.3`). Please upgrade to the
newest version before reporting.

## Reporting a vulnerability

**Please don't open a public issue.** Report it privately through GitHub:
[Security → Report a vulnerability](https://github.com/anze3db/potatoq/security/advisories/new).

Include what you found, how to reproduce it, and the impact as you understand it. You'll
get an acknowledgement within a few days. We'll keep you updated on the fix, credit you
in the advisory unless you prefer otherwise, and publish the advisory once a fixed
release is out.

## Security model

potatoq trusts its broker and result backend. Things worth knowing when you deploy it:

- **Anyone who can write to the broker can run your tasks** with arguments of their
  choosing. They can only run tasks that are registered in your code: messages are
  JSON, never pickle, so broker access isn't arbitrary code execution. Protect broker
  credentials like database credentials, and use TLS (`rediss://`, `amqps://`,
  `sslmode=require`) across untrusted networks.
- **Exceptions in results are rebuilt only from classes already imported** in the
  reading process; nothing is imported because a result says so.
- **The Django `django.tasks` integration** resolves task names from messages, and only
  imports modules inside `INSTALLED_APPS`.
- **Task arguments and results are stored in plain text** in the broker or database, so
  don't pass secrets as task arguments. Pass an id and look the secret up inside the task.
