# Contributing

Thanks for taking the time! Issues and pull requests are welcome.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
make install        # dev dependencies + pre-commit hooks
make check          # lint, types and tests, the same as CI
```

You need Python 3.10+ and Java 17+. The first run downloads the Delta Lake jars.

## Adding a lab

1. Put the logic in `labs/<name>.py` as plain functions that take DataFrames or paths.
2. Write a test in `tests/test_<name>.py` that asserts the **property** the lab claims,
   not just that it runs.
3. Add a demo function to `labs/demo.py` and a row to the table in `README.md`.
4. Add a diagram to `docs/architecture.md`.

## Pull requests

- Keep each PR to one change, and describe what it proves.
- `make check` must pass. CI runs the same checks on Python 3.10, 3.11 and 3.12 and
  in Docker.
- Follow [Conventional Commits](https://www.conventionalcommits.org/) for titles,
  for example `feat: add CDC watermark lab` or `fix: handle null keys in salting`.
