# Runbook

## Prereqs

- Python 3.12 via `uv` (`uv python install 3.12`), Docker, OpenTofu ≥ 1.9, AWS CLI.
- AWS credentials for a user/role allowed to create the bootstrap + infra resources.

## First-time setup (Phase 0)

1. `cp .env.example .env` and fill in values (never commit `.env`).
2. `make install` — installs deps, writes `uv.lock`.
3. `make lint typecheck test` — must be green before any other work.
4. `make infra-bootstrap` — one-time state backend (S3 + DynamoDB).
5. `make infra-plan` — review the output. Budget alert is created first.
6. `make infra-apply` — **only with explicit human approval.**
7. Create an access key for the `tickstream-pipeline` IAM user, put it in `.env`.

## Daily commands

See `make help`. `make up` / `make down` / `make logs` (Phase 1+), `make quality` /
`make dbt-build` (Phase 2+).

## Tear down

`tofu destroy` inside `infra/` — **approval required** (AGENTS.md safety rules). Empty
the lake bucket first (`force_destroy = false` blocks destroying a non-empty bucket).
