# Minimal Superset config for tickstream (loaded via SUPERSET_CONFIG).
# The custom module *replaces* superset.config, so defaults come first.
import os

from superset.config import *  # noqa: F401,F403  (defaults, overridden below)


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required (see .env.example)")
    return value


SECRET_KEY = _required("SUPERSET_SECRET_KEY")

# SQLite metadata DB on a named volume (survives container rebuilds).
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/metadata.db"

# Single-node portfolio deployment: no proxy, no HTTPS, no extra feature flags.
# Row limit stays at the default (10k) — dashboards aggregate in Athena.
