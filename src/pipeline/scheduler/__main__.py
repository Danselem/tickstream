"""Scheduler entrypoint: `python -m pipeline.scheduler [--once ...]`."""

from pipeline.scheduler.run import main

if __name__ == "__main__":
    raise SystemExit(main())
