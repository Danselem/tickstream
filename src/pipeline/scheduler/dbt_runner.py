"""Subprocess wrapper around the dbt CLI. Returns exit codes, never raises on dbt failure."""

import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from pipeline.common.logging import get_logger

logger = get_logger("scheduler.dbt")

CommandRunner = Callable[[Sequence[str], Path], int]


def repo_root() -> Path:
    """Repo root resolved from this file: <root>/src/pipeline/scheduler/*.py."""
    return Path(__file__).resolve().parents[3]


def dbt_binary() -> Path:
    """dbt from the current venv (host `uv run`, container /app/.venv)."""
    candidate = Path(sys.executable).with_name("dbt")
    if candidate.is_file():
        return candidate
    found = shutil.which("dbt")
    if found is None:
        raise FileNotFoundError("dbt binary not found next to sys.executable or on PATH")
    return Path(found)


def _default_runner(cmd: Sequence[str], cwd: Path) -> int:
    completed = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    logger.info(
        "dbt_finished",
        cmd=list(cmd[1:]),
        returncode=completed.returncode,
        stdout_tail=completed.stdout[-2000:],
        stderr_tail=completed.stderr[-2000:],
    )
    return completed.returncode


def dbt_command(binary: Path, directory: Path, verb: str) -> list[str]:
    """`dbt <verb>` bound to our project + profiles dirs."""
    return [str(binary), verb, "--project-dir", str(directory), "--profiles-dir", str(directory)]


def run_dbt_build(
    dbt_dir: Path | None = None,
    runner: CommandRunner = _default_runner,
) -> int:
    """`dbt deps` (first time) then `dbt build`. Returns the dbt exit code."""
    directory = dbt_dir if dbt_dir is not None else repo_root() / "dbt"
    binary = dbt_binary()
    if not (directory / "dbt_packages").exists():
        logger.info("dbt_deps_installing")
        returncode = runner(dbt_command(binary, directory, "deps"), directory)
        if returncode != 0:
            logger.error("dbt_deps_failed", returncode=returncode)
            return returncode
    return runner(dbt_command(binary, directory, "build"), directory)
