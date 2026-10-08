"""dbt runner: command construction, deps-on-first-run, exit-code propagation."""

import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

from pipeline.scheduler import dbt_runner
from pipeline.scheduler.dbt_runner import (
    CommandRunner,
    dbt_binary,
    dbt_command,
    run_dbt_build,
)


def test_dbt_command_binds_project_and_profiles_dirs(tmp_path: Path) -> None:
    cmd = dbt_command(Path("/venv/bin/dbt"), tmp_path, "build")
    assert cmd == [
        "/venv/bin/dbt",
        "build",
        "--project-dir",
        str(tmp_path),
        "--profiles-dir",
        str(tmp_path),
    ]


def _recording_runner(codes: list[int]) -> tuple[list[tuple[list[str], Path]], CommandRunner]:
    calls: list[tuple[list[str], Path]] = []

    def fake(cmd: Sequence[str], cwd: Path) -> int:
        calls.append((list(cmd), cwd))
        return codes.pop(0)

    return calls, fake


def test_first_run_installs_deps_then_builds(tmp_path: Path) -> None:
    calls, fake = _recording_runner([0, 0])
    assert run_dbt_build(tmp_path, runner=fake) == 0
    verbs = [cmd[1] for cmd, _ in calls]
    assert verbs == ["deps", "build"]


def test_deps_failure_skips_build(tmp_path: Path) -> None:
    calls, fake = _recording_runner([1])
    assert run_dbt_build(tmp_path, runner=fake) == 1
    assert [cmd[1] for cmd, _ in calls] == ["deps"]


def test_existing_packages_skip_deps(tmp_path: Path) -> None:
    (tmp_path / "dbt_packages").mkdir()
    calls, fake = _recording_runner([0])
    assert run_dbt_build(tmp_path, runner=fake) == 0
    assert [cmd[1] for cmd, _ in calls] == ["build"]


def test_dbt_binary_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "executable", "/nonexistent/python")
    monkeypatch.setattr(dbt_runner.shutil, "which", lambda name: None)
    with pytest.raises(FileNotFoundError):
        dbt_binary()
