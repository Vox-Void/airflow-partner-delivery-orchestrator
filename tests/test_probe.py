import runpy
import subprocess
from pathlib import Path

import pytest

PROBE = runpy.run_path(str(Path(__file__).parent / "integration" / "probe.py"))


def test_probe_preserves_startup_error_when_diagnostics_and_cleanup_also_fail(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    stack = PROBE["Stack"]("probe-test", tmp_path / "probe.env")
    stack.token = "test-token"
    monkeypatch.setattr(stack, "runs", lambda _: [{}])
    commands = []

    def fail(command, **kwargs):
        action = command[6]
        commands.append(action)
        if action == "up":
            raise FileNotFoundError("Docker startup failed")
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(FileNotFoundError, match="Docker startup failed"):
        PROBE["run_isolated"](stack, lambda _: {})

    assert commands == ["up", "ps", "logs", "down"]
    diagnostics = capsys.readouterr().err
    assert "Could not collect task logs" in diagnostics
    assert "Could not remove Compose project" in diagnostics


def test_probe_fails_if_cleanup_after_a_successful_scenario_fails(
    tmp_path: Path, monkeypatch
) -> None:
    stack = PROBE["Stack"]("probe-test", tmp_path / "probe.env")
    monkeypatch.setattr(stack, "start", lambda **_: {"worker": "healthy"})

    def fail(command, **_):
        return subprocess.CompletedProcess(command, 1, "", "Compose teardown failed")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="Compose teardown failed"):
        PROBE["run_isolated"](stack, lambda _: {"published": True})
