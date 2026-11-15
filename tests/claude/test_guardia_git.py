"""Tests del hook .claude/hooks/guardia_git.py (ADR 0038).

El hook no es parte del paquete ``medallion``: se carga desde su ruta.
"""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

HOOK = Path(__file__).resolve().parents[2] / ".claude" / "hooks" / "guardia_git.py"


def _cargar() -> ModuleType:
    spec = importlib.util.spec_from_file_location("guardia_git", HOOK)
    assert spec is not None and spec.loader is not None
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


guardia = _cargar()


@pytest.mark.parametrize(
    "comando",
    [
        'git commit -m "feat: algo"',
        "git merge feat/fase-4-gold",
        "cd repo && git commit -am wip",
        "git push",
        "git push origin",
    ],
)
def test_blocks_commit_merge_and_bare_push_on_main(comando: str) -> None:
    assert guardia.motivo_de_bloqueo(comando, "main") is not None


@pytest.mark.parametrize(
    "comando",
    [
        "git push origin main",
        "git push origin HEAD:main",
        "git push -u origin feat/x:refs/heads/main",
        "git push origin +main",
    ],
)
def test_blocks_push_to_main_from_any_branch(comando: str) -> None:
    assert guardia.motivo_de_bloqueo(comando, "feat/x") is not None


def test_blocks_co_author_line_in_heredoc_message() -> None:
    comando = "git commit -F - <<'EOF'\nfeat: algo\n\nCo-Authored-By: Alguien <a@b.c>\nEOF"
    motivo = guardia.motivo_de_bloqueo(comando, "feat/x")
    assert motivo is not None and "coautoría" in motivo


def test_blocks_no_verify() -> None:
    assert guardia.motivo_de_bloqueo("git commit --no-verify -m x", "feat/x") is not None


@pytest.mark.parametrize(
    "comando",
    [
        'git commit -m "feat: algo"',
        "git push -u origin feat/x",
        "git push",
        "git status && git diff",
        "git checkout main",
        "git log --oneline main",
        "uv run pytest",
    ],
)
def test_allows_normal_work_on_a_feature_branch(comando: str) -> None:
    assert guardia.motivo_de_bloqueo(comando, "feat/x") is None


@pytest.mark.parametrize("comando", ["git status", "git checkout -b docs/x", "git pull"])
def test_allows_reading_and_branching_on_main(comando: str) -> None:
    assert guardia.motivo_de_bloqueo(comando, "main") is None


def _ejecutar(comando: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    evento = {"tool_name": "Bash", "tool_input": {"command": comando}, "cwd": str(cwd)}
    return subprocess.run(
        [sys.executable, str(HOOK)], input=json.dumps(evento), capture_output=True, text=True
    )


def test_end_to_end_exit_code_2_with_reason_on_stderr(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main", str(tmp_path)], check=True)

    bloqueado = _ejecutar('git commit -m "x"', tmp_path)
    permitido = _ejecutar("git status", tmp_path)

    assert bloqueado.returncode == 2 and "nunca se commitea" in bloqueado.stderr
    assert permitido.returncode == 0 and permitido.stderr == ""
