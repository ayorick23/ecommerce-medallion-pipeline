"""Hook PreToolUse de Claude Code: hace cumplir las reglas de git del CLAUDE.md (ADR 0038).

El CLAUDE.md las *pide*; este hook las *garantiza*. Claude Code le pasa por
stdin el JSON de la llamada a la herramienta (Bash o PowerShell). Si el
comando rompe una regla, sale con código 2: Claude Code bloquea la llamada y
le muestra a Claude el mensaje de stderr. Si no, sale con 0 y sigue el flujo
normal de permisos (el hook nunca *aprueba* nada, solo puede bloquear).

Reglas:

1. No commitear ni mergear estando en ``main`` (o ``master``).
2. No hacer push a ``main``, ni un ``git push`` sin destino estando en ``main``.
3. Ningún commit con una línea de coautoría (``Co-Authored-By``).
4. No saltear los hooks de git (``--no-verify``): pre-commit tiene que pasar.

Límites conocidos: mira el texto del comando, así que no ve un mensaje de
commit que venga de un archivo (``git commit -F mensaje.txt``), y la rama se
consulta antes de que corra el comando (``git checkout main && git commit``
no se detecta). Cubre lo que Claude escribe normalmente; no es una barrera
de seguridad contra alguien que intenta esquivarla.

Solo usa la biblioteca estándar: corre con ``uv run --no-project``.
"""

import json
import re
import subprocess
import sys

RAMAS_PROTEGIDAS = ("main", "master")

# Un "segmento" es un comando simple: se corta en los separadores de la shell.
_SEPARADORES = re.compile(r"&&|\|\||;|\||\n")
_GIT = re.compile(r"(?:^|\s)git(?:\.exe)?\s+(?:-C\s+\S+\s+)?(\S+)(.*)$", re.DOTALL)
_COAUTORIA = re.compile(r"co-authored-by\s*:", re.IGNORECASE)


def rama_actual(cwd: str) -> str | None:
    # symbolic-ref y no rev-parse: funciona en una rama sin commits todavía (repo recién creado).
    try:
        salida = subprocess.run(
            ["git", "symbolic-ref", "--short", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return salida.stdout.strip()


def _empuja_a_protegida(args: str) -> bool:
    destinos = [a for a in args.split() if not a.startswith("-")]
    # git push <remoto> <refspec>...: basta con que una refspec apunte a main.
    for refspec in destinos[1:]:
        destino = refspec.lstrip("+").split(":")[-1]
        if destino.removeprefix("refs/heads/") in RAMAS_PROTEGIDAS:
            return True
    return False


def motivo_de_bloqueo(comando: str, rama: str | None) -> str | None:
    """Por qué hay que bloquear ``comando`` estando en ``rama``, o ``None`` si se permite."""
    if _COAUTORIA.search(comando) and re.search(r"\bgit\b.*\bcommit\b", comando, re.DOTALL):
        return (
            "Los commits de este repo no llevan línea de coautoría (CLAUDE.md). "
            "Quita la línea Co-Authored-By del mensaje."
        )
    en_protegida = rama in RAMAS_PROTEGIDAS
    for segmento in _SEPARADORES.split(comando):
        coincidencia = _GIT.search(segmento.strip())
        if not coincidencia:
            continue
        subcomando, args = coincidencia.group(1), coincidencia.group(2)
        if subcomando in ("commit", "merge", "push") and "--no-verify" in args.split():
            return "No se saltean los hooks de git (--no-verify): pre-commit tiene que pasar."
        if subcomando in ("commit", "merge") and en_protegida:
            return (
                f"Estás en '{rama}': nunca se commitea ni mergea sobre main (CLAUDE.md). "
                "Crea una rama feat/..., fix/... o docs/... primero."
            )
        if subcomando == "push":
            sin_destino = len([a for a in args.split() if not a.startswith("-")]) <= 1
            if _empuja_a_protegida(args) or (sin_destino and en_protegida):
                return (
                    "El push a main lo hace el usuario con un merge en GitHub, "
                    "nunca Claude (CLAUDE.md). Haz push de tu rama."
                )
    return None


def main() -> int:
    evento = json.load(sys.stdin)
    comando = evento.get("tool_input", {}).get("command", "")
    if "git" not in comando:
        return 0
    motivo = motivo_de_bloqueo(comando, rama_actual(evento.get("cwd", ".")))
    if motivo is None:
        return 0
    # Claude Code lee stderr como UTF-8; en Windows Python escribiría en cp1252.
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    print(f"Bloqueado por .claude/hooks/guardia_git.py: {motivo}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
