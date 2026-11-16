# 0038 — Contexto para agentes de IA: qué carga siempre, qué bajo demanda y qué se hace cumplir

**Estado:** Aceptada
**Fecha:** 2026-11-15

## Contexto

El repo se construye con Claude Code. Hasta ahora todo su contexto era un
`CLAUDE.md` de 78 líneas más la memoria automática de Claude, y en una sola
sesión aparecieron los problemas de ese esquema:

- **Reglas que eran solo peticiones.** "Nunca commitear sobre `main`" y "sin
  línea de coautoría" estaban en prosa. Claude Code trata `CLAUDE.md` como
  contexto, no como configuración obligatoria; en la sesión, una instrucción
  del sistema pedía justo agregar la línea de coautoría, y solo la prosa lo
  impedía.
- **Contexto desactualizado.** El `CLAUDE.md` no mencionaba `gold-build`,
  `gold-dbt`, sqlfmt ni el fixture de Gold: se había quedado en la Fase 3.
  La memoria automática apuntaba a una nota inexistente y daba mal el número
  de la siguiente ADR.
- **Todo cargaba siempre.** Las convenciones de dbt (capas, marca de agua,
  contratos) solo importan al tocar Gold, pero no había dónde ponerlas sin
  pagar su costo de contexto en cada sesión.
- **Estado perdido entre máquinas.** Al retomar se preguntaba "¿en qué
  quedamos?" y la respuesta salía de la memoria local de cada PC. Una rama
  remota con 4 commits de la Fase 4 hechos el 2026-10-02 desde otra máquina
  nunca se mergeó, y el mismo trabajo se rehízo el 2026-11-15 sin saberlo
  (con un choque de numeración en la ADR 0037).

## Decisión

Cada pieza de contexto va donde corresponde según **quién es su dueño** y
**cuándo tiene que estar disponible**:

| Pieza | Dónde | Git | Cuándo entra |
| --- | --- | --- | --- |
| Lo que aplica siempre: proyecto, fuentes de verdad, comandos, reglas, mapa del resto | `CLAUDE.md` (~70 líneas) | sí | Siempre |
| Convenciones de Gold y dbt | `.claude/rules/dbt-gold.md` con `paths:` (`dbt/**`, `src/medallion/gold/**`, `tests/integration/gold/**`) | sí | Al leer o editar un archivo que coincide |
| Procedimientos | `.claude/skills/nueva-adr/` y `.claude/skills/retomar/` | sí | La descripción siempre; el cuerpo al invocarla |
| Permisos del equipo | `.claude/settings.json` → `permissions` | sí | Nunca: los aplica Claude Code |
| Reglas que no pueden fallar | `.claude/settings.json` → hook `PreToolUse` + `.claude/hooks/guardia_git.py` | sí | Nunca: los aplica Claude Code |
| Preferencias personales | `CLAUDE.local.md`, `.claude/settings.local.json` | no (`.gitignore`) | Siempre, solo para quien los tiene |
| Lo que Claude aprende del usuario | memoria automática (`~/.claude/projects/.../memory/`) | no | Índice siempre; notas bajo demanda |

En concreto:

- **Hook `guardia_git.py`** (Bash y PowerShell): bloquea commit y merge
  sobre `main`, push a `main`, líneas de coautoría y `--no-verify`. Sale con
  código 2 y el motivo en stderr. Tiene tests (`tests/claude/`) y entra en
  mypy de pre-commit y de la CI.
- **Permisos:** `allow` para los comandos del pipeline y de calidad, `ask`
  para `git reset --hard`, `git clean` y `git rebase`, `deny` para
  `git push --force`, leer `.env` y escribir en `data/raw/`.
- **`/nueva-adr`:** numeración, plantilla, regla de "superada por" e índice.
  Solo la invoca el usuario (`disable-model-invocation`): las decisiones se
  toman en conjunto.
- **`/retomar`:** deriva el estado de git (rama, cambios, ramas sin mergear
  después de un `fetch`), de los PRs abiertos y de las fases cerradas del
  plan, con inyección dinámica de comandos. La memoria es una pista que se
  verifica. Claude la invoca solo cuando se pregunta en qué quedamos.
- **Sin MCP:** ningún sistema externo lo justifica hoy.

## Alternativas consideradas

- **Todo en `CLAUDE.md`:** un solo archivo, pero carga siempre lo que solo
  importa a veces, crece sin control y deja las reglas críticas como
  peticiones.
- **`dbt/CLAUDE.md` anidado en lugar de una regla:** carga bajo demanda
  igual, pero solo al tocar archivos dentro de `dbt/`; las convenciones de
  Gold también aplican a `src/medallion/gold/` y a sus tests.
- **Partir `CLAUDE.md` con imports (`@ruta`):** ordena, pero los importados
  cargan junto con el archivo que los importa: no ahorra contexto.
- **Reglas de git solo como hooks de git** (`commit-msg`, `pre-push`):
  cubrirían también a las personas, pero no ven el comando antes de
  ejecutarlo ni le explican al agente por qué se bloqueó. Compatibles con
  esta decisión si algún día hacen falta.
- **`/cerrar-sesion` con una nota versionada de traspaso**, leída por
  `/retomar`: guardaría la intención acordada en cualquier máquina, pero
  suma un archivo que cambia en cada sesión y una disciplina más. Se
  prefirió derivar el estado de git y dejar la intención en commits y PRs.
- **Subagente revisor de SQL incremental:** útil (aísla una revisión
  ruidosa), pero no entra en esta sesión; queda como mejora.
- **`AGENTS.md`:** el formato que leen varias herramientas de agentes. Con
  una sola herramienta en uso, `CLAUDE.md` alcanza; si se suma otra, un
  `CLAUDE.md` que importe `@AGENTS.md` evita duplicar.

## Por qué

El contexto de un agente es un presupuesto finito y se pudre si nadie lo
mantiene. El criterio es el mismo que el resto del proyecto ya aplica:
lo que no puede fallar se hace cumplir con una herramienta (como pre-commit
con el formato y los tests de dbt con los datos), no con una instrucción; el
detalle se carga cuando hace falta; y hay una sola fuente de verdad,
versionada y revisada en PRs. La memoria del agente es una caché, no un
registro.

## Consecuencias

- Las reglas de git dejan de depender de que el modelo las recuerde. El
  hook mira el texto del comando: no ve un mensaje de commit que venga de un
  archivo (`-F archivo`) ni un `git checkout main && git commit`. Cubre lo
  que el agente hace normalmente; no es una barrera de seguridad.
- Hay que mantener el contexto como código: una ADR que cambie algo que
  describe una regla o el `CLAUDE.md` los actualiza en el mismo PR
  (`/nueva-adr` lo recuerda).
- Las reglas y skills nuevas pueden no aparecer en una sesión ya iniciada:
  en la que las creó, `/retomar` apareció con retraso y la regla no llegó a
  cargarse. Se verifican en una sesión nueva (`/context`, invocar la skill).
- Las reglas `allow` de `.claude/settings.json` aplican solo después de que
  cada persona confía en la carpeta; `deny`, `ask` y los hooks, siempre.
- `/retomar` encontró en su primera prueba la rama sin mergear del
  2026-10-02, que queda pendiente de revisar.
