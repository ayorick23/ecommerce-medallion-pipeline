# 0036 — Estrategia de tests de Gold

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

La ADR 0007 llevó la validación Silver→Gold a tests de dbt. El diseño de
esta fase agregó reglas que dbt no cubre de fábrica:

- unicidad de claves compuestas;
- rangos SCD2 sin solape (ADR 0030);
- reglas por fila;
- la equivalencia incremental == full refresh (ADR 0029);
- la escritura atómica (ADR 0028).

Hay además un problema de datos de prueba: el Olist en miniatura de
`tests/integration/conftest.py` tiene 3 pedidos y 8 días. No incluye
ningún cliente con dos direcciones, ninguna cancelación después de una
entrega, ningún `invoiced` y ninguna review con varios pedidos. Con esos
datos, las propiedades de Gold pasarían sin probar nada.

## Decisión

**Paquetes de dbt:**

- `dbt_utils`, con la versión exacta fijada en `packages.yml`, para
  `unique_combination_of_columns`, `mutually_exclusive_ranges`,
  `expression_is_true` y `generate_surrogate_key`.
- El calendario se genera con SQL nativo de DuckDB (`generate_series`), no
  con `date_spine` (ADR 0032).
- `dbt_packages/` va al `.gitignore`, y `dbt deps` es un paso explícito del
  setup y de la CI.
- Un test genérico propio solo si una regla real no la cubre ningún
  paquete.
- Sin `dbt-expectations`.

**Contratos de modelo** (`contract: {enforced: true}`) **solo en los
marts**:

- El build falla si las columnas o sus tipos no coinciden con los del
  `.yml`.
- Staging e intermediate quedan sin contrato: nadie los consume y
  necesitan libertad para iterar.

**Unit tests de dbt** (dbt ≥ 1.8) en los dos modelos intermedios con
lógica real:

- `int_cliente_direcciones` (ADR 0030) e `int_pedido_estado` (ADR 0031).
- Un caso con nombre por cada situación de esas ADRs.
- Staging, que solo renombra y castea, no lleva unit tests.

**Tests de datos:** todos con severidad `error` (el valor por defecto).
`dbt build` corre cada test después de su modelo; si uno falla, se saltean
los modelos que dependen de él, el comando falla y no se publica nada (ADR
0028). No se usa `warn`: sería "seguir con warnings" (ADR 0001).

**Tests con pytest:**

- `tests/unit/gold/`: los comandos `gold-build` y `gold-dbt` (guardas,
  variables de entorno, reemplazo atómico).
- `tests/integration/gold/`: **un fixture propio de Gold**, con su propio
  `conftest.py`.
  - Reusa `write_sources(root, overrides)` para armar un Olist con los
    casos que importan: cliente que cambia de dirección y vuelve a una
    anterior, cancelado después de entregado, `invoiced`, `processing`,
    `unavailable`, entrega tardía, review con varios pedidos y review
    adelantada.
  - Pasa por los comandos reales `bronze-replay` → `silver-build` →
    `gold-build`, día por día.
  - El fixture de Silver no se toca.
- **Ventana real:** full refresh en un día D₀, incremental día por día
  durante unos 30 días y comparación contra un full refresh en D₀+30,
  sobre los datos completos. Se corre en local y el resultado se documenta
  en `docs/schemas.md`, como la corrida real de la Fase 3.

**Formato del SQL con sqlfmt**, el paralelo de `ruff format`:

- Dependencia de desarrollo, con `line_length = 100` en `pyproject.toml`
  (el mismo largo que ruff).
- Hook en pre-commit y paso `sqlfmt --check dbt` en la CI.
- No renderiza Jinja, así que no necesita las variables de entorno de la
  ADR 0027.
- No se agrega un linter de SQL (SQLFluff) en esta fase.

## Alternativas consideradas

- **Todo con `dbt_utils`, incluido `date_spine`:** el estándar, pero
  `date_spine` es más difícil de leer que una línea de SQL nativo.
- **Nada externo, todo con tests genéricos propios:** cero dependencias,
  pero reinventa algo resuelto y probado. `mutually_exclusive_ranges`
  tiene bordes sutiles (rangos abiertos, huecos, extremos iguales) que es
  fácil implementar mal.
- **Contratos en todos los modelos:** fricción en modelos internos sin
  consumidores.
- **Sin contratos:** los tipos de Gold quedarían a lo que infiera DuckDB.
  Por ejemplo, `SUM` sobre `DECIMAL(18,2)` devuelve `DECIMAL(38,2)`, así
  que el tipo de montos de la ADR 0022 podría cambiar sin que nadie lo
  note.
- **Solo tests de datos, sin unit tests:** verifican propiedades del
  resultado ("no hay solapes"), pero no dicen *qué caso* se rompió.
- **Agrandar el fixture de Silver con los casos de Gold:** una sola fuente
  de datos de prueba, pero cambia los resultados esperados de tests ya
  cerrados de la Fase 3 por razones que no son de esa fase.
- **Recorrer los ~780 días reales en la CI:** cada corrida invoca dbt; no
  es viable en la CI y en local no vale la pena. El fixture prueba la
  lógica en cada caso límite; la ventana real, la escala.
- **Sin formateador de SQL:** el estilo dependería de quien escribe; es
  lo contrario de lo que se hace con Python.
- **SQLFluff** (linter y formateador): se puede configurar con la guía de
  estilo de dbt Labs. Su templater de dbt tiene que compilar el proyecto,
  lo que con la ADR 0027 obliga a pasar por el envoltorio. El templater de
  Jinja evita eso, pero exige simular las macros de `dbt_utils`, es lento y
  su corrección automática es menos completa que la de un formateador.
  Las reglas que revisaría las cubren, en lo importante, los tests y
  contratos de dbt. Queda como mejora posible, compatible con sqlfmt.
- **La extensión dbt Formatter de VS Code:** solo existe en el editor, no
  en la CI, y está sin mantenimiento.

## Por qué

Usa el estándar donde aporta y SQL claro donde el paquete estorba. Gold es
la capa de consumo: su esquema es la promesa hacia afuera, igual que los
contratos de la Fase 1 lo fueron hacia adentro, y los contratos de dbt la
hacen cumplir en cada build. Los unit tests convierten cada caso de las
decisiones de diseño en un test con nombre. El fixture propio asegura que
las propiedades de la fase se prueben de verdad y no pasen por falta de
datos.

## Consecuencias

- La CI suma un paso `dbt deps` antes de pytest, y sus tiempos crecen:
  cada `gold-build` invoca dbt. También suma `sqlfmt --check dbt`.
- sqlfmt impone su estilo, sin configuración: `with` en su propia línea y
  CTEs indentadas, y listas largas con un elemento por línea. Se acepta tal
  cual, como con ruff.
- Los contratos obligan a castear explícitamente las columnas de los marts,
  sobre todo los montos después de agregar.
- Cada ADR de modelado (0030–0035) enumera los tests que le corresponden;
  esta ADR fija las herramientas, no la lista completa.
