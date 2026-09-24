# 0014 — `batch_hash`: codificación canónica con prefijo de longitud

**Estado:** Aceptada
**Fecha:** 2026-09-24
**Supera parcialmente a:** [0013](0013-batch-hash-contenido-canonico.md)
(solo la serialización y el orden de filas; el resto de la 0013 sigue vigente)

## Contexto

La ADR 0013 fijó que `batch_hash` es el SHA-256 de una serialización
canónica del batch, y definió esa serialización como "CSV con una
representación fija de los nulos", con filas ordenadas por todas las
columnas. Al diseñar `src/bronze/hashing.py` se vio que esa definición no
es canónica:

- **Un marcador de nulo puede colisionar con un valor real.** Si el nulo
  se escribe como un texto fijo (p. ej. `\N`), una fila cuyo valor sea
  literalmente `\N` produce los mismos bytes que una fila con nulo: dos
  batches distintos, mismo hash. En Bronze todo es texto crudo (ADR 0011),
  así que no hay forma de garantizar que un marcador no aparezca en los
  datos.
- **Si se evita el marcador, el hash depende del escritor CSV.** Distinguir
  nulo de cadena vacía, o un valor con comas, comillas o saltos de línea,
  queda en manos de la política de comillas y escapes del escritor CSV de
  Polars. Esa política no es un contrato estable: un cambio de versión
  podría cambiar el hash sin que cambien los datos, que es justo lo que la
  0013 quería evitar al descartar `hash_rows()`.
- **Ordenar por todas las columnas** deja abierto dónde van los nulos y
  qué criterio de comparación se usa, y lo hace depender de la semántica
  de ordenamiento de la librería.

## Decisión

La serialización canónica del batch se define así (todo en UTF-8):

1. **Codificación de un valor:**
   - nulo → `N`
   - no nulo → `S<n>:<valor>`, donde `<n>` es la longitud del valor en
     **bytes UTF-8** (no en caracteres), en decimal.
2. **Registro:** concatenación de los valores codificados de sus columnas,
   en el orden de columnas de la fuente, terminado en `\n`.
3. **Encabezado:** un primer registro con los **nombres de las columnas**
   fuente, codificados con la misma regla.
4. **Filas:** un registro por fila (solo columnas fuente, sin linaje,
   ADR 0013), **ordenados por su codificación**, comparando bytes
   (orden lexicográfico). Las filas duplicadas se conservan.
5. `batch_hash` = SHA-256 (hex) de `encabezado + filas ordenadas`.

Ejemplo con las columnas `order_id` y `review_comment_title` (cada línea
es un registro terminado en `\n`; las filas ya están en orden final):

```text
S8:order_idS20:review_comment_title     ← encabezado
S3:abcN                                 ← ("abc", nulo)
S3:abcS0:                               ← ("abc", "")
S3:abcS1:N                              ← ("abc", "N")
S3:xyzS10:São Paulo                     ← "São Paulo": 9 caracteres, 10 bytes
```

La codificación se construye con expresiones de Polars (vectorizado); solo
el cálculo final del SHA-256 ocurre en Python (`hashlib`).

## Alternativas consideradas

- **CSV con marcador de nulo** (ADR 0013 tal cual): ambiguo, ver contexto.
- **CSV sin marcador, confiando en el escritor de Polars**: el hash queda
  atado a detalles de formato no garantizados entre versiones.
- **Separadores con escape** (p. ej. `\x1f` entre campos y escapar ese
  carácter si aparece en un valor): también es canónico, pero exige
  implementar y testear el escape correctamente; el prefijo de longitud no
  necesita escapes porque nunca "busca" un separador dentro del valor.
- **JSON por fila**: canónico solo si se fija la forma de escapar
  caracteres y Unicode, lo que vuelve a depender del serializador.

## Por qué

Con prefijo de longitud cada valor se puede leer sin ambigüedad: `N` y
`S` se distinguen por la primera letra, y `<n>` dice exactamente cuántos
bytes leer, sin importar qué contenga el valor (comas, `\n`, la letra `N`,
lo que sea). Por lo tanto, dos batches distintos nunca producen la misma
serialización, y la serialización no depende de ninguna librería: se
puede reimplementar a mano en cualquier lenguaje y da los mismos bytes.
Es el mismo principio que usan formatos como netstrings o bencode.

Ordenar por la codificación en lugar de por columnas elimina la pregunta
de dónde van los nulos (su codificación ya tiene un lugar fijo en el orden
de bytes) y reduce el criterio a "comparar bytes", que no depende de la
librería.

Incluir los nombres de columna hace que un cambio de schema (columna
renombrada, agregada o reordenada) cambie el hash, aunque los valores sean
los mismos: para Bronze eso es un cambio real del batch.

## Consecuencias

- `hashing.py` se testea con casos que la 0013 no distinguía: nulo vs
  `""`, nulo vs el texto `N`, valores con `\n` o comas, caracteres
  multibyte, filas duplicadas, y que el orden de las filas de entrada no
  cambie el hash.
- El encabezado cuenta aunque el batch no tenga filas, así que un batch
  vacío también tiene un hash bien definido (aunque, según
  `docs/schemas.md`, un batch vacío no genera partición).
- La distinción nulo vs `""` es tan buena como la lectura del CSV: con la
  configuración actual (ADR 0011) un campo vacío se lee como nulo, así que
  en la práctica `S0:` no aparecerá en datos de Olist. La codificación lo
  soporta igual, por si la lectura cambia.
- Cambiar esta codificación en el futuro cambia todos los hashes. Es
  aceptable: las capas son derivados reproducibles (ADR 0010) y se
  regeneran con un replay.
- El resto de la ADR 0013 no cambia: SHA-256 en hex, solo columnas fuente,
  y comparación de `batch_hash` para no reescribir snapshots de referencia
  sin cambios.
