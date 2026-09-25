# 0020 — Reviews que llegan antes que su pedido: plazo de gracia y luego fail-fast

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

64 reviews tienen `review_creation_date` anterior al día de compra de su
pedido: llegan a Bronze entre 1 y **111** días antes que el pedido
(mediana 16); 57 de ellas son de pedidos cancelados. En Silver(D) son
huérfanas de FK temporales (*early-arriving facts*).

La regla fail-fast "huérfano de FK no documentado" (`docs/schemas.md`,
sección 4) tiene que distinguir "el pedido todavía no llegó" de "el pedido
no existe". Un sistema real no ve el futuro: la única forma de separarlos
es el tiempo.

Es el único caso. Items y pagos llegan con la compra de su pedido (ADR
0004), los 2 pedidos despachados antes de su compra aparecen el día de la
compra (ADR 0019), y las FK contra tablas de referencia apuntan a
snapshots completos.

## Decisión

- Una review visible en Silver(D) cuyo pedido todavía no existe en
  `silver_orders` queda **pendiente**: se excluye de
  `silver_order_reviews` y se escribe en `silver/_pendientes/order_reviews.parquet`,
  con los días que lleva esperando.
- Si lleva **más de 120 días** esperando, es un huérfano real: entra en el
  reporte de `SilverValidationError` (ADR 0023) y el pipeline se detiene.
- El plazo es configurable en `config/pipeline.yaml`. 120 días sale de los
  datos: el máximo observado es 111.
- Cuando el pedido llega, la review entra sola a Silver en esa corrida: con
  la reconstrucción completa (ADR 0017), el archivo de pendientes se
  recalcula en cada corrida y no tiene estado propio.

## Alternativas consideradas

- **Excluir sin plazo:** una review con un pedido que no existe esperaría
  para siempre sin avisar.
- **Incluir la review con la FK sin resolver** (*inferred member*): patrón
  estándar, pero rompe la garantía de integridad referencial de Silver y
  obliga a Gold a filtrar; la lógica queda repartida entre dos capas.
- **Fail-fast inmediato:** frenaría el pipeline en decenas de días con
  datos legítimos.

## Por qué

Es la única alternativa que separa "todavía no llegó" de "no existe" y
mantiene la integridad referencial de Silver. Detectar tarde un huérfano
real es el costo honesto de no poder ver el futuro, y queda acotado por un
plazo que sale de los datos.

No contradice la ADR 0001 (sin cuarentena): las reviews pendientes no son
registros inválidos, sino válidos a los que les falta el padre. Ningún
registro inválido pasa en silencio: al vencer el plazo, el hard stop se
aplica igual.

## Consecuencias

- Un huérfano real se detecta hasta 120 días tarde.
- Silver del último día del replay debe tener 0 pendientes.
- Si la fuente cambiara y una review legítima superara los 120 días, el
  pipeline se detendría y habría que revisar el plazo. Es el comportamiento
  buscado.
