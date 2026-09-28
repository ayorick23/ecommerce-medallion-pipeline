select estado, descripcion, categoria, es_terminal, orden from {{ ref('estados_pedido') }}
