{#
    Marca de agua de un hecho incremental (ADR 0029): el mayor `_visible_desde`
    ya cargado. Un hecho que quedó vacío en una corrida anterior (p. ej. Gold
    que arranca un día sin reviews) daría max() = null, y `x > null` es null:
    no cargaría ninguna fila nunca más, sin error. Con -infinity carga todo.
#}
{% macro marca_de_agua() -%}
    coalesce((select max(_visible_desde) from {{ this }}), cast('-infinity' as timestamp))
{%- endmacro %}
