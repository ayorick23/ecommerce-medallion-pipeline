-- Marca de agua de un hecho incremental (ADR 0029): el mayor _visible_desde ya
-- cargado. Si la tabla existe pero está vacía, max() es NULL y "x > NULL" nunca
-- es verdadero: el hecho no volvería a cargar nada. Pasa cuando el primer día
-- de Gold no tiene filas para ese hecho (p. ej. una review que espera a su
-- pedido, ADR 0020); lo encontró la equivalencia del fixture de Gold (ADR 0036).
{% macro marca_de_agua() %}
    (select coalesce(max(_visible_desde), cast('1900-01-01' as timestamp)) from {{ this }})
{% endmacro %}
