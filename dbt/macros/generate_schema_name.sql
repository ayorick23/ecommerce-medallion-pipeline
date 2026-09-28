{#
    Por defecto dbt antepone el esquema del target al esquema propio
    ("main_staging"). Aquí el esquema propio se usa tal cual: staging,
    intermediate y seeds quedan en sus esquemas, y los marts (sin esquema
    propio) en `main`, que es lo que ve quien abre warehouse.duckdb (ADR 0026).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
