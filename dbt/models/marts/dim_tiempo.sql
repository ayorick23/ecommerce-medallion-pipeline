-- Calendario fijo (ADR 0032): no depende de D ni de los datos. Los nombres en
-- español salen de listas explícitas, no del locale de la máquina.
with
    dias as (

        select cast(dia as date) as fecha
        from
            generate_series(
                cast('{{ var("calendario_inicio") }}' as timestamp),
                cast('{{ var("calendario_fin") }}' as timestamp),
                interval 1 day
            ) as t(dia)

    )

select
    d.fecha,
    year(d.fecha) as anio,
    quarter(d.fecha) as trimestre,
    month(d.fecha) as mes,
    [
        'enero',
        'febrero',
        'marzo',
        'abril',
        'mayo',
        'junio',
        'julio',
        'agosto',
        'septiembre',
        'octubre',
        'noviembre',
        'diciembre'
    ][month(d.fecha)] as nombre_mes,
    day(d.fecha) as dia,
    isodow(d.fecha) as dia_semana,
    ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'][
        isodow(d.fecha)
    ] as nombre_dia,
    weekofyear(d.fecha) as semana_iso,
    strftime(d.fecha, '%Y-%m') as anio_mes,
    isodow(d.fecha) in (6, 7) as es_fin_de_semana,
    f.fecha is not null as es_feriado,
    f.nombre_feriado
from dias as d
left join {{ ref('feriados_brasil') }} as f on d.fecha = f.fecha
