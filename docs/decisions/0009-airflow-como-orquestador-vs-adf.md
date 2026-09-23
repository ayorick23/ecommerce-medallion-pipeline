# 0009 — Apache Airflow como orquestador (Azure Data Factory como alternativa documentada)

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

Airflow estaba en el plan desde el inicio, pero no como decisión
registrada. Al revisar el stack surgió la comparación con **Azure Data
Factory (ADF)**, el servicio gestionado de integración y orquestación de
Azure, habitual en empresas "full Azure" (a menudo junto a Databricks, y
cada vez más dentro de Microsoft Fabric).

Ambos cumplen el mismo rol — orquestación — con filosofías distintas:

| | Airflow | Azure Data Factory |
| --- | --- | --- |
| Tipo | Open-source, *code-first* (DAGs en Python) | Servicio gestionado, *low-code* (GUI; artefactos en JSON) |
| Dónde corre | Cualquier lado: Docker, Kubernetes, o gestionado (AWS MWAA, GCP Composer, Astronomer) | Solo Azure |
| Alcance | Solo orquesta; el cómputo se delega | Orquesta y además mueve datos (Copy Activity) y transforma (Data Flows, Spark por detrás) |
| Costo | Gratis (se paga la infraestructura) | Pago por ejecución de actividades y cómputo |

Equivalencias de conceptos: *pipeline* ≈ DAG · *activity* ≈ task ·
*trigger* ≈ schedule · *linked service* ≈ connection.

## Decisión

El pipeline se orquesta con **Apache Airflow** (LocalExecutor + Postgres
como metastore, en Docker — ya validado en la Fase 0). ADF queda
documentado como alternativa en este ADR y en el README.

## Alternativas consideradas

- **Azure Data Factory**: requiere suscripción de pago, no se puede correr
  localmente ni reproducir desde el repo, y su definición (GUI/JSON) es
  difícil de revisar en un PR.
- **Dagster / Prefect**: orquestadores modernos y válidos, pero con menos
  presencia en ofertas de empleo que Airflow.

## Por qué

Airflow es el orquestador open-source más extendido de la industria, su
código es revisable en GitHub, corre gratis en Docker y lo que se aprende
se traslada a cualquier empresa (incluidas las que usan Airflow gestionado
en la nube). Encaja con la ADR 0002: Airflow solo orquesta, la lógica vive
en `src/` y en el proyecto dbt (ADR 0007).

## Consecuencias

- Fase 5 envuelve las funciones de Bronze/Silver y el `dbt build` de Gold
  como tasks de Airflow.
- El README (Fase 8) incluye la tabla de equivalencias Airflow ↔ ADF, para
  mostrar que el rol se entiende más allá de la herramienta.
