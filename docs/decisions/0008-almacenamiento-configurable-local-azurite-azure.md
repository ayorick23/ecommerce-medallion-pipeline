# 0008 — Almacenamiento configurable: local por defecto, Azure Blob (o Azurite) como variante

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

En la industria, las capas de un lakehouse viven en almacenamiento de
objetos en la nube (Amazon S3, Azure Blob Storage / ADLS Gen2, Google Cloud
Storage), no en un disco local. Replicar eso con una cuenta de Azure es
viable y barato (Olist pesa ~120 MB), pero choca con el criterio de "hecho"
de la Fase 7: que otra persona clone el repo y lo levante **sin ayuda** —
lo que no ocurre si el pipeline exige credenciales de una cuenta en la
nube.

## Decisión

- Todo el código del pipeline construye sus rutas a partir de una **raíz
  de almacenamiento configurable** (YAML/`.env`, principio rector #5),
  nunca de rutas fijas. Ejemplo de la idea (formato exacto se fija en la
  Fase 2):

  ```yaml
  storage_root: ./data                                        # por defecto
  # storage_root: abfs://lake@<cuenta>.dfs.core.windows.net    # Azure
  ```

- **Por defecto: sistema de archivos local** (`data/`). Cualquiera lo
  corre sin cuentas ni credenciales.
- **Variante opcional: Azurite**, el emulador oficial de Azure Storage de
  Microsoft, como servicio adicional (opcional) del `docker-compose`, para
  ejercitar la API de Blob sin cuenta ni costo.
- **Variante opcional: Azure Blob Storage real**, para demostrar en el
  README que el mismo pipeline corre en la nube cambiando solo la
  configuración.
- Las credenciales de Azure, si se usan, van en `.env` (nunca en git).

## Alternativas consideradas

- **Solo local, rutas fijas**: lo más simple, pero acopla el código al
  disco y rompe el principio de configuración externalizada.
- **Azure obligatorio**: más fiel a producción, pero el proyecto deja de
  ser reproducible para un evaluador sin cuenta de Azure.
- **MinIO** (emulador compatible con S3): igual de válido técnicamente;
  se prefiere Azurite por coherencia con el ecosistema Azure explorado en
  la ADR 0009.

## Por qué

Abstraer la raíz de almacenamiento cuesta casi nada desde el inicio y es
práctica estándar: el código no sabe (ni le importa) si escribe en disco o
en la nube. Mantiene la reproducibilidad local como camino principal y deja
la nube como demostración, no como requisito.

## Consecuencias

- Polars y DuckDB deben recibir las rutas como URI y, para la nube, las
  opciones de autenticación correspondientes (`storage_options` en Polars;
  extensión `azure` en DuckDB y su configuración en `dbt-duckdb`).
- Azurite emula la API de Blob pero **no replica todo ADLS Gen2** (p. ej.
  el namespace jerárquico / endpoint `dfs`): si se incorpora, hay que
  validar qué operaciones del pipeline soporta antes de darlo por bueno.
- La incorporación de Azurite/Azure real no bloquea ninguna fase: se
  evalúa en la Fase 7 (reproducibilidad) o la Fase 8 (narrativa).
