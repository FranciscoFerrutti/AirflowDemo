# Changelog — Demo (Pipeline de reportes de ventas)

Registro de cambios de la demostración práctica.
Formato: las entradas más recientes se agregan AL FINAL.
Lectura recomendada: `tail -n 30 CHANGELOG_DEMO.md` (o `Get-Content CHANGELOG_DEMO.md -Tail 30` en PowerShell).

---

## [0.0.0] — 2026-06-05 — Pendiente
### Status
- Demo AÚN NO desarrollada.
- Según el CLAUDE.md, la preparación detallada de la demo se solicita DESPUÉS de revisar la presentación.

### Planned (caso sugerido)
- Pipeline diario de generación de reportes de ventas:
  1. Leer CSV de ventas.
  2. Validar datos.
  3. Calcular métricas básicas.
  4. Generar reporte resumido.
  5. Guardar resultado.
  6. Enviar notificación simulada.
- Debe mostrar: definición del DAG, dependencias, ejecución manual, visualización en UI, estado de cada task y manejo de errores/retries.
- Restricción: ejecutable en menos de 5 minutos.

---

## [0.1.0] — 2026-06-08 — Demo implementada y verificada

### Added — Entorno (`demo/`)
- `docker-compose.yaml`: stack reproducible Postgres (Metadata DB) + Airflow
  2.10.4 con `LocalExecutor`. UI en http://localhost:8080 (admin/admin).
  Mapea 1:1 al diagrama de arquitectura de la diapositiva 4.
- `warehouse-db`: Postgres dedicado como Data Warehouse destino (separado de la
  Metadata DB a propósito). Connection `warehouse` inyectada por env var.
- `README.md`: guion de demo en vivo, troubleshooting y estructura.

### Added — DAG simple `reporte_ventas_diario`
- Pipeline lineal de 6 pasos (leer → validar → calcular → generar → guardar →
  notificar), TaskFlow API, sin dependencias externas (stdlib).
- `retries=2` con backoff; params `archivo` y `simular_fallo_transitorio`.
- Datasets: `ventas.csv` (válido) y `ventas_corrupto.csv` (para fail-fast).

### Added — DAG avanzado `reporte_ventas_avanzado`
- ETL multi-fuente que ejercita: PARALELISMO (fan-out/fan-in de 2 fuentes),
  BRANCH (`@task.branch`) + TRIGGER RULE (`NONE_FAILED_MIN_ONE_SUCCESS`),
  operadores heterogéneos (Empty/Python/SQLExecuteQuery/Bash) y CARGA a
  warehouse vía SQL. Datasets `ventas_online.csv` y `ventas_tienda.csv`.

### Verified (ejecución real sobre el scheduler)
- Simple — camino feliz: 6/6 success en ~5 s; reporte generado.
- Simple — CSV corrupto: `validar_datos` falla fast; downstream no corre.
- Simple — fallo transitorio: `validar_datos` UP_FOR_RETRY → SUCCESS (~33 s).
- Avanzado: 16 success + 1 skipped (rama no tomada); fila cargada en warehouse
  (segmento ALERTA); artefacto `.txt.gz` + `.sha256` generado por Bash.

### State
- Ambos DAGs quedan PAUSADOS; warehouse y `data/output/` limpios para la demo.

---

## [0.1.0] — 2026-06-05 — Demo implementada

### Added
- **DAG `reporte_ventas_diario`** (`dags/reporte_ventas_diario.py`): pipeline batch de 6 tasks con la TaskFlow API (`@dag`/`@task`):
  `leer_ventas → validar_datos → calcular_metricas → generar_reporte → guardar_reporte → notificar`.
  - Scheduling `@daily` con `catchup=False`; arranca pausado (se activa desde la UI).
  - Política de reintentos declarativa en `default_args` (`retries=2`, `retry_delay=15s`, backoff exponencial), compartida por todas las tasks.
  - Paso de datos entre tasks vía XCom (con la advertencia de no hacerlo con datasets grandes).
  - Parámetros configurables en "Trigger DAG w/ config": `archivo` y `simular_fallo_transitorio`.
- **Entorno reproducible** (`docker-compose.yaml` + `.env`): stack Postgres (Metadata DB) + `airflow-init` + scheduler + webserver, con `LocalExecutor`. Mapea al diagrama de la diapositiva 4.
- **Datasets de prueba**: `data/input/ventas.csv` (camino feliz) y `data/input/ventas_corrupto.csv` (demo de validación / fail-fast).
- **`README.md`** completo: requisitos, puesta en marcha, descripción del DAG, guion de la demo en vivo (~5 min, 3 actos), alternativa por CLI, estructura del proyecto y troubleshooting.
- **`.gitignore`** para artefactos de runtime (`logs/`, `data/output/`, `__pycache__/`).

### Demostrado en runtime
- Ejecuciones registradas en `logs/` que verifican los tres caminos de la demo:
  - Camino feliz (las 6 tasks en success).
  - Recuperación automática ante fallo transitorio (`validar_datos` en `up_for_retry` y éxito en el reintento).
  - Fail-fast con `ventas_corrupto.csv` (validación falla, downstream no se ejecuta).

### Notes
- El cómputo es trivial a propósito: el foco es la orquestación (dependencias, scheduling, reintentos, observabilidad), no el procesamiento.
- Artefactos de runtime (`logs/`, `data/output/`) no se versionan; se regeneran al ejecutar.
- Recomendación: levantar el stack ANTES de la presentación; el presupuesto de 5 min es solo para ejecutar el DAG, no para el arranque.
