# Demo — Pipeline diario de reportes de ventas (Apache Airflow)

Demostración práctica para la presentación de **Apache Airflow** (Ingeniería de
Software II). Implementa el caso sugerido en el enunciado: un pipeline batch que
lee ventas desde un CSV, valida, calcula métricas, genera y guarda un reporte, y
envía una notificación simulada.

El objetivo **no** es el cómputo (es trivial a propósito), sino mostrar las
capacidades que justifican adoptar un orquestador: **dependencias entre tareas,
scheduling, reintentos, recuperación ante fallos y observabilidad** (ver
diapositivas 2, 5, 7 y 9 de `presentacion.md`).

---

## 1. Requisitos

- **Docker Desktop** en ejecución (incluye `docker compose`).
- Puerto **8080** libre (Web UI de Airflow).
- ~2 GB de RAM disponibles para los contenedores.

No se necesita instalar Python ni Airflow en el host: todo corre en contenedores.

---

## 2. Arquitectura del entorno (mapea al diagrama de la diapositiva 4)

| Contenedor          | Rol en la arquitectura                                  |
|---------------------|---------------------------------------------------------|
| `postgres`          | **Metadata Database** — única fuente de verdad          |
| `airflow-init`      | One-shot: migra el schema y crea el usuario `admin`     |
| `airflow-scheduler` | **Scheduler** — parsea DAGs, resuelve dependencias      |
| `airflow-webserver` | **Web UI / API** — visualización y disparo manual       |
| `warehouse-db`      | Postgres **dedicado** como Data Warehouse destino (DAG avanzado) |

**Executor:** `LocalExecutor` (las tasks corren en procesos del scheduler).
Punto clave de la charla: pasar a `Celery`/`KubernetesExecutor` para escalar
**no requiere tocar el DAG** (separación planificación/ejecución).

---

## 3. Puesta en marcha

Desde la carpeta `demo/`:

```bash
docker compose up -d
```

La primera vez descarga las imágenes (~5 min según la red). Esperar a que el
webserver quede *healthy*:

```bash
docker compose ps
```

Abrir la UI: **http://localhost:8080**  ·  usuario **`admin`** / contraseña **`admin`**

> Recomendación: levantar el stack **antes** de la presentación. El presupuesto
> de 5 minutos es solo para ejecutar el DAG, no para el arranque.

Para apagar (conservando datos):

```bash
docker compose down
```

Para apagar y borrar todo (incluida la Metadata DB):

```bash
docker compose down -v
```

---

## 4. El DAG: `reporte_ventas_diario`

Definido en `dags/reporte_ventas_diario.py`. Flujo lineal de 6 pasos:

```
leer_ventas → validar_datos → calcular_metricas → generar_reporte → guardar_reporte → notificar
```

- **Scheduling:** `@daily` (arranca **pausado**; se activa desde la UI).
- **Reintentos:** `retries=2`, `retry_delay=15s` con backoff exponencial
  (definidos en `default_args`, compartidos por todas las tasks).
- **Entrada:** `data/input/ventas.csv`.
- **Salida:** `data/output/reporte_ventas_<fecha>.txt`.
- **Parámetros** (configurables al disparar "Trigger DAG w/ config"):
  - `archivo`: CSV de entrada (default `ventas.csv`).
  - `simular_fallo_transitorio`: si es `true`, `validar_datos` falla en el
    primer intento y **se recupera en el reintento** (demo de fault tolerance).

---

## 5. Guion de la demo en vivo (~5 min)

### Acto 1 — El pipeline y sus dependencias (1 min)
1. En la UI, mostrar el DAG `reporte_ventas_diario` (vista **Graph**).
2. Señalar el grafo: las 6 tasks y sus **dependencias** (las aristas).
3. Abrir `dags/reporte_ventas_diario.py`: el pipeline **es código** (versionable,
   revisable). Mostrar `default_args` (retries) y el encadenamiento de tasks.

### Acto 2 — Ejecución manual y observabilidad (1.5 min)
4. **Activar** el DAG (toggle) y dispararlo: botón ▶ → *Trigger DAG*.
5. Ver en **Grid/Graph** cómo cada task pasa a verde (success) en orden.
6. Abrir una task → **Logs**: mostrar la trazabilidad (qué corrió y su salida).
7. Mostrar el reporte generado en `data/output/`.

### Acto 3 — Manejo de errores y reintentos (1.5 min)
Elegir **una** de las dos variantes (o ambas si sobra tiempo):

- **Variante A — Recuperación automática (recomendada):**
  *Trigger DAG **w/ config*** con:
  ```json
  { "simular_fallo_transitorio": true }
  ```
  `validar_datos` falla (queda **up_for_retry**, en amarillo) y, tras ~15s, el
  reintento **tiene éxito**: el pipeline continúa hasta el final. Mensaje:
  *los fallos transitorios se resuelven solos, sin intervención humana*.

- **Variante B — Fail-fast ante datos inválidos:**
  *Trigger DAG **w/ config*** con:
  ```json
  { "archivo": "ventas_corrupto.csv" }
  ```
  `validar_datos` detecta filas inválidas, agota los reintentos y la task queda
  **failed** (roja); las tasks downstream no se ejecutan. En **Logs** se ve el
  detalle de cada fila rechazada. Mensaje: *reintentar no arregla datos malos →
  fallar rápido y con trazabilidad*.

### Cierre (15 s)
Volver al mensaje de la charla: Airflow no hizo el cómputo, lo **orquestó**, y a
cambio obtuvimos dependencias, reintentos, estado persistido y auditoría — los
atributos de calidad que justifican la decisión arquitectónica.

---

## 5b. DAG avanzado: `reporte_ventas_avanzado`

Versión "realista" del pipeline, pensada para mostrar **más capacidades en
vivo** cuando sobra tiempo (o para reemplazar al simple si se prioriza impacto).
Definido en `dags/reporte_ventas_avanzado.py`.

```
inicio ─┬─► extraer_online ─► validar_online ─┐
        │                                      ├─► consolidar ─► calcular_metricas ─► decidir ─┬─► ruta_alerta ─┐
        ├─► extraer_tienda ─► validar_tienda ──┘                                               └─► ruta_normal ─┤
        │                                                                                                        ▼
        └─► preparar_warehouse ──────────────────────────────────────────────────────────────► cargar_warehouse(SQL)
              fin ◄─ notificar ◄─ comprimir_reporte(Bash) ◄─ guardar_reporte ◄─ generar_reporte ◄─ verificar_carga(SQL)
```

| Concepto que demuestra | Cómo | Atributo de calidad |
|---|---|---|
| **Paralelismo** (fan-out/fan-in) | 2 fuentes (online/tienda) extraídas y validadas en ramas concurrentes, luego `consolidar` | Scalability |
| **Branch + trigger rules** | `@task.branch decidir` elige `ruta_alerta`/`ruta_normal`; el join usa `NONE_FAILED_MIN_ONE_SUCCESS` para seguir aunque una rama quede *skipped* | Fault Tolerance |
| **Operadores heterogéneos** | `EmptyOperator`, `@task` (Python), `SQLExecuteQueryOperator`, `BashOperator` en un mismo grafo | Interoperability |
| **Carga a warehouse** | `SQLExecuteQueryOperator` inserta el resumen en un Postgres dedicado (la "L" de ETL/ELT) | Interoperability |

**Parámetro:** `umbral_alerta` (default `15000000`). Si los ingresos del día lo
superan, se toma `ruta_alerta` y el segmento se marca `ALERTA`.

### Guion (variante avanzada, ~3 min)
1. Vista **Graph**: señalar las **ramas paralelas** (online/tienda corren a la
   vez) y la **bifurcación** `decidir` con una rama en gris (*skipped*).
2. Disparar ▶. Ver el fan-out ejecutándose en paralelo y el join continuar pese
   a la rama skipped (trigger rule).
3. Abrir **Logs** de `cargar_warehouse` y `verificar_carga`: el `SELECT` muestra
   la fila cargada en el warehouse.
4. Mostrar el artefacto comprimido (`.txt.gz` + `.sha256`) que dejó el
   `BashOperator` en `data/output/`.

Consultar el warehouse directamente (opcional):
```bash
docker compose exec warehouse-db psql -U wh -d warehouse -c "SELECT * FROM ventas_resumen;"
```

---

## 6. Alternativa por línea de comandos (sin UI)

Útil si falla el proyector o para ensayar:

```bash
# Activar y disparar
docker compose exec airflow-scheduler airflow dags unpause reporte_ventas_diario
docker compose exec airflow-scheduler airflow dags trigger reporte_ventas_diario

# Ver el estado de las tasks del último run
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run reporte_ventas_diario <run_id>

# Reproducir el fallo por datos inválidos (in-process, sin scheduler)
docker compose exec airflow-scheduler \
  airflow dags test reporte_ventas_diario -c '{"archivo":"ventas_corrupto.csv"}'
```

---

## 7. Estructura del proyecto

```
demo/
├── docker-compose.yaml          # Stack: Postgres + Airflow (LocalExecutor)
├── .env                         # AIRFLOW_UID
├── dags/
│   ├── reporte_ventas_diario.py   # DAG simple (6 tasks lineales + retries)
│   └── reporte_ventas_avanzado.py # DAG avanzado (paralelismo+branch+SQL+Bash)
├── data/
│   ├── input/
│   │   ├── ventas.csv           # Dataset válido (camino feliz, DAG simple)
│   │   ├── ventas_corrupto.csv  # Dataset inválido (demo de validación)
│   │   ├── ventas_online.csv    # Fuente 1 (DAG avanzado)
│   │   └── ventas_tienda.csv    # Fuente 2 (DAG avanzado)
│   └── output/                  # Reportes generados (runtime)
├── logs/                        # Logs de Airflow (runtime)
└── plugins/                     # (vacío)
```

---

## 8. Troubleshooting

- **El DAG no aparece:** esperar ~10s (intervalo de parseo) o revisar errores de
  import: `docker compose exec airflow-scheduler airflow dags list-import-errors`.
- **Puerto 8080 ocupado:** cambiar el mapeo a `"8081:8080"` en el compose.
- **Permisos en `logs/` (Linux/Mac):** poner el UID real en `.env`
  (`AIRFLOW_UID=$(id -u)`) y reiniciar.
- **Reset total:** `docker compose down -v` y volver a `up -d`.
