"""
DAG de demostración (avanzado) — ETL de ventas multi-fuente
===========================================================

Ingeniería de Software II — Apache Airflow

Versión "realista" del pipeline de ventas. A diferencia de
`reporte_ventas_diario` (lineal y minimalista), este DAG ejercita las
capacidades que justifican adoptar un orquestador y que serían costosas de
reimplementar a mano en `cron` (ver diapositivas 4, 5 y 7):

  * PARALELISMO (fan-out / fan-in): dos fuentes (online y tienda) se extraen
    y validan en RAMAS CONCURRENTES y luego se consolidan.   -> Scalability
  * RAMIFICACIÓN (branch) + TRIGGER RULES: según el volumen de ingresos se
    toma una ruta de "alerta" o "normal"; el join continúa aunque una rama
    quede skipped.                                           -> Fault Tolerance
  * OPERADORES HETEROGÉNEOS: TaskFlow (@task), SQLExecuteQueryOperator,
    BashOperator y EmptyOperator conviven en el mismo grafo. -> Interoperability
  * CARGA A UN WAREHOUSE: la "L" de ETL/ELT, contra un Postgres dedicado
    (NO la Metadata DB de Airflow).                          -> Interoperability

Grafo:

    inicio ─┬─► extraer_online ─► validar_online ─┐
            │                                      ├─► consolidar ─► calcular_metricas ─► decidir ─┬─► ruta_alerta ─┐
            ├─► extraer_tienda ─► validar_tienda ──┘                                               └─► ruta_normal ─┤
            │                                                                                                        ▼
            └─► preparar_warehouse ───────────────────────────────────────────────────────────► cargar_warehouse(SQL)
                                                                                                         │
                  fin ◄─ notificar ◄─ comprimir_reporte(Bash) ◄─ guardar_reporte ◄─ generar_reporte ◄─ verificar_carga(SQL)

Nota de altitud: el cómputo sigue siendo trivial a propósito. Airflow
ORQUESTA (coordina fuentes, ramas, carga y reporte); el cómputo pesado, en un
caso real, se delegaría a Spark/dbt/el warehouse.
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# Umbral (en $) a partir del cual el día se clasifica como de ventas ALTAS y
# se dispara la ruta de alerta. Editable como Param al disparar el DAG.
UMBRAL_ALERTA_DEFAULT = 15_000_000.0

COLUMNAS_REQUERIDAS = {"fecha", "producto", "categoria", "cantidad", "precio_unitario"}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
}


# --- Helpers reutilizables (no son tasks) -----------------------------------
def _leer_csv(nombre: str) -> list[dict]:
    archivo = INPUT_DIR / nombre
    if not archivo.exists():
        raise AirflowException(f"No se encontró el archivo de entrada: {archivo}")
    with archivo.open(newline="", encoding="utf-8") as fh:
        filas = list(csv.DictReader(fh))
    if not filas:
        raise AirflowException(f"El archivo {nombre} está vacío.")
    return filas


def _validar(filas: list[dict], fuente: str) -> list[dict]:
    errores, validas = [], []
    for i, fila in enumerate(filas, start=1):
        if COLUMNAS_REQUERIDAS - fila.keys():
            errores.append(f"[{fuente}] Fila {i}: faltan columnas requeridas")
            continue
        try:
            cantidad = int(fila["cantidad"])
            precio = float(fila["precio_unitario"])
        except (TypeError, ValueError):
            errores.append(f"[{fuente}] Fila {i}: cantidad/precio no numéricos")
            continue
        if cantidad <= 0 or precio < 0:
            errores.append(f"[{fuente}] Fila {i}: valores fuera de rango")
            continue
        validas.append(fila)
    if errores:
        for e in errores:
            log.error(e)
        raise AirflowException(f"[{fuente}] Validación fallida: {len(errores)} fila(s) inválida(s).")
    log.info("[%s] Validación OK: %d filas.", fuente, len(validas))
    return validas


@dag(
    dag_id="reporte_ventas_avanzado",
    description="ETL multi-fuente con paralelismo, branch y carga a warehouse (demo Ingeniería de Software II)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    default_args=default_args,
    tags=["demo", "ventas", "etl", "avanzado", "ingesoft2"],
    params={"umbral_alerta": UMBRAL_ALERTA_DEFAULT},
    doc_md=__doc__,
)
def reporte_ventas_avanzado():

    inicio = EmptyOperator(task_id="inicio")

    # --- DDL idempotente del warehouse (corre en paralelo con la extracción) -
    preparar_warehouse = SQLExecuteQueryOperator(
        task_id="preparar_warehouse",
        conn_id="warehouse",
        sql="""
            CREATE TABLE IF NOT EXISTS ventas_resumen (
                id              SERIAL PRIMARY KEY,
                fecha_run       DATE NOT NULL,
                operaciones     INT,
                unidades        INT,
                ingresos_total  NUMERIC(16,2),
                ticket_promedio NUMERIC(16,2),
                segmento        VARCHAR(16),
                cargado_en      TIMESTAMP DEFAULT now()
            );
        """,
    )

    # ----------------------- FAN-OUT: 2 fuentes en paralelo -----------------
    @task
    def extraer_online() -> list[dict]:
        return _leer_csv("ventas_online.csv")

    @task
    def extraer_tienda() -> list[dict]:
        return _leer_csv("ventas_tienda.csv")

    @task
    def validar_online(filas: list[dict]) -> list[dict]:
        return _validar(filas, "online")

    @task
    def validar_tienda(filas: list[dict]) -> list[dict]:
        return _validar(filas, "tienda")

    # ----------------------- FAN-IN: consolidación --------------------------
    @task
    def consolidar(online: list[dict], tienda: list[dict]) -> list[dict]:
        combinadas = online + tienda
        log.info("Consolidado: %d (online) + %d (tienda) = %d filas",
                 len(online), len(tienda), len(combinadas))
        return combinadas

    @task
    def calcular_metricas(filas: list[dict], params: dict) -> dict:
        ingresos = 0.0
        unidades = 0
        por_categoria: dict[str, float] = defaultdict(float)
        por_canal: dict[str, float] = defaultdict(float)
        for f in filas:
            monto = int(f["cantidad"]) * float(f["precio_unitario"])
            ingresos += monto
            unidades += int(f["cantidad"])
            por_categoria[f["categoria"]] += monto
            por_canal[f.get("canal", "n/d")] += monto

        umbral = float(params["umbral_alerta"])
        segmento = "ALERTA" if ingresos >= umbral else "NORMAL"
        metricas = {
            "operaciones": len(filas),
            "unidades_total": unidades,
            "ingresos_total": round(ingresos, 2),
            "ticket_promedio": round(ingresos / len(filas), 2) if filas else 0.0,
            "ingresos_por_categoria": {k: round(v, 2) for k, v in por_categoria.items()},
            "ingresos_por_canal": {k: round(v, 2) for k, v in por_canal.items()},
            "umbral_alerta": umbral,
            "segmento": segmento,
        }
        log.info("Segmento del día: %s (ingresos=%.2f, umbral=%.2f)", segmento, ingresos, umbral)
        return metricas

    # ----------------------- BRANCH: ruta según volumen ---------------------
    @task.branch
    def decidir(metricas: dict) -> str:
        # Devuelve el task_id de la rama a ejecutar; la otra queda 'skipped'.
        return "ruta_alerta" if metricas["segmento"] == "ALERTA" else "ruta_normal"

    @task
    def ruta_alerta(metricas: dict) -> None:
        log.warning("🚨 ALERTA: ingresos $%.2f superan el umbral $%.2f.",
                    metricas["ingresos_total"], metricas["umbral_alerta"])
        log.warning("    -> Notificación URGENTE simulada a dirección comercial.")

    @task
    def ruta_normal(metricas: dict) -> None:
        log.info("Ventas dentro de lo normal ($%.2f). Sin alertas.",
                 metricas["ingresos_total"])

    # ----------------------- LOAD: carga al warehouse -----------------------
    # trigger_rule: el join se ejecuta aunque una de las ramas haya quedado
    # 'skipped' (solo importa que ninguna upstream haya FALLADO).
    cargar_warehouse = SQLExecuteQueryOperator(
        task_id="cargar_warehouse",
        conn_id="warehouse",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        sql=[
            "DELETE FROM ventas_resumen WHERE fecha_run = '{{ ds }}';",
            """
            INSERT INTO ventas_resumen
                (fecha_run, operaciones, unidades, ingresos_total, ticket_promedio, segmento)
            VALUES
                ('{{ ds }}',
                 {{ ti.xcom_pull(task_ids='calcular_metricas')['operaciones'] }},
                 {{ ti.xcom_pull(task_ids='calcular_metricas')['unidades_total'] }},
                 {{ ti.xcom_pull(task_ids='calcular_metricas')['ingresos_total'] }},
                 {{ ti.xcom_pull(task_ids='calcular_metricas')['ticket_promedio'] }},
                 '{{ ti.xcom_pull(task_ids='calcular_metricas')['segmento'] }}');
            """,
        ],
    )

    verificar_carga = SQLExecuteQueryOperator(
        task_id="verificar_carga",
        conn_id="warehouse",
        sql="SELECT fecha_run, operaciones, ingresos_total, segmento "
            "FROM ventas_resumen WHERE fecha_run = '{{ ds }}';",
        show_return_value_in_logs=True,
    )

    # ----------------------- REPORTE + COMPRESIÓN + NOTIFICACIÓN ------------
    @task
    def generar_reporte() -> str:
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        m = ctx["ti"].xcom_pull(task_ids="calcular_metricas")
        ds = ctx["ds"]

        categorias = "\n".join(
            f"    - {cat:<15} ${monto:>14,.2f}"
            for cat, monto in sorted(m["ingresos_por_categoria"].items(),
                                     key=lambda kv: kv[1], reverse=True)
        )
        canales = "\n".join(
            f"    - {canal:<15} ${monto:>14,.2f}"
            for canal, monto in sorted(m["ingresos_por_canal"].items(),
                                       key=lambda kv: kv[1], reverse=True)
        )
        reporte = f"""\
================================================================
  REPORTE DIARIO DE VENTAS (multi-fuente)
  Fecha lógica : {ds}
  Generado     : {datetime.now():%Y-%m-%d %H:%M:%S}
  Segmento     : {m['segmento']}
================================================================

  Operaciones      : {m['operaciones']:>12}
  Unidades         : {m['unidades_total']:>12}
  Ingresos totales : ${m['ingresos_total']:>16,.2f}
  Ticket promedio  : ${m['ticket_promedio']:>16,.2f}

  Ingresos por canal:
{canales}

  Ingresos por categoría:
{categorias}
================================================================
"""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino = OUTPUT_DIR / f"reporte_avanzado_{ds}.txt"
        destino.write_text(reporte, encoding="utf-8")
        log.info("Reporte generado en %s:\n%s", destino, reporte)
        return str(destino)

    # BashOperator: comprime el reporte y calcula un checksum (operador no-Python).
    comprimir_reporte = BashOperator(
        task_id="comprimir_reporte",
        bash_command=(
            "set -euo pipefail\n"
            'REPORTE="{{ ti.xcom_pull(task_ids=\'generar_reporte\') }}"\n'
            'echo "Comprimiendo: $REPORTE"\n'
            'gzip -f -k "$REPORTE"\n'
            'sha256sum "${REPORTE}.gz" | tee "${REPORTE}.gz.sha256"\n'
            'echo "OK: artefacto comprimido y checksum generado."\n'
        ),
    )

    @task
    def notificar() -> None:
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        m = ctx["ti"].xcom_pull(task_ids="calcular_metricas")
        log.info("=" * 60)
        log.info("📧  NOTIFICACIÓN SIMULADA — Reporte de ventas disponible")
        log.info("    Segmento del día : %s", m["segmento"])
        log.info("    Ingresos totales : $%.2f", m["ingresos_total"])
        log.info("    Cargado en warehouse y reporte comprimido.")
        log.info("=" * 60)

    fin = EmptyOperator(task_id="fin")

    # ----------------------- DEPENDENCIAS (las aristas) ---------------------
    online = extraer_online()
    tienda = extraer_tienda()
    online_ok = validar_online(online)
    tienda_ok = validar_tienda(tienda)

    filas = consolidar(online_ok, tienda_ok)
    metricas = calcular_metricas(filas)
    rama = decidir(metricas)

    alerta = ruta_alerta(metricas)
    normal = ruta_normal(metricas)

    rep = generar_reporte()
    notif = notificar()

    # fan-out inicial
    inicio >> [online, tienda, preparar_warehouse]
    # branch
    rama >> [alerta, normal]
    # join + carga (espera ambas ramas y el DDL del warehouse)
    [alerta, normal, preparar_warehouse] >> cargar_warehouse
    # cola: verificación -> reporte -> compresión -> notificación -> fin
    cargar_warehouse >> verificar_carga >> rep >> comprimir_reporte >> notif >> fin


reporte_ventas_avanzado()
