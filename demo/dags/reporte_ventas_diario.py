"""
DAG de demostración — Pipeline diario de reportes de ventas
===========================================================

Ingeniería de Software II — Apache Airflow

Este DAG materializa el caso de la presentación (diapositivas 5, 9 y 13):
un pipeline batch que coordina **tareas interdependientes** con
**scheduling**, **reintentos** y **observabilidad**, sin que ninguna tarea
haga cómputo pesado (Airflow ORQUESTA, no computa).

Flujo (6 pasos):

    leer_ventas
        >> validar_datos
        >> calcular_metricas
        >> generar_reporte
        >> guardar_reporte
        >> notificar

Conceptos que ilustra:
  * DAG ............. el grafo completo, dirigido y acíclico.
  * Task ............ cada @task es un nodo del grafo.
  * Operator ........ usamos el PythonOperator vía la TaskFlow API (@task).
  * Dependency ...... el operador `>>` define el orden de ejecución.
  * Scheduling ...... `schedule="@daily"` (pero arranca pausado).
  * Retry ........... `retries=2` con backoff ante fallos transitorios.
  * Trigger ......... disparo manual desde la UI / API.

Notas de diseño:
  * Sin dependencias externas (solo stdlib): el cómputo es liviano a
    propósito. En un caso real, calcular_metricas delegaría en Spark/dbt/
    el warehouse; aquí lo hacemos inline solo para que la demo sea
    autocontenida.
  * Los datos viajan entre tasks por **XCom** (el valor que retorna cada
    @task). Para datasets grandes esto NO se hace: se pasan rutas/punteros.
"""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

# --- Rutas (dentro del contenedor) -----------------------------------------
# ./data se monta en /opt/airflow/data (ver docker-compose.yaml).
DATA_DIR = Path("/opt/airflow/data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# --- default_args: política de reintentos compartida por todas las tasks ----
# Esto resuelve, de forma DECLARATIVA, el problema de la diapositiva 2:
# "un timeout de red obligaba a reejecutar todo el día a mano".
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="reporte_ventas_diario",
    description="Pipeline diario de reportes de ventas (demo Ingeniería de Software II)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,  # no rellenar ejecuciones históricas al activar el DAG
    default_args=default_args,
    tags=["demo", "ventas", "etl", "ingesoft2"],
    params={
        # Nombre del archivo CSV de entrada. Cambiarlo en "Trigger DAG w/ config"
        # permite, en vivo, apuntar al CSV "roto" y ver fallar la validación.
        "archivo": "ventas.csv",
        # Si es True, validar_datos simula un fallo TRANSITORIO en el primer
        # intento y se recupera en el reintento -> muestra retries en la UI.
        "simular_fallo_transitorio": False,
    },
    doc_md=__doc__,
)
def reporte_ventas_diario():

    # ----------------------------------------------------------------------
    # 1) LEER — extraer las filas crudas del CSV de ventas
    # ----------------------------------------------------------------------
    @task
    def leer_ventas(params: dict) -> list[dict]:
        archivo = INPUT_DIR / params["archivo"]
        log.info("Leyendo archivo de ventas: %s", archivo)
        if not archivo.exists():
            raise AirflowException(f"No se encontró el archivo de entrada: {archivo}")

        with archivo.open(newline="", encoding="utf-8") as fh:
            filas = list(csv.DictReader(fh))

        log.info("Filas leídas: %d", len(filas))
        if not filas:
            raise AirflowException("El archivo de ventas está vacío.")
        return filas

    # ----------------------------------------------------------------------
    # 2) VALIDAR — chequeos de calidad de datos (fail-fast ante datos malos)
    # ----------------------------------------------------------------------
    @task
    def validar_datos(filas: list[dict], params: dict) -> list[dict]:
        from airflow.operators.python import get_current_context

        # --- Simulación opcional de fallo transitorio (para demo de retries) -
        if params.get("simular_fallo_transitorio"):
            ti = get_current_context()["ti"]
            if ti.try_number == 1:
                raise AirflowException(
                    "Fallo transitorio simulado (timeout de red). "
                    "Airflow reintentará automáticamente."
                )
            log.info("Reintento OK: el fallo transitorio se recuperó solo.")

        columnas_requeridas = {"fecha", "producto", "categoria", "cantidad", "precio_unitario"}
        errores: list[str] = []
        filas_validas: list[dict] = []

        for i, fila in enumerate(filas, start=1):
            faltantes = columnas_requeridas - fila.keys()
            if faltantes:
                errores.append(f"Fila {i}: faltan columnas {sorted(faltantes)}")
                continue
            try:
                cantidad = int(fila["cantidad"])
                precio = float(fila["precio_unitario"])
            except (TypeError, ValueError):
                errores.append(f"Fila {i}: cantidad/precio no numéricos -> {fila}")
                continue
            if cantidad <= 0 or precio < 0:
                errores.append(f"Fila {i}: valores fuera de rango (cantidad={cantidad}, precio={precio})")
                continue
            filas_validas.append(fila)

        if errores:
            # Datos estructuralmente inválidos: reintentar NO ayuda -> fail-fast.
            for e in errores:
                log.error(e)
            raise AirflowException(
                f"Validación fallida: {len(errores)} fila(s) inválida(s) de {len(filas)}."
            )

        log.info("Validación OK: %d filas válidas.", len(filas_validas))
        return filas_validas

    # ----------------------------------------------------------------------
    # 3) CALCULAR — métricas de negocio básicas
    # ----------------------------------------------------------------------
    @task
    def calcular_metricas(filas: list[dict]) -> dict:
        ingresos_total = 0.0
        unidades_total = 0
        ingresos_por_categoria: dict[str, float] = defaultdict(float)
        ingresos_por_producto: dict[str, float] = defaultdict(float)

        for fila in filas:
            cantidad = int(fila["cantidad"])
            precio = float(fila["precio_unitario"])
            monto = cantidad * precio
            ingresos_total += monto
            unidades_total += cantidad
            ingresos_por_categoria[fila["categoria"]] += monto
            ingresos_por_producto[fila["producto"]] += monto

        ticket_promedio = ingresos_total / len(filas) if filas else 0.0
        top_producto = max(ingresos_por_producto.items(), key=lambda kv: kv[1])

        metricas = {
            "operaciones": len(filas),
            "unidades_total": unidades_total,
            "ingresos_total": round(ingresos_total, 2),
            "ticket_promedio": round(ticket_promedio, 2),
            "ingresos_por_categoria": {k: round(v, 2) for k, v in ingresos_por_categoria.items()},
            "top_producto": {"producto": top_producto[0], "ingresos": round(top_producto[1], 2)},
        }
        log.info("Métricas calculadas: %s", json.dumps(metricas, ensure_ascii=False))
        return metricas

    # ----------------------------------------------------------------------
    # 4) GENERAR — armar el reporte resumido (texto legible)
    # ----------------------------------------------------------------------
    @task
    def generar_reporte(metricas: dict, params: dict) -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        fecha_logica = ctx["ds"]  # fecha lógica del run (YYYY-MM-DD)

        categorias = "\n".join(
            f"    - {cat:<15} ${monto:>12,.2f}"
            for cat, monto in sorted(
                metricas["ingresos_por_categoria"].items(),
                key=lambda kv: kv[1],
                reverse=True,
            )
        )

        reporte = f"""\
================================================================
  REPORTE DIARIO DE VENTAS
  Fecha lógica del run : {fecha_logica}
  Archivo procesado    : {params['archivo']}
  Generado             : {datetime.now():%Y-%m-%d %H:%M:%S}
================================================================

  Operaciones procesadas : {metricas['operaciones']:>10}
  Unidades vendidas      : {metricas['unidades_total']:>10}
  Ingresos totales       : ${metricas['ingresos_total']:>14,.2f}
  Ticket promedio        : ${metricas['ticket_promedio']:>14,.2f}

  Ingresos por categoría:
{categorias}

  Producto top : {metricas['top_producto']['producto']} \
(${metricas['top_producto']['ingresos']:,.2f})
================================================================
"""
        log.info("Reporte generado:\n%s", reporte)
        return reporte

    # ----------------------------------------------------------------------
    # 5) GUARDAR — persistir el reporte (idempotente por fecha lógica)
    # ----------------------------------------------------------------------
    @task
    def guardar_reporte(reporte: str) -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        fecha_logica = ctx["ds"]
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        destino = OUTPUT_DIR / f"reporte_ventas_{fecha_logica}.txt"
        destino.write_text(reporte, encoding="utf-8")
        log.info("Reporte guardado en: %s", destino)
        return str(destino)

    # ----------------------------------------------------------------------
    # 6) NOTIFICAR — notificación SIMULADA (en real: email/Slack operator)
    # ----------------------------------------------------------------------
    @task
    def notificar(ruta_reporte: str) -> None:
        log.info("=" * 60)
        log.info("📧  NOTIFICACIÓN SIMULADA")
        log.info("    Para  : finanzas@empresa.com")
        log.info("    Asunto: Reporte diario de ventas disponible")
        log.info("    Cuerpo: El reporte se generó correctamente.")
        log.info("    Adjunto: %s", ruta_reporte)
        log.info("=" * 60)

    # ----------------------------------------------------------------------
    # Dependencias del DAG (las aristas del grafo).
    # La TaskFlow API infiere el orden a partir del paso de datos:
    #   la salida de una task es la entrada de la siguiente.
    # ----------------------------------------------------------------------
    filas = leer_ventas()
    filas_ok = validar_datos(filas)
    metricas = calcular_metricas(filas_ok)
    reporte = generar_reporte(metricas)
    ruta = guardar_reporte(reporte)
    notificar(ruta)


reporte_ventas_diario()
