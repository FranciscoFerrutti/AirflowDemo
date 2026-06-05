# Apache Airflow — Orquestación de pipelines de datos como decisión arquitectónica

Material técnico para una charla de ~15 minutos (10 min de teoría + 5 min de demo)
sobre **Apache Airflow**, dirigida a una audiencia profesional y académica con
base en arquitectura de software, bases de datos y sistemas distribuidos.

El eje no es descriptivo ("¿qué es Airflow?") sino arquitectónico: **qué driver
de negocio justifica adoptar un orquestador, qué atributos de calidad se compran
y qué se paga a cambio**. Las tecnologías se analizan desde la perspectiva de las
decisiones de arquitectura y sus trade-offs.

---

## Contenido del repositorio

| Archivo / carpeta            | Descripción                                                        |
|------------------------------|-------------------------------------------------------------------|
| `presentacion.md`            | Presentación completa en Markdown (10–14 diapositivas).            |
| `presentacion.pdf`           | Versión exportada de la presentación para proyección.             |
| `demo/`                      | Demostración práctica ejecutable: pipeline de Airflow sobre Docker.|
| `demo/README.md`             | Guía de puesta en marcha y guion de la demo en vivo.              |
| `CHANGELOG_PRESENTACION.md`  | Historial de cambios de la presentación.                          |
| `CHANGELOG_DEMO.md`          | Historial de cambios de la demo.                                  |

---

## La presentación

Estructura orientada a la toma de decisiones, no al inventario de features:

- **Problema de negocio:** los riesgos de gobernar pipelines con scripts aislados
  y `cron` (dependencias implícitas, fallos silenciosos, falta de trazabilidad).
- **Decisión arquitectónica:** por qué una plataforma de orquestación, qué
  alternativas existen (Cron, Apache NiFi, Prefect, Dagster) y sus trade-offs.
- **Atributos de calidad (ADD):** impacto sobre Fault Tolerance, Interoperability,
  Manageability, Scalability, Auditability y Portability, con sus compromisos.
- **Trade-offs:** qué resuelve Airflow, qué **no** resuelve, costos operativos y
  escenarios donde su adopción **no** está justificada.
- **Arquitectura y conceptos:** Scheduler, DAGs, Tasks, Executors, Workers,
  Metadata Database y Web UI; DAG, Operator, Dependency, Trigger, Retry, Scheduling.

Cada diapositiva incluye título, subtítulo, tiempo estimado y notas del presentador.

---

## La demo

Pipeline batch **`reporte_ventas_diario`**: lee ventas desde un CSV, valida los
datos, calcula métricas, genera y guarda un reporte, y envía una notificación
simulada. El cómputo es trivial a propósito; lo que se exhibe son las capacidades
que justifican un orquestador: **dependencias entre tareas, scheduling,
reintentos, recuperación ante fallos y observabilidad**.

### Requisitos

- **Docker Desktop** en ejecución (incluye `docker compose`).
- Puerto **8080** libre y ~2 GB de RAM para los contenedores.
- No se requiere instalar Python ni Airflow en el host.

### Puesta en marcha

```bash
cd demo
docker compose up -d
```

Abrir la UI en **http://localhost:8080** (usuario `admin` / contraseña `admin`).

El guion completo de la demo en vivo, los parámetros del DAG y el troubleshooting
están en [`demo/README.md`](demo/README.md).

---

## Referencia

Sitio oficial del proyecto: <https://airflow.apache.org/>
