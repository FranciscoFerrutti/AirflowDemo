# Apache Airflow
## Orquestación de pipelines de datos como decisión arquitectónica

**Ingeniería de Software II — Comisión S**

> Duración objetivo: 10 min teoría + 5 min demo

---

## Diapositiva 1 — Portada
### Apache Airflow: orquestación como decisión de arquitectura

**Subtítulo:** No "¿qué es?", sino "¿por qué una organización decide adoptarlo y qué cuesta?"

⏱️ **Tiempo estimado: 0:20**

> **Notas del presentador:**
> Enmarcar la charla: no es una demo de una herramienta de moda, es el análisis de una decisión arquitectónica. La pregunta que guía toda la exposición es *qué driver de negocio justifica incorporar un orquestador y qué atributos de calidad estamos comprando*. Anticipar que cerramos con una demo de 5 minutos.

---

## Diapositiva 2 — El problema de negocio
### Cuando la empresa empieza a depender de sus pipelines de datos

**Subtítulo:** El riesgo no es técnico, es organizacional

- Toda empresa data-driven acumula procesos: ETL nocturnos, cargas a un data warehouse, reentrenamiento de modelos, reportes regulatorios.
- **Fase inicial:** scripts aislados + `cron` + tareas programadas manuales. Funciona... hasta que escala.
- **Síntomas de que el modelo se rompe:**
  - *Dependencias implícitas:* el reporte de las 6 AM asume que la carga de las 5 AM terminó. Nadie lo garantiza.
  - *Fallos silenciosos:* un script falla a las 3 AM y se descubre cuando un gerente abre un dashboard vacío.
  - *Sin reintentos ni recuperación:* un timeout de red obliga a reejecutar todo el día a mano.
  - *Conocimiento tribal:* el flujo "vive" en la cabeza de una persona y en un `crontab` sin versionar.
  - *Sin trazabilidad:* nadie sabe qué corrió, cuándo, con qué datos ni por qué falló.

⏱️ **Tiempo estimado: 1:15**

> **Notas del presentador:**
> Este es el corazón del *driver de negocio*. La necesidad real no es "ejecutar tareas" (eso lo hace `cron`), sino **gobernar dependencias, garantizar la confiabilidad de los datos y dar visibilidad/auditoría**. El costo del problema es de negocio: decisiones tomadas sobre datos incompletos, incumplimiento regulatorio, horas de ingeniería apagando incendios. Plantar aquí la tensión que Airflow vendrá a resolver.

---

## Diapositiva 3 — ¿Qué es Apache Airflow?
### Automatización vs. Orquestación

**Subtítulo:** Plataforma para crear, programar y monitorear flujos de trabajo *como código*

- **Definición:** plataforma open-source para definir flujos de trabajo programáticamente como **DAGs** (grafos dirigidos acíclicos) en Python, y orquestar su ejecución, scheduling y monitoreo.
- **Automatización** = ejecutar una tarea sin intervención humana (lo hace un script en `cron`).
- **Orquestación** = coordinar **muchas tareas interdependientes**: ordenarlas, gestionar dependencias, reintentos, paralelismo, recuperación ante fallos y observabilidad sobre el conjunto.
- **Workflows as Code:** el pipeline es código Python → versionable (Git), revisable (code review), testeable y reproducible.

**Historia (driver de origen):**
- 2014: nace en **Airbnb** (autor: Maxime Beauchemin) para gestionar la creciente complejidad de sus flujos internos de datos.
- 2016: ingresa a la incubadora de la **Apache Software Foundation**.
- 2019: se convierte en **proyecto Top-Level de Apache**. Hoy es estándar de facto en data engineering.

⏱️ **Tiempo estimado: 1:00**

> **Notas del presentador:**
> El concepto clave es la distinción automatización/orquestación: justifica por qué `cron` no alcanza. Subrayar "workflows as code" porque de ahí se derivan luego varios atributos de calidad (manageability, auditability). La historia importa como argumento: surgió de un problema real de escala en Airbnb, no de un ejercicio académico, y su gobernanza Apache reduce el riesgo de lock-in.

---

## Diapositiva 4 — Arquitectura de Airflow
### Componentes y separación de responsabilidades

**Subtítulo:** Una arquitectura distribuida orientada a desacoplar planificación de ejecución

```
        ┌──────────────────────────────────────────────────────┐
        │                       Web UI / API                    │
        │        (visualización, disparo manual, monitoreo)     │
        └───────────────────────┬──────────────────────────────┘
                                │
        ┌───────────────────────▼──────────────────────────────┐
        │                  METADATA DATABASE                    │
        │   (estado de DAGs, tasks, runs, conexiones, logs)     │
        │            ── única fuente de verdad ──               │
        └───────▲───────────────▲──────────────────▲───────────┘
                │               │                  │
      ┌─────────┴──────┐  ┌─────┴────────┐   ┌─────┴──────────┐
      │   SCHEDULER    │  │   EXECUTOR    │   │    WORKERS     │
      │ - parsea DAGs  │─▶│ (cómo/dónde   │──▶│ ejecutan las   │
      │ - resuelve     │  │  corren las   │   │ tasks (Local / │
      │   dependencias │  │  tasks)       │   │ Celery / K8s)  │
      │ - encola tasks │  └───────────────┘   └────────────────┘
      └────────────────┘
                ▲
      ┌─────────┴───────┐
      │   DAG FILES      │  (definiciones en Python, versionadas en Git)
      └──────────────────┘
```

- **Scheduler:** corazón del sistema. Parsea los DAGs, evalúa dependencias y scheduling, y decide qué task ejecutar y cuándo.
- **Executor:** define *cómo y dónde* se ejecutan las tasks (estrategia de ejecución). Es un punto de configuración clave.
- **Workers:** procesos que ejecutan efectivamente la lógica de cada task.
- **Metadata Database:** PostgreSQL/MySQL. **Única fuente de verdad**: estado de cada run, historial, conexiones, variables. Habilita recuperación y auditoría.
- **Web UI / API:** observabilidad, disparo manual, inspección de logs, re-ejecución selectiva.

⏱️ **Tiempo estimado: 1:30**

> **Notas del presentador:**
> El mensaje arquitectónico central: **separación entre planificación (Scheduler) y ejecución (Executor + Workers), con la Metadata DB como fuente de verdad**. Esa separación es lo que habilita escalabilidad (cambiar el Executor sin tocar los DAGs) y fault tolerance (si un worker muere, el estado persiste en la DB y la task se reintenta). Señalar que la Metadata DB es también el **single point of failure** que hay que proteger — adelanto de los trade-offs.

---

## Diapositiva 5 — Conceptos fundamentales
### El vocabulario mínimo de un DAG

**Subtítulo:** Pocas primitivas, alto poder expresivo

- **DAG:** el flujo completo. *Dirigido y acíclico* → las dependencias tienen sentido y no hay ciclos infinitos.
- **Task:** unidad de trabajo (un nodo del grafo).
- **Operator:** plantilla que define *qué hace* una task. `PythonOperator`, `BashOperator`, `SQLExecuteQueryOperator`, operadores para AWS/GCP, etc.
- **Dependency:** la arista. `validar >> transformar >> cargar` define el orden de ejecución.
- **Scheduling:** cuándo corre el DAG (cron expression o presets, p. ej. `@daily`).
- **Trigger:** qué inicia un run (programado, manual, vía API, o por sensores ante eventos externos).
- **Retry:** reintentos automáticos con backoff ante fallos transitorios (`retries=3`, `retry_delay`).

```python
validar_datos >> calcular_metricas >> generar_reporte >> notificar
```

⏱️ **Tiempo estimado: 1:00**

> **Notas del presentador:**
> No memorizar la API: entender que con DAG + Operator + dependencias (`>>`) + scheduling + retries se cubre el 90% de los casos. El operador `>>` es azúcar sintáctico para "depende de". Conectar *retry* con el problema de la diapositiva 2 (el timeout de red que obligaba a reejecutar todo a mano): aquí se resuelve declarativamente.

---

## Diapositiva 6 — La decisión arquitectónica
### ¿Por qué adoptar una plataforma de orquestación?

**Subtítulo:** Centralizar una capability transversal en lugar de reimplementarla en cada script

- **El problema es recurrente y transversal:** scheduling, dependencias, reintentos, monitoreo y auditoría reaparecen en *cada* pipeline.
- **Decisión:** ¿lo resolvemos *ad hoc* en cada script o adoptamos una **plataforma** que provea estas capacidades como servicio?
- Adoptar un orquestador = tratar la orquestación como **preocupación arquitectónica de primera clase**, no como detalle de implementación disperso.
- **Alternativas en el espectro:**
  - `cron` + scripts → mínima inversión, nula gobernanza.
  - Orquestadores (Airflow, Prefect, Dagster) → plataforma dedicada.
  - Orquestación gestionada/cloud (MWAA, Cloud Composer, Astronomer) → terceriza la operación.

⏱️ **Tiempo estimado: 0:45**

> **Notas del presentador:**
> Encadenar con el "build vs. buy/adopt". El argumento ADD: identificamos un conjunto de atributos de calidad (fault tolerance, manageability, auditability) que se necesitan repetidamente; en vez de reimplementarlos artesanalmente y mal en cada script, los centralizamos en una plataforma probada. El costo de esa decisión es complejidad operativa — lo veremos en trade-offs.

---

## Diapositiva 7 — Atributos de calidad (1/2)
### Lo que Airflow fortalece (perspectiva ADD)

**Subtítulo:** Beneficios concretos sobre los quality attributes

| Atributo | Cómo lo aborda Airflow |
|---|---|
| **Fault Tolerance** | Reintentos automáticos, estado persistido en Metadata DB, re-ejecución selectiva de tasks fallidas sin rehacer el DAG completo. |
| **Manageability** | Web UI con estado de cada run, logs centralizados, instrumentación y métricas (StatsD/OpenTelemetry) para monitoreo y tuning. |
| **Auditability** | Historial completo y persistente: qué corrió, cuándo, con qué resultado y por qué falló. Trazabilidad para compliance. |
| **Interoperability** | Cientos de *providers*/operadores: bases de datos, AWS/GCP/Azure, Spark, Kubernetes, APIs. Actúa como capa de integración. |
| **Scalability** | Separación scheduler/executor: del `LocalExecutor` al `Celery`/`KubernetesExecutor` sin reescribir los DAGs. |
| **Portability** | Mismo DAG corre en local, on-premise o cloud; empaquetable en contenedores. |

⏱️ **Tiempo estimado: 1:00**

> **Notas del presentador:**
> No leer la tabla entera: destacar 3 atributos diferenciales. **Fault tolerance** (estado persistido + reintentos + re-run granular) resuelve directamente los fallos silenciosos de la diapositiva 2. **Auditability** es el argumento de venta hacia compliance/gobierno de datos. **Scalability** es la consecuencia directa de la separación arquitectónica de la diapositiva 4. Interoperability vía providers es a menudo el motivo práctico de adopción.

---

## Diapositiva 8 — Atributos de calidad (2/2)
### Los compromisos: ningún atributo es gratis

**Subtítulo:** Cada beneficio tiene un costo arquitectónico asociado

- **Fault Tolerance tiene un límite:** la Metadata DB es **single point of failure**. Alta disponibilidad real exige HA en la base, scheduler redundante y monitoreo → más complejidad.
- **Scalability cuesta operación:** el `CeleryExecutor` agrega broker (Redis/RabbitMQ); el `KubernetesExecutor` exige operar un clúster. Se gana escala, se paga superficie operativa.
- **Manageability/Auditability requieren disciplina:** la UI ayuda, pero sin convenciones de naming, ownership y alerting el historial se vuelve ruido.
- **Portability ≠ trivialidad:** portar es posible, pero las dependencias de Python y los providers atan el entorno; reproducibilidad real exige contenedores.

⏱️ **Tiempo estimado: 0:50**

> **Notas del presentador:**
> Esta diapositiva es deliberadamente la contracara de la anterior — el enfoque de la materia exige mostrar el compromiso, no solo el beneficio. La idea-fuerza: **Airflow no es un motor de cómputo distribuido**. Si se mete el procesamiento pesado dentro de las tasks en lugar de delegarlo (Spark, dbt, el warehouse), se convierte el orquestador en cuello de botella. "Orquestar, no computar" es la frase a recordar.

---

## Diapositiva 9 — Trade-offs
### Qué resuelve y qué NO resuelve Airflow

**Subtítulo:** Delimitar el alcance evita decisiones arquitectónicas erróneas

**✅ Resuelve:**
- Orquestación de dependencias complejas entre tareas.
- Scheduling confiable, reintentos y recuperación ante fallos.
- Observabilidad, trazabilidad y auditoría de procesos.
- Integración heterogénea (workflows as code).

**❌ NO resuelve / NO es:**
- **No es un motor de procesamiento de datos** (eso es Spark/dbt/el warehouse).
- **No es streaming en tiempo real** — es orientado a *batch* y scheduling. Para eventos continuos → Kafka/Flink.
- **No es una herramienta ETL visual** de bajo código (eso se acerca más a NiFi).
- **No es trivial de operar:** requiere mantener scheduler, DB, workers y dependencias.

**Costos:** infraestructura (DB, workers, broker), curva de aprendizaje, mantenimiento de upgrades, esfuerzo de gobierno de DAGs.

⏱️ **Tiempo estimado: 1:00**

> **Notas del presentador:**
> Diapositiva crítica para una audiencia de ingeniería. El error más común es usar Airflow como motor de cómputo o pretender procesamiento en tiempo real. Recalcar la naturaleza **batch**: Airflow dispara y coordina trabajos; el cómputo pesado se delega. Mencionar que el costo operativo es real y no debe subestimarse en el TCO de la decisión.

---

## Diapositiva 10 — Casos de uso reales
### Dónde aporta valor

**Subtítulo:** Patrones de adopción en arquitecturas empresariales

- **ETL / ELT:** orquestar extracción, transformación y carga hacia un Data Warehouse (patrón canónico). En ELT, Airflow dispara las transformaciones (p. ej. dbt) dentro del warehouse.
- **Data Lake / Data Warehouse:** ingesta programada, particionamiento, validaciones de calidad de datos y backfills históricos.
- **Machine Learning Pipelines:** orquestar extracción de features → entrenamiento → validación → despliegue/scoring batch.
- **Automatización de procesos empresariales:** cierres contables, generación de reportes regulatorios, sincronización entre sistemas, conciliaciones nocturnas.

⏱️ **Tiempo estimado: 0:30**

> **Notas del presentador:**
> Ir rápido. El patrón transversal: Airflow es el **director de orquesta** que coordina herramientas especializadas, no el ejecutor del cómputo. En ELT moderno la combinación Airflow + dbt + warehouse cloud es casi un estándar. El caso de "reportes empresariales" es justamente el que mostraremos en la demo.

---

## Diapositiva 11 — Comparación con alternativas
### Airflow en el ecosistema de orquestación

**Subtítulo:** Ninguna herramienta es universalmente superior; depende de los drivers

| Herramienta | Enfoque | Ventaja | Desventaja vs. Airflow |
|---|---|---|---|
| **Cron** | Scheduler de tareas | Trivial, ubicuo, cero infra | Sin dependencias, reintentos, UI ni auditoría |
| **Apache NiFi** | Dataflow visual (flow-based) | Excelente para streaming/ruteo de datos, low-code | Menos orientado a batch-as-code; otra filosofía |
| **Prefect** | Orquestación moderna (Python) | API más liviana, dynamic workflows, DX moderna | Ecosistema/madurez menor; menos providers |
| **Dagster** | Orquestación *asset-oriented* | Foco en data assets, testing, lineage de primera clase | Más nuevo; cambio de paradigma (assets vs. tasks) |

- **Airflow:** máxima madurez, comunidad enorme, ecosistema de providers inmenso → opción "segura" y con menor riesgo de lock-in (gobernanza Apache).
- **Trade-off de Airflow:** API más verbosa y peso operativo mayor que los competidores "modernos".

⏱️ **Tiempo estimado: 0:45**

> **Notas del presentador:**
> El mensaje: la elección depende del driver. Si el problema es **streaming/ruteo** → NiFi. Si se prioriza **DX moderna y workflows dinámicos** → Prefect. Si el centro es el **data asset y el lineage** → Dagster. Airflow gana en **madurez, comunidad y ecosistema**, lo que reduce el riesgo de adopción en entornos empresariales conservadores. No hay ganador absoluto; hay ajuste a contexto.

---

## Diapositiva 12 — Conclusiones
### ¿Qué drivers de negocio justifican incorporar Airflow?

**Subtítulo:** Cierre arquitectónico

- **Confiabilidad de los datos como activo de negocio:** cuando decisiones, reportes y modelos dependen de pipelines, su fiabilidad deja de ser un detalle técnico y pasa a ser un driver de negocio.
- **Gobernanza y auditoría:** trazabilidad de qué corrió, cuándo y con qué resultado → compliance y confianza.
- **Reducción del conocimiento tribal:** workflows as code → versionables, revisables, mantenibles por el equipo.
- **Escalabilidad organizacional:** una plataforma común para decenas/cientos de pipelines en lugar de scripts dispersos.
- **El costo a aceptar:** complejidad operativa y curva de aprendizaje. Airflow se justifica **cuando el costo de la desorganización supera el costo de operar la plataforma**.

> **Decisión arquitectónica = atributos de calidad ganados − costos operativos asumidos.**

⏱️ **Tiempo estimado: 0:35**

> **Notas del presentador:**
> Cerrar volviendo a la pregunta del CLAUDE.md. La frase final resume el método ADD: toda decisión es un balance entre atributos de calidad ganados y costos asumidos. Airflow no es "bueno" o "malo" en abstracto: es la respuesta correcta cuando el driver es la **confiabilidad y gobernanza de pipelines batch a escala**. Transición a la demo: "Veámoslo en un pipeline real de reportes de ventas."

---

## Diapositiva 13 — Demo
### Pipeline diario de reportes de ventas

**Subtítulo:** De la teoría a un DAG en ejecución (5 min)

Veremos en vivo:
1. Definición de un **DAG** y sus **dependencias**.
2. **Ejecución manual** desde la UI.
3. **Estado de cada task** y logs.
4. Manejo de **errores y retries**.

⏱️ **Tiempo estimado: transición — 0:10**

> **Notas del presentador:**
> Slide puente hacia la demostración práctica. Mantenerla mínima. (La demo se prepara en una etapa posterior, según el plan.)

---

### Presupuesto de tiempo (teoría)

| # | Diapositiva | Tiempo |
|---|---|---|
| 1 | Portada | 0:20 |
| 2 | Problema de negocio | 1:15 |
| 3 | Qué es / Historia | 1:00 |
| 4 | Arquitectura | 1:30 |
| 5 | Conceptos fundamentales | 1:00 |
| 6 | Decisión arquitectónica | 0:45 |
| 7 | Atributos de calidad (1/2) | 1:00 |
| 8 | Atributos de calidad (2/2) | 0:50 |
| 9 | Trade-offs | 1:00 |
| 10 | Casos de uso | 0:30 |
| 11 | Comparación | 0:45 |
| 12 | Conclusiones | 0:35 |
| 13 | Transición a demo | 0:10 |
| | **Total** | **≈ 9:30** |
