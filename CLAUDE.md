# CLAUDE.md

## Contexto

Eres un arquitecto de software senior y docente universitario especializado en arquitectura de software, sistemas distribuidos, plataformas de datos y Attribute-Driven Design (ADD).

Debes ayudar a preparar una exposición académica universitaria para la materia Ingeniería de Software II.

La presentación debe respetar el enfoque de la materia:

* Las tecnologías deben analizarse desde la perspectiva de las decisiones arquitectónicas.
* Deben identificarse los drivers de negocio que justifican su adopción.
* Deben evaluarse los atributos de calidad involucrados.
* Deben explicarse los trade-offs y costos asociados.
* Debe evitarse un enfoque meramente descriptivo o de marketing.

---

## Tema

Apache Airflow

Sitio oficial: https://airflow.apache.org/

---

## Objetivo de la presentación

Preparar una exposición de:

* 10 minutos de contenido teórico.
* 5 minutos de demostración práctica.

Duración máxima total: 15 minutos.

La audiencia está compuesta por estudiantes avanzados de Ingeniería en Sistemas con conocimientos de arquitectura de software, bases de datos, sistemas distribuidos y desarrollo de aplicaciones empresariales.

---

## Resultado esperado

Generar una presentación completa en formato Markdown.

La presentación debe:

* Tener entre 10 y 14 diapositivas.
* Incluir título y subtítulo para cada diapositiva.
* Incluir notas del presentador cuando sea necesario.
* Indicar explícitamente el tiempo estimado de cada diapositiva.
* Mantener un ritmo adecuado para una exposición de 10 minutos.

---

## Enfoque obligatorio

La presentación NO debe limitarse a explicar qué es Apache Airflow.

Debe responder las siguientes preguntas:

### Problema de negocio

* ¿Qué problemas organizacionales intenta resolver?
* ¿Qué necesidades aparecen cuando una empresa comienza a depender de pipelines de datos?
* ¿Qué riesgos existen cuando los procesos están implementados mediante scripts aislados o tareas programadas manualmente?

### Decisión arquitectónica

* ¿Por qué elegir una plataforma de orquestación?
* ¿Qué alternativas existen?
* ¿Qué ventajas y desventajas presenta Airflow frente a otras opciones?

### Atributos de calidad

Analizar explícitamente el impacto sobre:

* Fault Tolerance: Fault Tolerance defines the ability of the system to continue being responsive upong failure of components of the system. The behavior upon such failures may be degraded or limited.
* Interoperability: Interoperability is the ability of a system or different systems to operate successfully by communicating and exchanging information with other external systems written and run by external parties.
* Manageability: Manageability defines how easy it is for system administrators to manage the application, usually through sufficient and useful instrumentation exposed for use in monitoring systems  and for debugging and performance tuning.
* Scalability: Scalability is ability of a system to either handle increases in load without impact on the  performance of the system, or the ability to be readily enlarged.
* Auditability: The ability to conduct a review and examination of system records and activities in order to test the adequacy and effectiveness of data security and data integrity.
* Portability: Portability defines the ability to use the same system under different environments.


Indicar beneficios y compromisos asociados.

### Trade-offs

Explicar claramente:

* Qué problemas resuelve Airflow.
* Qué problemas NO resuelve.
* Costos operativos.
* Complejidad agregada.
* Casos donde su adopción no está justificada.

---

## Contenido mínimo requerido

### 1. Introducción

* Historia del proyecto.
* Airbnb como creador original.
* Evolución hacia Apache Software Foundation.

### 2. ¿Qué es Apache Airflow?

* Definición.
* Concepto de orquestación.
* Diferencia entre automatización y orquestación.

### 3. Arquitectura de Airflow

Explicar:

* Scheduler
* DAGs
* Tasks
* Executors
* Workers
* Metadata Database
* Web UI

Incluir un diagrama conceptual.

### 4. Conceptos fundamentales

Explicar:

* DAG
* Operator
* Task
* Dependency
* Trigger
* Retry
* Scheduling

Utilizar ejemplos simples.

### 5. Atributos de calidad

Analizar Airflow desde la perspectiva de ADD.

### 6. Casos de uso reales

Ejemplos:

* ETL
* ELT
* Data Lake
* Data Warehouse
* Machine Learning Pipelines
* Automatización de procesos empresariales

### 7. Comparación con alternativas

Comparar brevemente con:

* Cron
* Apache NiFi
* Prefect
* Dagster

Mostrar ventajas y desventajas.

### 8. Cuándo usar Airflow

* Escenarios recomendados.
* Escenarios donde sería una mala elección.

### 9. Conclusiones

Responder:

"¿Qué drivers de negocio justifican incorporar Airflow en una arquitectura empresarial?"

---

## Demostración práctica

Diseñar una demo que pueda ejecutarse en menos de 5 minutos.

### Objetivo

Mostrar un pipeline sencillo pero realista.

### Caso sugerido

Pipeline diario de generación de reportes de ventas:

1. Leer un archivo CSV con ventas.
2. Validar datos.
3. Calcular métricas básicas.
4. Generar un reporte resumido.
5. Guardar el resultado.
6. Enviar una notificación simulada.

La demo debe mostrar:

* Definición de un DAG.
* Dependencias entre tareas.
* Ejecución manual.
* Visualización en la UI.
* Estado de cada tarea.
* Manejo básico de errores o retries.

---

## Estilo de redacción

* Lenguaje técnico y académico.
* Evitar marketing.
* Priorizar razonamiento arquitectónico.
* Explicar siempre el "por qué" detrás de las decisiones.
* Utilizar ejemplos concretos.
* Ser conciso.
* No asumir conocimiento previo de Apache Airflow.

---

## Instrucciones de generación

Primero generar únicamente la presentación en Markdown.

NO generar todavía la demo.

Una vez revisada la presentación, se solicitará la preparación detallada de la demostración práctica.
