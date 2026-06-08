# Changelog — Presentación (Apache Airflow)

Registro de cambios de la presentación teórica (`presentacion.md`).
Formato: las entradas más recientes se agregan AL FINAL.
Lectura recomendada: `tail -n 30 CHANGELOG_PRESENTACION.md` (o `Get-Content CHANGELOG_PRESENTACION.md -Tail 30` en PowerShell).

---

## [0.1.0] — 2026-06-05 — Versión inicial
### Added
- Presentación completa en Markdown: 14 diapositivas (13 de contenido + 1 transición a demo).
- Estructura alineada al enfoque ADD del CLAUDE.md: drivers de negocio, atributos de calidad y trade-offs.
- Cobertura del contenido mínimo requerido:
  - Introducción e historia (Airbnb 2014 → Apache TLP 2019).
  - Automatización vs. orquestación.
  - Diagrama conceptual de arquitectura (Scheduler, Executor, Workers, Metadata DB, Web UI).
  - Conceptos fundamentales (DAG, Operator, Task, Dependency, Trigger, Retry, Scheduling).
  - Análisis de los 7 atributos de calidad (Fault Tolerance, Interoperability, Manageability, Scalability, Auditability, Portability) con beneficios y compromisos.
  - Trade-offs explícitos (qué resuelve / qué NO resuelve / costos).
  - Casos de uso (ETL, ELT, Data Lake/DW, ML pipelines, automatización empresarial).
  - Comparación con Cron, NiFi, Prefect y Dagster.
  - Criterios de cuándo usar / cuándo no.
  - Conclusiones respondiendo al driver de negocio.
- Notas del presentador y tiempo estimado por diapositiva.
- Tabla de presupuesto de tiempo total (≈ 10:00 min).

### Notes
- Demo aún NO generada (según instrucción del CLAUDE.md: primero revisar presentación).
- Pendiente: incorporar feedback del usuario tras revisión.

---

## [0.2.0] — 2026-06-05 — Ajustes de contenido + presentación en Canva
### Removed
- Eliminada la diapositiva "¿Cuándo usar Airflow? (Recomendado vs. mala elección)".

### Changed
- Renumeración: ahora 13 diapositivas (Conclusiones → 12, Demo → 13).
- Tabla de presupuesto de tiempo actualizada (total ≈ 9:30 min).

### Added
- Generada la presentación visual en Canva (estilo profesional/académico) a partir del outline de 13 diapositivas.
- Variante elegida: B (de 4 candidatas).
- Proyecto Canva: "Presentación - Orquestación" (14 páginas; incluye portada generada).
  - Editar: https://www.canva.com/d/P3oRhPcukL02e1g
  - Ver: https://www.canva.com/d/Kxu5ceCJSupum9T

### Notes
- Canva NO importa las notas del presentador ni los tiempos del .md (pendiente cargarlas como presenter notes si se desea).
- Fuente de verdad del contenido: presentacion.md.

---

## [0.3.0] — 2026-06-08 — Reducción de atributos de calidad
### Removed
- Eliminados los atributos **Interoperability** y **Portability** del análisis de atributos de calidad.
  - Diapositiva 7 (Atributos de calidad 1/2): quitadas ambas filas de la tabla.
  - Diapositiva 8 (Atributos de calidad 2/2): quitado el compromiso asociado a Portability.

### Changed
- Notas del presentador de la Diapositiva 7 ajustadas (eliminada la mención a Interoperability vía providers).
- El análisis ADD queda enfocado en los 4 atributos del CLAUDE.md: Fault Tolerance, Manageability, Scalability y Auditability.
