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
