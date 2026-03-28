/*
  Vista: v_battery_level_daily
  Tile 2 del Dashboard — Gráfico de líneas temporales

  Propósito:
    Muestra la evolución diaria del nivel de batería promedio,
    desglosada por estado de batería (Discharging, Charging, Not charging).
    Permite identificar tendencias y patrones de carga/descarga a lo largo
    del tiempo.

  Filtro interactivo en Looker Studio:
    - La columna `date` es el eje X del gráfico de líneas Y el campo
      que usa el control de rango de fechas global del dashboard.
    - Al acotar el período, la vista solo devuelve las filas del rango
      seleccionado en lugar de recalcular sobre toda la tabla.

  Configuración en Looker Studio:
    - Dimensión (eje X): date
    - Desglose:          battery_state
    - Métrica:           battery_level_promedio (AVG)
    - Métrica extra:     dispositivos_unicos (para contexto en tooltip)
    - Tipo:              Gráfico de series temporales (líneas)
    - Filtro global:     date (control de rango de fechas)

  Fuente: kestra-sandbox-2026.greenhub.samples (tabla nativa particionada)
*/
CREATE OR REPLACE VIEW `kestra-sandbox-2026.greenhub.v_battery_level_daily`
AS
SELECT
    date,
    battery_state,
    ROUND(AVG(battery_level), 2)       AS battery_level_promedio,
    ROUND(AVG(up_time) / 3600.0, 1)   AS promedio_horas_encendido,
    COUNT(*)                           AS registros,
    COUNT(DISTINCT device_id)          AS dispositivos_unicos
FROM
    `kestra-sandbox-2026.greenhub.samples`
WHERE
    battery_level IS NOT NULL
    AND battery_state IS NOT NULL
    AND date          IS NOT NULL
GROUP BY
    date,
    battery_state
ORDER BY
    date,
    battery_state;
