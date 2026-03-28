/*
  Vista: v_battery_by_charger_daily
  Tile 1 del Dashboard — Barras apiladas (100%)

  Propósito:
    Muestra la distribución del estado de batería (Discharging, Charging,
    Not charging) agrupada por tipo de cargador (unplugged, ac, usb, wireless)
    y por día.

  Filtro interactivo en Looker Studio:
    - La columna `date` permite al visitante filtrar por rango de fechas.
    - Al cambiar el rango, el gráfico se recalcula sobre las filas
      ya pre-agrupadas de esta vista (no sobre los 21M de samples).

  Configuración en Looker Studio:
    - Dimensión:     charger
    - Desglose:      battery_state
    - Métrica:       registros (SUM)
    - Tipo:          Gráfico de barras apiladas 100%
    - Filtro global: date (control de rango de fechas)

  Fuente: kestra-sandbox-2026.greenhub.samples (tabla nativa particionada)
*/
CREATE OR REPLACE VIEW `kestra-sandbox-2026.greenhub.v_battery_by_charger_daily`
AS
SELECT
    date,
    charger,
    battery_state,
    COUNT(*)                           AS registros,
    COUNT(DISTINCT device_id)          AS dispositivos_unicos,
    ROUND(AVG(battery_level), 2)       AS battery_level_promedio
FROM
    `kestra-sandbox-2026.greenhub.samples`
WHERE
    charger       IS NOT NULL
    AND battery_state IS NOT NULL
    AND date          IS NOT NULL
GROUP BY
    date,
    charger,
    battery_state;
