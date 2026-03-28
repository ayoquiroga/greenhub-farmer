/*
  Consulta 1: Top 3 marcas de celular cuyos dispositivos agotan la batería
              antes de las 18 hs del día.

  Lógica:
    - Filtramos registros donde el dispositivo está descargándose (Discharging)
      sin cargador conectado (unplugged) y la hora del registro es antes de las 18.
    - Consideramos batería "agotada" cuando battery_level <= 15%.
    - Contamos dispositivos únicos por marca y seleccionamos los 3 primeros.

  Join:
    samples.device_id → devices.id
*/
SELECT
    d.brand                       AS marca,
    COUNT(DISTINCT s.device_id)   AS dispositivos_con_bateria_baja
FROM
    `kestra-sandbox-2026.greenhub.samples` s
JOIN
    `kestra-sandbox-2026.greenhub.devices` d ON s.device_id = d.id
WHERE
    s.battery_state = 'Discharging'
    AND s.charger       = 'unplugged'
    AND s.battery_level <= 15          -- batería al 15% o menos (casi agotada)
    AND s.hour          < 18           -- antes de las 18:00 hs
    AND d.brand IS NOT NULL
    AND d.brand != ''
GROUP BY
    d.brand
ORDER BY
    dispositivos_con_bateria_baja DESC
LIMIT 3;
