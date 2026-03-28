/*
  Consulta 2: Top 3 marcas de celular cuya batería dura más de 24 horas encendida.

  Lógica:
    - up_time es el tiempo (en segundos) que el dispositivo lleva encendido
      desde el último reinicio.
    - Filtramos registros donde el dispositivo está descargándose (Discharging)
      sin cargador (unplugged) y up_time supera las 24 horas (86.400 segundos).
    - Esto indica que la batería aguantó más de 24 hs de uso continuo.
    - Mostramos la marca, la cantidad de dispositivos únicos y el promedio de
      horas encendido para contexto.

  Join:
    samples.device_id → devices.id
*/
SELECT
    d.brand                                        AS marca,
    COUNT(DISTINCT s.device_id)                    AS dispositivos,
    ROUND(AVG(s.up_time) / 3600.0, 1)             AS promedio_horas_encendido
FROM
    `kestra-sandbox-2026.greenhub.samples` s
JOIN
    `kestra-sandbox-2026.greenhub.devices` d ON s.device_id = d.id
WHERE
    s.battery_state = 'Discharging'
    AND s.charger    = 'unplugged'
    AND s.up_time    > 86400           -- más de 24 horas encendido (en segundos)
    AND d.brand IS NOT NULL
    AND d.brand != ''
GROUP BY
    d.brand
ORDER BY
    dispositivos DESC
LIMIT 3;
