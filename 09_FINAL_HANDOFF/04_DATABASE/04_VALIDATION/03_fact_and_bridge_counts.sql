-- Flight DW 2025 | Exact row counts for large fact/bridge tables
-- IMPORTANT: Run only after STEP 30E3 and STEP 30E4 are PASS, the Bridge loader has stopped,
-- and the latest log/checkpoint confirms all 48 source partitions are committed and validated.
-- This script performs full COUNT(*) scans and may take time. Run once, not repeatedly.
USE flight_dw_2025;

SELECT 'fact_flight' AS TableName, COUNT(*) AS ActualRows, 7001619 AS ExpectedRows,
       IF(COUNT(*) = 7001619, 'PASS', 'CHECK') AS Result
FROM fact_flight
UNION ALL
SELECT 'fact_airport_weather_hourly', COUNT(*), 3083520,
       IF(COUNT(*) = 3083520, 'PASS', 'CHECK')
FROM fact_airport_weather_hourly
UNION ALL
SELECT 'bridge_flight_weather_exposure', COUNT(*), 28006476,
       IF(COUNT(*) = 28006476, 'PASS', 'CHECK')
FROM bridge_flight_weather_exposure;
