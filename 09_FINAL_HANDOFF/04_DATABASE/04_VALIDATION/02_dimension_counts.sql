-- Flight DW 2025 | Exact row counts for small dimensions and mapping table
-- Safe to run while Bridge ETL is in progress. Does not scan the large fact/bridge tables.
USE flight_dw_2025;

SELECT 'dim_airline' AS TableName, COUNT(*) AS ActualRows, 14 AS ExpectedRows,
       IF(COUNT(*) = 14, 'PASS', 'CHECK') AS Result
FROM dim_airline
UNION ALL
SELECT 'dim_airport', COUNT(*), 86335,
       IF(COUNT(*) = 86335, 'PASS', 'CHECK')
FROM dim_airport
UNION ALL
SELECT 'dim_date', COUNT(*), 370,
       IF(COUNT(*) = 370, 'PASS', 'CHECK')
FROM dim_date
UNION ALL
SELECT 'dim_time', COUNT(*), 1442,
       IF(COUNT(*) = 1442, 'PASS', 'CHECK')
FROM dim_time
UNION ALL
SELECT 'map_bts_airport', COUNT(*), 352,
       IF(COUNT(*) = 352, 'PASS', 'CHECK')
FROM map_bts_airport;
