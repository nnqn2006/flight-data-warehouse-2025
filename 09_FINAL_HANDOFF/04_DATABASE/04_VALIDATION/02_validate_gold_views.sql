-- Metadata-only validation. This does NOT scan fact_flight.
USE flight_dw_2025;

SELECT
    TABLE_NAME,
    VIEW_DEFINITION IS NOT NULL AS HasDefinition
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'flight_dw_2025'
  AND TABLE_NAME IN (
      'vw_gold_flight_kpi',
      'vw_gold_monthly_summary',
      'vw_gold_airline_performance',
      'vw_gold_airport_role_performance',
      'vw_gold_departure_hour_performance',
      'vw_gold_monthly_airline_performance'
  )
ORDER BY TABLE_NAME;

SELECT COUNT(*) AS GoldViewsCreated
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'flight_dw_2025'
  AND TABLE_NAME LIKE 'vw_gold_%';

-- Expected from this script: 6 Gold views.
-- Do not run SELECT * FROM each view repeatedly: each view is logical and
-- will aggregate the 7,001,619-row fact_flight table when queried.
