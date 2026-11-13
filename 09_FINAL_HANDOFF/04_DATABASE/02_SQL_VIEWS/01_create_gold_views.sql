-- ============================================================================
-- FLIGHT DW 2025 | GOLD LAYER - AGGREGATED VIEWS
-- Target: MySQL 8.0+
-- Source: fact_flight + dim_date + dim_airline + dim_airport + dim_time
-- Scope: calendar year 2025
--
-- IMPORTANT
-- 1) These are logical SQL views; CREATE VIEW does not materialize/cache rows.
-- 2) Querying a view aggregates fact_flight (7,001,619 source flight rows).
-- 3) Import only the views needed by Power BI; avoid refreshing all views
--    repeatedly during active ETL.
-- 4) Weather views are intentionally excluded until STEP 30E3 and 30E4 PASS.
-- 5) No source, RAW, STAGING, CLEAN, Fact or Dimension rows are modified.
-- ============================================================================

USE flight_dw_2025;

-- 1. Overall KPI: one row for the full 2025 BTS reporting-airline scope.
CREATE OR REPLACE VIEW vw_gold_flight_kpi AS
SELECT
    2025 AS CalendarYear,
    COUNT(*) AS FlightRows,
    SUM(f.FlightCount) AS TotalFlights,
    SUM(CASE WHEN f.Cancelled = 1 THEN f.FlightCount ELSE 0 END) AS CancelledFlights,
    SUM(CASE WHEN f.Diverted = 1 THEN f.FlightCount ELSE 0 END) AS DivertedFlights,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleDepartureFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedDepartureFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DepartureDelayRatePct,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleArrivalFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedArrivalFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS ArrivalDelayRatePct,

    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.DepDelay IS NOT NULL
        THEN f.DepDelay END), 2) AS AvgSignedDepartureDelayMinutes,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDelay IS NOT NULL
        THEN f.ArrDelay END), 2) AS AvgSignedArrivalDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
WHERE d.CalendarYear = 2025;

-- 2. Monthly summary: suitable for trend charts.
CREATE OR REPLACE VIEW vw_gold_monthly_summary AS
SELECT
    d.CalendarYear,
    d.MonthNumber,
    d.MonthName,
    COUNT(*) AS FlightRows,
    SUM(f.FlightCount) AS TotalFlights,
    SUM(CASE WHEN f.Cancelled = 1 THEN f.FlightCount ELSE 0 END) AS CancelledFlights,
    SUM(CASE WHEN f.Diverted = 1 THEN f.FlightCount ELSE 0 END) AS DivertedFlights,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleDepartureFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedDepartureFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DepartureDelayRatePct,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleArrivalFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedArrivalFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS ArrivalDelayRatePct,

    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.DepDelay IS NOT NULL
        THEN f.DepDelay END), 2) AS AvgSignedDepartureDelayMinutes,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDelay IS NOT NULL
        THEN f.ArrDelay END), 2) AS AvgSignedArrivalDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
WHERE d.CalendarYear = 2025
GROUP BY
    d.CalendarYear,
    d.MonthNumber,
    d.MonthName;

-- 3. Airline performance: reporting-airline level.
-- The dimension contains carrier codes/identifiers, not carrier display names.
CREATE OR REPLACE VIEW vw_gold_airline_performance AS
SELECT
    a.AirlineKey,
    a.DOT_ID_Reporting_Airline,
    a.ReportingAirlineCode,
    a.IATAReportingAirlineCode,
    COUNT(*) AS FlightRows,
    SUM(f.FlightCount) AS TotalFlights,
    SUM(CASE WHEN f.Cancelled = 1 THEN f.FlightCount ELSE 0 END) AS CancelledFlights,
    SUM(CASE WHEN f.Diverted = 1 THEN f.FlightCount ELSE 0 END) AS DivertedFlights,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleDepartureFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedDepartureFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DepartureDelayRatePct,

    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleArrivalFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedArrivalFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS ArrivalDelayRatePct,

    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.DepDelay IS NOT NULL
        THEN f.DepDelay END), 2) AS AvgSignedDepartureDelayMinutes,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDelay IS NOT NULL
        THEN f.ArrDelay END), 2) AS AvgSignedArrivalDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
INNER JOIN dim_airline AS a
    ON a.AirlineKey = f.AirlineKey
WHERE d.CalendarYear = 2025
GROUP BY
    a.AirlineKey,
    a.DOT_ID_Reporting_Airline,
    a.ReportingAirlineCode,
    a.IATAReportingAirlineCode;

-- 4. Airport performance by role.
-- ORIGIN uses departure delay measures; DESTINATION uses arrival measures.
-- Arrival eligibility excludes cancelled and diverted flights.
CREATE OR REPLACE VIEW vw_gold_airport_role_performance AS
SELECT
    ap.AirportKey,
    ap.OurAirportsID,
    ap.Ident,
    ap.IATACode,
    ap.AirportName,
    ap.Municipality,
    ap.ISOCountry,
    'ORIGIN' AS AirportRole,
    SUM(f.FlightCount) AS ScheduledFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DelayRatePct,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.DepDelay IS NOT NULL
        THEN f.DepDelay END), 2) AS AvgSignedDelayMinutes,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDelayMinutes IS NOT NULL
        THEN f.DepDelayMinutes ELSE 0 END) AS TotalPositiveDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
INNER JOIN dim_airport AS ap
    ON ap.AirportKey = f.OriginAirportKey
WHERE d.CalendarYear = 2025
GROUP BY
    ap.AirportKey, ap.OurAirportsID, ap.Ident, ap.IATACode,
    ap.AirportName, ap.Municipality, ap.ISOCountry

UNION ALL

SELECT
    ap.AirportKey,
    ap.OurAirportsID,
    ap.Ident,
    ap.IATACode,
    ap.AirportName,
    ap.Municipality,
    ap.ISOCountry,
    'DESTINATION' AS AirportRole,
    SUM(f.FlightCount) AS ScheduledFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DelayRatePct,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDelay IS NOT NULL
        THEN f.ArrDelay END), 2) AS AvgSignedDelayMinutes,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDelayMinutes IS NOT NULL
        THEN f.ArrDelayMinutes ELSE 0 END) AS TotalPositiveDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
INNER JOIN dim_airport AS ap
    ON ap.AirportKey = f.DestinationAirportKey
WHERE d.CalendarYear = 2025
GROUP BY
    ap.AirportKey, ap.OurAirportsID, ap.Ident, ap.IATACode,
    ap.AirportName, ap.Municipality, ap.ISOCountry;

-- 5. Scheduled departure time profile.
-- TimeKey=0 (Unknown) is excluded. The 24:00 sentinel is retained separately.
CREATE OR REPLACE VIEW vw_gold_departure_hour_performance AS
SELECT
    t.HourOfDay AS ScheduledDepartureHour,
    t.IsEndOfDay,
    CASE
        WHEN t.IsEndOfDay = 1 THEN '24:00'
        ELSE CONCAT(LPAD(t.HourOfDay, 2, '0'), ':00')
    END AS ScheduledDepartureHourLabel,
    SUM(f.FlightCount) AS ScheduledFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleDepartureFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedDepartureFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DepartureDelayRatePct,
    ROUND(AVG(CASE
        WHEN f.Cancelled = 0 AND f.DepDelay IS NOT NULL
        THEN f.DepDelay END), 2) AS AvgSignedDepartureDelayMinutes
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
INNER JOIN dim_time AS t
    ON t.TimeKey = f.CRSDepTimeKey
WHERE d.CalendarYear = 2025
  AND t.IsUnknown = 0
GROUP BY
    t.HourOfDay,
    t.IsEndOfDay;

-- 6. Monthly x airline performance: useful for a matrix/heatmap.
CREATE OR REPLACE VIEW vw_gold_monthly_airline_performance AS
SELECT
    d.CalendarYear,
    d.MonthNumber,
    d.MonthName,
    a.AirlineKey,
    a.ReportingAirlineCode,
    a.IATAReportingAirlineCode,
    SUM(f.FlightCount) AS TotalFlights,
    SUM(CASE WHEN f.Cancelled = 1 THEN f.FlightCount ELSE 0 END) AS CancelledFlights,
    SUM(CASE WHEN f.Diverted = 1 THEN f.FlightCount ELSE 0 END) AS DivertedFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleDepartureFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.DepDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedDepartureFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.DepDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS DepartureDelayRatePct,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
        THEN f.FlightCount ELSE 0 END) AS EligibleArrivalFlights,
    SUM(CASE
        WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
        THEN f.FlightCount ELSE 0 END) AS DelayedArrivalFlights,
    ROUND(
        100.0 * SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 = 1
            THEN f.FlightCount ELSE 0 END)
        / NULLIF(SUM(CASE
            WHEN f.Cancelled = 0 AND f.Diverted = 0 AND f.ArrDel15 IS NOT NULL
            THEN f.FlightCount ELSE 0 END), 0),
        2
    ) AS ArrivalDelayRatePct
FROM fact_flight AS f
INNER JOIN dim_date AS d
    ON d.DateKey = f.FlightDateKey
INNER JOIN dim_airline AS a
    ON a.AirlineKey = f.AirlineKey
WHERE d.CalendarYear = 2025
GROUP BY
    d.CalendarYear, d.MonthNumber, d.MonthName,
    a.AirlineKey, a.ReportingAirlineCode, a.IATAReportingAirlineCode;

-- End of Gold SQL Views.
