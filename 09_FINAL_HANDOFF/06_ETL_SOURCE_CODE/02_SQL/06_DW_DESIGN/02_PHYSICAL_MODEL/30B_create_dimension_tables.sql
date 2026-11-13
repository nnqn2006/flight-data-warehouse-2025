/*
===============================================================================
FLIGHT DATA WAREHOUSE 2025 - STEP 30B
Physical model: initial Dimension and BTS airport mapping structures
Target: MySQL 8.0.46 / InnoDB / utf8mb4

Safety:
- This script does NOT DROP or TRUNCATE any database/table.
- It creates tables only when they do not already exist.
- It seeds Dim_Date and Dim_Time with deterministic calendar rows.
- It does NOT load BTS, OurAirports, Open-Meteo or Bridge source data.
- It does NOT modify any RAW, STAGING, CLEAN or GOLD CSV files.

Run this script with the active schema set to flight_dw_2025.
===============================================================================
*/

CREATE DATABASE IF NOT EXISTS flight_dw_2025
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE flight_dw_2025;

SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;

/* ---------------------------------------------------------------------------
   1. Dim_Date
   Date range includes a small UTC boundary buffer around calendar year 2025.
   DateKey format: YYYYMMDD. DateKey = 0 is the Unknown member.
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Dim_Date (
    DateKey             INT UNSIGNED NOT NULL,
    CalendarDate        DATE NULL,
    CalendarYear        SMALLINT UNSIGNED NULL,
    CalendarQuarter     TINYINT UNSIGNED NULL,
    MonthNumber         TINYINT UNSIGNED NULL,
    MonthName           VARCHAR(9) NULL,
    DayOfMonth          TINYINT UNSIGNED NULL,
    DayOfWeekISO        TINYINT UNSIGNED NULL COMMENT 'Monday=1 ... Sunday=7',
    DayName             VARCHAR(9) NULL,
    DayOfYear           SMALLINT UNSIGNED NULL,
    ISOYear             SMALLINT UNSIGNED NULL,
    ISOWeekNumber       TINYINT UNSIGNED NULL,
    IsWeekend           TINYINT(1) NULL,
    IsLeapYear          TINYINT(1) NULL,
    IsUnknown           TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (DateKey),
    UNIQUE KEY uq_dim_date_calendar_date (CalendarDate),
    KEY ix_dim_date_year_month (CalendarYear, MonthNumber),
    KEY ix_dim_date_iso_year_week (ISOYear, ISOWeekNumber)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Calendar dimension; DateKey uses YYYYMMDD, 0 is Unknown';

/* Unknown date member. */
INSERT IGNORE INTO Dim_Date (
    DateKey, CalendarDate, CalendarYear, CalendarQuarter, MonthNumber,
    MonthName, DayOfMonth, DayOfWeekISO, DayName, DayOfYear,
    ISOYear, ISOWeekNumber, IsWeekend, IsLeapYear, IsUnknown
) VALUES (
    0, NULL, NULL, NULL, NULL, 'UNKNOWN', NULL, NULL, 'UNKNOWN', NULL,
    NULL, NULL, NULL, NULL, 1
);

/* Seed 2024-12-30 through 2026-01-02 (369 calendar dates) plus Unknown. */
SET @step30b_old_cte_depth := @@SESSION.cte_max_recursion_depth;
SET SESSION cte_max_recursion_depth = 2000;

INSERT IGNORE INTO Dim_Date (
    DateKey, CalendarDate, CalendarYear, CalendarQuarter, MonthNumber,
    MonthName, DayOfMonth, DayOfWeekISO, DayName, DayOfYear,
    ISOYear, ISOWeekNumber, IsWeekend, IsLeapYear, IsUnknown
)
WITH RECURSIVE date_seq AS (
    SELECT DATE('2024-12-30') AS d
    UNION ALL
    SELECT DATE_ADD(d, INTERVAL 1 DAY)
    FROM date_seq
    WHERE d < DATE('2026-01-02')
)
SELECT
    CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED) AS DateKey,
    d AS CalendarDate,
    YEAR(d) AS CalendarYear,
    QUARTER(d) AS CalendarQuarter,
    MONTH(d) AS MonthNumber,
    DATE_FORMAT(d, '%M') AS MonthName,
    DAYOFMONTH(d) AS DayOfMonth,
    WEEKDAY(d) + 1 AS DayOfWeekISO,
    DAYNAME(d) AS DayName,
    DAYOFYEAR(d) AS DayOfYear,
    FLOOR(YEARWEEK(d, 3) / 100) AS ISOYear,
    WEEK(d, 3) AS ISOWeekNumber,
    CASE WHEN WEEKDAY(d) >= 5 THEN 1 ELSE 0 END AS IsWeekend,
    CASE
        WHEN MOD(YEAR(d), 400) = 0
          OR (MOD(YEAR(d), 4) = 0 AND MOD(YEAR(d), 100) <> 0)
        THEN 1 ELSE 0
    END AS IsLeapYear,
    0 AS IsUnknown
FROM date_seq;

SET SESSION cte_max_recursion_depth = @step30b_old_cte_depth;

/* ---------------------------------------------------------------------------
   2. Dim_Time
   Minute grain. MinuteOfDay 0..1439 covers 00:00..23:59.
   MinuteOfDay 1440 represents source HHMM value 2400 (end-of-day sentinel).
   TimeKey = MinuteOfDay + 1; TimeKey = 0 is the Unknown member.
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Dim_Time (
    TimeKey             SMALLINT UNSIGNED NOT NULL,
    MinuteOfDay         SMALLINT UNSIGNED NULL,
    TimeValue           TIME NULL COMMENT 'NULL for Unknown and 24:00 sentinel',
    TimeLabel           VARCHAR(8) NOT NULL,
    HourOfDay           TINYINT UNSIGNED NULL,
    MinuteOfHour        TINYINT UNSIGNED NULL,
    IsEndOfDay          TINYINT(1) NOT NULL DEFAULT 0,
    IsUnknown           TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (TimeKey),
    UNIQUE KEY uq_dim_time_minute_of_day (MinuteOfDay),
    KEY ix_dim_time_hour_minute (HourOfDay, MinuteOfHour)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Minute-of-day dimension; includes 24:00 and Unknown members';

/* Unknown time member. */
INSERT IGNORE INTO Dim_Time (
    TimeKey, MinuteOfDay, TimeValue, TimeLabel,
    HourOfDay, MinuteOfHour, IsEndOfDay, IsUnknown
) VALUES (
    0, NULL, NULL, 'UNKNOWN', NULL, NULL, 0, 1
);

/* Seed all 1,441 minute positions, including the 24:00 sentinel. */
SET @step30b_old_cte_depth := @@SESSION.cte_max_recursion_depth;
SET SESSION cte_max_recursion_depth = 2000;

INSERT IGNORE INTO Dim_Time (
    TimeKey, MinuteOfDay, TimeValue, TimeLabel,
    HourOfDay, MinuteOfHour, IsEndOfDay, IsUnknown
)
WITH RECURSIVE minute_seq AS (
    SELECT 0 AS m
    UNION ALL
    SELECT m + 1
    FROM minute_seq
    WHERE m < 1440
)
SELECT
    m + 1 AS TimeKey,
    m AS MinuteOfDay,
    CASE
        WHEN m = 1440 THEN NULL
        ELSE MAKETIME(FLOOR(m / 60), MOD(m, 60), 0)
    END AS TimeValue,
    CONCAT(
        LPAD(FLOOR(m / 60), 2, '0'),
        ':',
        LPAD(MOD(m, 60), 2, '0')
    ) AS TimeLabel,
    FLOOR(m / 60) AS HourOfDay,
    MOD(m, 60) AS MinuteOfHour,
    CASE WHEN m = 1440 THEN 1 ELSE 0 END AS IsEndOfDay,
    0 AS IsUnknown
FROM minute_seq;

SET SESSION cte_max_recursion_depth = @step30b_old_cte_depth;

/* ---------------------------------------------------------------------------
   3. Dim_Airline
   DOT_ID_Reporting_Airline is the governed business key candidate.
   No airline name is fabricated: source provides carrier codes, not names.
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Dim_Airline (
    AirlineKey                  INT UNSIGNED NOT NULL AUTO_INCREMENT,
    DOT_ID_Reporting_Airline     INT UNSIGNED NOT NULL,
    ReportingAirlineCode         CHAR(2) NOT NULL,
    IATAReportingAirlineCode     CHAR(2) NOT NULL,
    CreatedAt                   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt                   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                               ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (AirlineKey),
    UNIQUE KEY uq_dim_airline_dot_id (DOT_ID_Reporting_Airline),
    KEY ix_dim_airline_reporting_code (ReportingAirlineCode),
    KEY ix_dim_airline_iata_code (IATAReportingAirlineCode),
    CONSTRAINT chk_dim_airline_dot_id_positive
        CHECK (DOT_ID_Reporting_Airline > 0)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Airline dimension; DOT Airline ID is the natural business key';

/* ---------------------------------------------------------------------------
   4. Dim_Airport
   OurAirports is the canonical source for airport descriptive attributes.
   LatitudeRaw is retained to preserve a non-numeric source token if present;
   LatitudeDeg is the parsed numeric attribute used for spatial analysis.
   TimeZoneIANA is populated later from the separately audited timezone map.
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Dim_Airport (
    AirportKey          INT UNSIGNED NOT NULL AUTO_INCREMENT,
    OurAirportsID       INT UNSIGNED NOT NULL,
    Ident               VARCHAR(8) NOT NULL,
    AirportType         VARCHAR(20) NOT NULL,
    AirportName         VARCHAR(150) NOT NULL,
    LatitudeRaw         VARCHAR(32) NOT NULL,
    LatitudeDeg         DECIMAL(10,7) NULL,
    LongitudeDeg        DECIMAL(10,7) NULL,
    ElevationFt         SMALLINT NULL,
    Continent           CHAR(2) NULL,
    ISOCountry          CHAR(2) NULL,
    ISORegion           VARCHAR(8) NULL,
    Municipality        VARCHAR(80) NULL,
    ScheduledService    VARCHAR(3) NULL,
    ICAOCode            VARCHAR(4) NULL,
    IATACode            CHAR(3) NULL,
    GPSCode             VARCHAR(4) NULL,
    LocalCode           VARCHAR(10) NULL,
    HomeLink            VARCHAR(255) NULL,
    WikipediaLink       VARCHAR(255) NULL,
    Keywords            VARCHAR(512) NULL,
    TimeZoneIANA        VARCHAR(64) NULL,
    CreatedAt           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (AirportKey),
    UNIQUE KEY uq_dim_airport_ourairports_id (OurAirportsID),
    UNIQUE KEY uq_dim_airport_ident (Ident),
    UNIQUE KEY uq_dim_airport_key_natural (AirportKey, OurAirportsID),
    KEY ix_dim_airport_iata (IATACode),
    KEY ix_dim_airport_icao (ICAOCode),
    KEY ix_dim_airport_country_region (ISOCountry, ISORegion),
    KEY ix_dim_airport_timezone (TimeZoneIANA),
    CONSTRAINT chk_dim_airport_latitude
        CHECK (LatitudeDeg IS NULL OR LatitudeDeg BETWEEN -90 AND 90),
    CONSTRAINT chk_dim_airport_longitude
        CHECK (LongitudeDeg IS NULL OR LongitudeDeg BETWEEN -180 AND 180)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Airport dimension; OurAirports provides canonical descriptive attributes';

/* ---------------------------------------------------------------------------
   5. Map_BTS_Airport
   Resolves BTS IATA airport codes to Dim_Airport rows.
   Includes direct and historically resolved aliases (e.g. PBI) explicitly.
   No airport mapping rows are inserted in this DDL step.
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Map_BTS_Airport (
    BTSAirportCode          CHAR(3) NOT NULL,
    AirportKey              INT UNSIGNED NOT NULL,
    OurAirportsID           INT UNSIGNED NOT NULL,
    MatchStatus             VARCHAR(20) NOT NULL,
    OriginalMatchStatus     VARCHAR(30) NULL,
    ResolvedMatchStatus     VARCHAR(30) NOT NULL,
    MappingMethod           VARCHAR(40) NOT NULL,
    OriginFlightRows        INT UNSIGNED NOT NULL DEFAULT 0,
    DestFlightRows          INT UNSIGNED NOT NULL DEFAULT 0,
    TotalOccurrences        INT UNSIGNED NOT NULL DEFAULT 0,
    HistoricalAliasValidTo DATE NULL,
    CreatedAt               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                                  ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (BTSAirportCode),
    KEY ix_map_bts_airport_key (AirportKey),
    KEY ix_map_bts_ourairports_id (OurAirportsID),
    CONSTRAINT fk_map_bts_airport_dim_airport
        FOREIGN KEY (AirportKey, OurAirportsID)
        REFERENCES Dim_Airport (AirportKey, OurAirportsID)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_map_bts_airport_code_length
        CHECK (CHAR_LENGTH(BTSAirportCode) = 3)
) ENGINE=InnoDB
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='BTS airport IATA code to canonical OurAirports dimension mapping';

/* ---------------------------------------------------------------------------
   6. Validation output
--------------------------------------------------------------------------- */
SELECT
    DATABASE() AS CurrentDatabase,
    VERSION() AS MySQLVersion;

SELECT
    'Dim_Date' AS TableName,
    COUNT(*) AS RowCount,
    MIN(CalendarDate) AS MinCalendarDate,
    MAX(CalendarDate) AS MaxCalendarDate,
    SUM(IsUnknown = 1) AS UnknownRows
FROM Dim_Date;

SELECT
    'Dim_Time' AS TableName,
    COUNT(*) AS RowCount,
    MIN(MinuteOfDay) AS MinMinuteOfDay,
    MAX(MinuteOfDay) AS MaxMinuteOfDay,
    SUM(IsEndOfDay = 1) AS EndOfDayRows,
    SUM(IsUnknown = 1) AS UnknownRows
FROM Dim_Time;

SELECT
    TABLE_NAME,
    ENGINE,
    TABLE_COLLATION
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
      'Dim_Date',
      'Dim_Time',
      'Dim_Airline',
      'Dim_Airport',
      'Map_BTS_Airport'
  )
ORDER BY TABLE_NAME;

/* Expected after first clean execution:
   Dim_Date       = 370 rows (369 dates + Unknown)
   Dim_Time       = 1,442 rows (1,441 minutes incl. 24:00 + Unknown)
   Dim_Airline    = 0 rows (loaded in a later ETL step)
   Dim_Airport    = 0 rows (loaded in a later ETL step)
   Map_BTS_Airport= 0 rows (loaded after Dim_Airport)
*/
