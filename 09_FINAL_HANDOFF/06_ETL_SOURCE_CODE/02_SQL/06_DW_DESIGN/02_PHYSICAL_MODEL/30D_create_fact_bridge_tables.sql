/*
===============================================================================
FLIGHT DATA WAREHOUSE 2025 - STEP 30D
Physical model: Fact and Flight-Weather Bridge table DDL
Target: MySQL 8.0.46 / InnoDB / utf8mb4

Prerequisites:
  - STEP 30B Dimension DDL completed.
  - STEP 30C Dimension ETL completed.
  - Map_BTS_Airport.MatchStatus expanded to VARCHAR(30).

Safety:
  - No DROP, TRUNCATE, DELETE, or UPDATE statements.
  - CREATE TABLE IF NOT EXISTS only.
  - No source files are read or modified by this SQL.
  - This step creates empty target tables; it does NOT load Fact/Bridge rows.

Grains:
  Fact_Flight:
    One row per BTS monthly source record. FlightKey is deterministic and must
    reproduce STEP 29C2 order exactly (1..7,001,619) to preserve Bridge joins.

  Fact_Airport_Weather_Hourly:
    One row per OurAirportsID x UTC hour (3,083,520 expected rows).

  Bridge_Flight_Weather_Exposure:
    One row per FlightKey x ExposureType x SegmentSequence
    (28,006,476 expected rows; 4 event types per BTS flight).

UTC convention:
  DATETIME columns ending in UTC contain UTC clock values without MySQL
  session-time-zone conversion. Do not change these values to local time.

Time dimension convention:
  Dim_Time.TimeKey = MinuteOfDay + 1; TimeKey 0 is Unknown.
  HHMM 2400 maps to TimeKey 1441 (MinuteOfDay 1440 sentinel).
  Missing/unapproved source time maps to TimeKey 0.
===============================================================================
*/

USE flight_dw_2025;
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;

/* ---------------------------------------------------------------------------
   1. Fact_Flight
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Fact_Flight (
    FlightKey                       BIGINT UNSIGNED NOT NULL
                                    COMMENT 'Deterministic 1..7,001,619 key from STEP 29C2 order',

    FlightDateKey                   INT UNSIGNED NOT NULL,
    AirlineKey                      INT UNSIGNED NOT NULL,
    OriginAirportKey                INT UNSIGNED NOT NULL,
    DestinationAirportKey           INT UNSIGNED NOT NULL,

    CRSDepTimeKey                   SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    DepTimeKey                      SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    CRSArrTimeKey                   SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    ArrTimeKey                      SMALLINT UNSIGNED NOT NULL DEFAULT 0,

    SourceFile                      VARCHAR(32) NOT NULL,
    SourceMonth                     CHAR(2) NOT NULL,
    SourceRowNumber                 INT UNSIGNED NOT NULL,

    FlightNumberReportingAirline    VARCHAR(8) NOT NULL,
    TailNumber                      VARCHAR(8) NULL,

    DepDelay                        INT NULL COMMENT 'Minutes; signed, early departures are negative',
    DepDelayMinutes                 INT UNSIGNED NULL COMMENT 'Non-negative delay minutes',
    DepDel15                        TINYINT UNSIGNED NULL,
    DepartureDelayGroups            SMALLINT NULL,
    TaxiOut                         SMALLINT UNSIGNED NULL,

    ArrDelay                        INT NULL COMMENT 'Minutes; signed, early arrivals are negative',
    ArrDelayMinutes                 INT UNSIGNED NULL,
    ArrDel15                        TINYINT UNSIGNED NULL,
    ArrivalDelayGroups              SMALLINT NULL,
    TaxiIn                          SMALLINT UNSIGNED NULL,

    Cancelled                       TINYINT UNSIGNED NOT NULL,
    CancellationCode                CHAR(1) NULL,
    Diverted                        TINYINT UNSIGNED NOT NULL,

    CRSElapsedTime                  SMALLINT NULL COMMENT 'Signed to preserve source value -99',
    ActualElapsedTime               SMALLINT UNSIGNED NULL,
    AirTime                         SMALLINT UNSIGNED NULL,
    FlightCount                     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    Distance                        SMALLINT UNSIGNED NOT NULL,
    DistanceGroup                   TINYINT UNSIGNED NOT NULL,

    CarrierDelay                    INT UNSIGNED NULL,
    WeatherDelay                    INT UNSIGNED NULL,
    NASDelay                        INT UNSIGNED NULL,
    SecurityDelay                   INT UNSIGNED NULL,
    LateAircraftDelay               INT UNSIGNED NULL,

    DivAirportLandings              TINYINT UNSIGNED NULL,
    DivReachedDest                  TINYINT UNSIGNED NULL,
    DivActualElapsedTime             SMALLINT NULL COMMENT 'Signed to preserve source negative values',
    DivArrDelay                     SMALLINT NULL COMMENT 'Signed diversion arrival delay',
    DivDistance                     SMALLINT UNSIGNED NULL,

    DQ_FlightDate_Status            VARCHAR(24) NOT NULL,
    DQ_CRSDepTime_Status            VARCHAR(24) NOT NULL,
    DQ_DepTime_Status               VARCHAR(24) NOT NULL,
    DQ_CRSArrTime_Status            VARCHAR(24) NOT NULL,
    DQ_ArrTime_Status               VARCHAR(24) NOT NULL,
    DQ_Cancelled_Status             VARCHAR(24) NOT NULL,
    DQ_Diverted_Status              VARCHAR(24) NOT NULL,

    CreatedAt                       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (FlightKey),
    UNIQUE KEY uq_fact_flight_source_lineage (SourceFile, SourceRowNumber),
    KEY ix_fact_flight_date_airline (FlightDateKey, AirlineKey),
    KEY ix_fact_flight_origin_date (OriginAirportKey, FlightDateKey),
    KEY ix_fact_flight_destination_date (DestinationAirportKey, FlightDateKey),
    KEY ix_fact_flight_cancel_divert_date (Cancelled, Diverted, FlightDateKey),

    CONSTRAINT fk_fact_flight_date
        FOREIGN KEY (FlightDateKey)
        REFERENCES Dim_Date (DateKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_airline
        FOREIGN KEY (AirlineKey)
        REFERENCES Dim_Airline (AirlineKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_origin_airport
        FOREIGN KEY (OriginAirportKey)
        REFERENCES Dim_Airport (AirportKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_destination_airport
        FOREIGN KEY (DestinationAirportKey)
        REFERENCES Dim_Airport (AirportKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_crs_dep_time
        FOREIGN KEY (CRSDepTimeKey)
        REFERENCES Dim_Time (TimeKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_actual_dep_time
        FOREIGN KEY (DepTimeKey)
        REFERENCES Dim_Time (TimeKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_crs_arr_time
        FOREIGN KEY (CRSArrTimeKey)
        REFERENCES Dim_Time (TimeKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_flight_actual_arr_time
        FOREIGN KEY (ArrTimeKey)
        REFERENCES Dim_Time (TimeKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    CONSTRAINT chk_fact_flight_cancelled CHECK (Cancelled IN (0, 1)),
    CONSTRAINT chk_fact_flight_diverted CHECK (Diverted IN (0, 1)),
    CONSTRAINT chk_fact_flight_depdel15 CHECK (DepDel15 IS NULL OR DepDel15 IN (0, 1)),
    CONSTRAINT chk_fact_flight_arrdel15 CHECK (ArrDel15 IS NULL OR ArrDel15 IN (0, 1)),
    CONSTRAINT chk_fact_flight_divreached CHECK (DivReachedDest IS NULL OR DivReachedDest IN (0, 1)),
    CONSTRAINT chk_fact_flight_source_month CHECK (SourceMonth BETWEEN '01' AND '12'),
    CONSTRAINT chk_fact_flight_count_positive CHECK (FlightCount > 0)
) ENGINE=InnoDB
  ROW_FORMAT=DYNAMIC
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='One row per BTS 2025 flight record; FlightKey reproduces STEP 29C2 key order';

/* ---------------------------------------------------------------------------
   2. Fact_Airport_Weather_Hourly
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Fact_Airport_Weather_Hourly (
    WeatherKey                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    AirportKey                  INT UNSIGNED NOT NULL,
    OurAirportsID               INT UNSIGNED NOT NULL,
    WeatherDateKey              INT UNSIGNED NOT NULL,
    WeatherTimeKey              SMALLINT UNSIGNED NOT NULL,
    WeatherHourUTC              DATETIME NOT NULL COMMENT 'UTC hour; DATETIME is stored as UTC by convention',

    Temperature2m               DECIMAL(6,2) NOT NULL,
    RelativeHumidity2m           TINYINT UNSIGNED NOT NULL,
    Precipitation               DECIMAL(8,2) NOT NULL,
    WindSpeed10m                DECIMAL(7,2) NOT NULL,
    WindDirection10m            SMALLINT UNSIGNED NOT NULL COMMENT 'Preserve 360 as supplied; circular variable',
    WindGusts10m                DECIMAL(7,2) NOT NULL,
    CloudCover                  TINYINT UNSIGNED NOT NULL,
    WeatherCode                 SMALLINT UNSIGNED NOT NULL COMMENT 'Preserve source code 96 and all other values',

    Latitude                    DECIMAL(12,9) NOT NULL COMMENT 'Open-Meteo request coordinate provenance',
    Longitude                   DECIMAL(12,9) NOT NULL COMMENT 'Open-Meteo request coordinate provenance',
    TimezoneRequested           VARCHAR(16) NOT NULL COMMENT 'Request timezone metadata; expected UTC',
    WeatherModelRequested       VARCHAR(32) NOT NULL COMMENT 'Requested model metadata, not proof of served model',

    CreatedAt                   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (WeatherKey),
    UNIQUE KEY uq_fact_weather_airport_utc (OurAirportsID, WeatherHourUTC),
    UNIQUE KEY uq_fact_weather_airportkey_utc (AirportKey, WeatherHourUTC),
    KEY ix_fact_weather_date_airport (WeatherDateKey, AirportKey),
    KEY ix_fact_weather_time (WeatherTimeKey),

    CONSTRAINT fk_fact_weather_airport
        FOREIGN KEY (AirportKey, OurAirportsID)
        REFERENCES Dim_Airport (AirportKey, OurAirportsID)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_weather_date
        FOREIGN KEY (WeatherDateKey)
        REFERENCES Dim_Date (DateKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_fact_weather_time
        FOREIGN KEY (WeatherTimeKey)
        REFERENCES Dim_Time (TimeKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    CONSTRAINT chk_fact_weather_humidity CHECK (RelativeHumidity2m <= 100),
    CONSTRAINT chk_fact_weather_wind_direction CHECK (WindDirection10m <= 360),
    CONSTRAINT chk_fact_weather_cloud_cover CHECK (CloudCover <= 100),
    CONSTRAINT chk_fact_weather_latitude CHECK (Latitude BETWEEN -90 AND 90),
    CONSTRAINT chk_fact_weather_longitude CHECK (Longitude BETWEEN -180 AND 180)
) ENGINE=InnoDB
  ROW_FORMAT=DYNAMIC
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='One row per canonical airport and UTC hour; expected 3,083,520 rows';

/* ---------------------------------------------------------------------------
   3. Bridge_Flight_Weather_Exposure
--------------------------------------------------------------------------- */
CREATE TABLE IF NOT EXISTS Bridge_Flight_Weather_Exposure (
    FlightKey                   BIGINT UNSIGNED NOT NULL,
    ExposureType                VARCHAR(32) NOT NULL,
    SegmentSequence             TINYINT UNSIGNED NOT NULL DEFAULT 1,

    SourceFile                  VARCHAR(32) NOT NULL,
    SourceMonth                 CHAR(2) NOT NULL,
    SourceRowNumber             INT UNSIGNED NOT NULL,

    ExposureAirportCode         CHAR(3) NOT NULL,
    AirportKey                  INT UNSIGNED NOT NULL,
    OurAirportsID               INT UNSIGNED NOT NULL,
    TimezoneIANA                VARCHAR(64) NOT NULL,

    EventTimestampUTC           DATETIME NULL COMMENT 'UTC by convention; NULL when timestamp not approved',
    WeatherHourUTC              DATETIME NULL COMMENT 'UTC floor-to-hour; NULL when unavailable',
    MinutesAfterWeatherHour     TINYINT UNSIGNED NULL COMMENT '0..59; do not nearest-hour round',

    TimestampStatus             VARCHAR(96) NOT NULL,
    WeatherNaturalKey           VARCHAR(64) NULL COMMENT 'OurAirportsID|WeatherHourUTC; populated for matched only',
    WeatherKey                  BIGINT UNSIGNED NULL COMMENT 'Resolved FK for matched weather hour only',
    WeatherCoverageStatus       VARCHAR(48) NOT NULL,

    CreatedAt                   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (FlightKey, ExposureType, SegmentSequence),
    KEY ix_bridge_weather_key (WeatherKey),
    KEY ix_bridge_airport_coverage (AirportKey, WeatherCoverageStatus),
    KEY ix_bridge_exposure_type_status (ExposureType, WeatherCoverageStatus),
    KEY ix_bridge_source_lineage (SourceFile, SourceRowNumber),

    CONSTRAINT fk_bridge_flight
        FOREIGN KEY (FlightKey)
        REFERENCES Fact_Flight (FlightKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_bridge_airport
        FOREIGN KEY (AirportKey, OurAirportsID)
        REFERENCES Dim_Airport (AirportKey, OurAirportsID)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_bridge_weather
        FOREIGN KEY (WeatherKey)
        REFERENCES Fact_Airport_Weather_Hourly (WeatherKey)
        ON UPDATE RESTRICT ON DELETE RESTRICT,

    CONSTRAINT chk_bridge_exposure_type
        CHECK (ExposureType IN (
            'SCHEDULED_DEPARTURE',
            'SCHEDULED_ARRIVAL',
            'ACTUAL_DEPARTURE',
            'ACTUAL_ARRIVAL'
        )),
    CONSTRAINT chk_bridge_segment_sequence CHECK (SegmentSequence >= 1),
    CONSTRAINT chk_bridge_minutes_after_hour
        CHECK (MinutesAfterWeatherHour IS NULL OR MinutesAfterWeatherHour BETWEEN 0 AND 59),
    CONSTRAINT chk_bridge_weather_key_status
        CHECK (
            (WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'
             AND WeatherNaturalKey IS NOT NULL AND WeatherKey IS NOT NULL)
            OR
            (WeatherCoverageStatus <> 'MATCHED_WEATHER_HOUR'
             AND WeatherNaturalKey IS NULL AND WeatherKey IS NULL)
        ),
    CONSTRAINT chk_bridge_source_month CHECK (SourceMonth BETWEEN '01' AND '12')
) ENGINE=InnoDB
  ROW_FORMAT=DYNAMIC
  DEFAULT CHARACTER SET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Flight x event exposure bridge; retains matched, unmatched and separately handled rows';

/* ---------------------------------------------------------------------------
   4. Metadata validation: exactly these three new tables must now exist.
--------------------------------------------------------------------------- */
SELECT
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS AS ApproximateRows,
    TABLE_COLLATION
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
      'Fact_Flight',
      'Fact_Airport_Weather_Hourly',
      'Bridge_Flight_Weather_Exposure'
  )
ORDER BY TABLE_NAME;

/* Exact target row counts should all be zero immediately after DDL. */
SELECT 'Fact_Flight' AS TableName, COUNT(*) AS RowCount FROM Fact_Flight
UNION ALL
SELECT 'Fact_Airport_Weather_Hourly', COUNT(*) FROM Fact_Airport_Weather_Hourly
UNION ALL
SELECT 'Bridge_Flight_Weather_Exposure', COUNT(*) FROM Bridge_Flight_Weather_Exposure;

/*
Expected immediately after STEP 30D DDL:
  3 tables present, all InnoDB.
  Fact_Flight                       = 0 rows
  Fact_Airport_Weather_Hourly       = 0 rows
  Bridge_Flight_Weather_Exposure   = 0 rows

The diversion child fact is intentionally deferred to a separate design pass.
The BTS Div1..Div5 airport identifiers have not yet been validated against a
complete diversion-airport crosswalk; do not assume they all match the current
352 Origin/Destination airport mappings.
*/
