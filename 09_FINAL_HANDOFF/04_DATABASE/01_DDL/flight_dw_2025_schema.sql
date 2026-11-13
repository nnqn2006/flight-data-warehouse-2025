-- MySQL dump 10.13  Distrib 8.0.46, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: flight_dw_2025
-- ------------------------------------------------------
-- Server version	8.0.46

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bridge_flight_weather_exposure`
--

DROP TABLE IF EXISTS `bridge_flight_weather_exposure`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bridge_flight_weather_exposure` (
  `FlightKey` bigint unsigned NOT NULL,
  `ExposureType` varchar(32) NOT NULL,
  `SegmentSequence` tinyint unsigned NOT NULL DEFAULT '1',
  `SourceFile` varchar(32) NOT NULL,
  `SourceMonth` char(2) NOT NULL,
  `SourceRowNumber` int unsigned NOT NULL,
  `ExposureAirportCode` char(3) NOT NULL,
  `AirportKey` int unsigned NOT NULL,
  `OurAirportsID` int unsigned NOT NULL,
  `TimezoneIANA` varchar(64) NOT NULL,
  `EventTimestampUTC` datetime DEFAULT NULL COMMENT 'UTC by convention; NULL when timestamp not approved',
  `WeatherHourUTC` datetime DEFAULT NULL COMMENT 'UTC floor-to-hour; NULL when unavailable',
  `MinutesAfterWeatherHour` tinyint unsigned DEFAULT NULL COMMENT '0..59; do not nearest-hour round',
  `TimestampStatus` varchar(96) NOT NULL,
  `WeatherNaturalKey` varchar(64) DEFAULT NULL COMMENT 'OurAirportsID|WeatherHourUTC; populated for matched only',
  `WeatherKey` bigint unsigned DEFAULT NULL COMMENT 'Resolved FK for matched weather hour only',
  `WeatherCoverageStatus` varchar(48) NOT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`FlightKey`,`ExposureType`,`SegmentSequence`),
  KEY `ix_bridge_weather_key` (`WeatherKey`),
  KEY `ix_bridge_airport_coverage` (`AirportKey`,`WeatherCoverageStatus`),
  KEY `ix_bridge_exposure_type_status` (`ExposureType`,`WeatherCoverageStatus`),
  KEY `ix_bridge_source_lineage` (`SourceFile`,`SourceRowNumber`),
  KEY `fk_bridge_airport` (`AirportKey`,`OurAirportsID`),
  CONSTRAINT `fk_bridge_airport` FOREIGN KEY (`AirportKey`, `OurAirportsID`) REFERENCES `dim_airport` (`AirportKey`, `OurAirportsID`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_bridge_flight` FOREIGN KEY (`FlightKey`) REFERENCES `fact_flight` (`FlightKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_bridge_weather` FOREIGN KEY (`WeatherKey`) REFERENCES `fact_airport_weather_hourly` (`WeatherKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `chk_bridge_exposure_type` CHECK ((`ExposureType` in (_utf8mb4'SCHEDULED_DEPARTURE',_utf8mb4'SCHEDULED_ARRIVAL',_utf8mb4'ACTUAL_DEPARTURE',_utf8mb4'ACTUAL_ARRIVAL'))),
  CONSTRAINT `chk_bridge_minutes_after_hour` CHECK (((`MinutesAfterWeatherHour` is null) or (`MinutesAfterWeatherHour` between 0 and 59))),
  CONSTRAINT `chk_bridge_segment_sequence` CHECK ((`SegmentSequence` >= 1)),
  CONSTRAINT `chk_bridge_source_month` CHECK ((`SourceMonth` between _utf8mb4'01' and _utf8mb4'12')),
  CONSTRAINT `chk_bridge_weather_key_status` CHECK ((((`WeatherCoverageStatus` = _utf8mb4'MATCHED_WEATHER_HOUR') and (`WeatherNaturalKey` is not null) and (`WeatherKey` is not null)) or ((`WeatherCoverageStatus` <> _utf8mb4'MATCHED_WEATHER_HOUR') and (`WeatherNaturalKey` is null) and (`WeatherKey` is null))))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC COMMENT='Flight x event exposure bridge; retains matched, unmatched and separately handled rows';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_airline`
--

DROP TABLE IF EXISTS `dim_airline`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_airline` (
  `AirlineKey` int unsigned NOT NULL AUTO_INCREMENT,
  `DOT_ID_Reporting_Airline` int unsigned NOT NULL,
  `ReportingAirlineCode` char(2) NOT NULL,
  `IATAReportingAirlineCode` char(2) NOT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `UpdatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`AirlineKey`),
  UNIQUE KEY `uq_dim_airline_dot_id` (`DOT_ID_Reporting_Airline`),
  KEY `ix_dim_airline_reporting_code` (`ReportingAirlineCode`),
  KEY `ix_dim_airline_iata_code` (`IATAReportingAirlineCode`),
  CONSTRAINT `chk_dim_airline_dot_id_positive` CHECK ((`DOT_ID_Reporting_Airline` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Airline dimension; DOT Airline ID is the natural business key';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_airport`
--

DROP TABLE IF EXISTS `dim_airport`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_airport` (
  `AirportKey` int unsigned NOT NULL AUTO_INCREMENT,
  `OurAirportsID` int unsigned NOT NULL,
  `Ident` varchar(8) NOT NULL,
  `AirportType` varchar(20) NOT NULL,
  `AirportName` varchar(150) NOT NULL,
  `LatitudeRaw` varchar(32) NOT NULL,
  `LatitudeDeg` decimal(10,7) DEFAULT NULL,
  `LongitudeDeg` decimal(10,7) DEFAULT NULL,
  `ElevationFt` smallint DEFAULT NULL,
  `Continent` char(2) DEFAULT NULL,
  `ISOCountry` char(2) DEFAULT NULL,
  `ISORegion` varchar(8) DEFAULT NULL,
  `Municipality` varchar(80) DEFAULT NULL,
  `ScheduledService` varchar(3) DEFAULT NULL,
  `ICAOCode` varchar(4) DEFAULT NULL,
  `IATACode` char(3) DEFAULT NULL,
  `GPSCode` varchar(4) DEFAULT NULL,
  `LocalCode` varchar(10) DEFAULT NULL,
  `HomeLink` varchar(255) DEFAULT NULL,
  `WikipediaLink` varchar(255) DEFAULT NULL,
  `Keywords` varchar(512) DEFAULT NULL,
  `TimeZoneIANA` varchar(64) DEFAULT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `UpdatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`AirportKey`),
  UNIQUE KEY `uq_dim_airport_ourairports_id` (`OurAirportsID`),
  UNIQUE KEY `uq_dim_airport_ident` (`Ident`),
  UNIQUE KEY `uq_dim_airport_key_natural` (`AirportKey`,`OurAirportsID`),
  KEY `ix_dim_airport_iata` (`IATACode`),
  KEY `ix_dim_airport_icao` (`ICAOCode`),
  KEY `ix_dim_airport_country_region` (`ISOCountry`,`ISORegion`),
  KEY `ix_dim_airport_timezone` (`TimeZoneIANA`),
  CONSTRAINT `chk_dim_airport_latitude` CHECK (((`LatitudeDeg` is null) or (`LatitudeDeg` between -(90) and 90))),
  CONSTRAINT `chk_dim_airport_longitude` CHECK (((`LongitudeDeg` is null) or (`LongitudeDeg` between -(180) and 180)))
) ENGINE=InnoDB AUTO_INCREMENT=86336 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Airport dimension; OurAirports provides canonical descriptive attributes';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_date`
--

DROP TABLE IF EXISTS `dim_date`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_date` (
  `DateKey` int unsigned NOT NULL,
  `CalendarDate` date DEFAULT NULL,
  `CalendarYear` smallint unsigned DEFAULT NULL,
  `CalendarQuarter` tinyint unsigned DEFAULT NULL,
  `MonthNumber` tinyint unsigned DEFAULT NULL,
  `MonthName` varchar(9) DEFAULT NULL,
  `DayOfMonth` tinyint unsigned DEFAULT NULL,
  `DayOfWeekISO` tinyint unsigned DEFAULT NULL COMMENT 'Monday=1 ... Sunday=7',
  `DayName` varchar(9) DEFAULT NULL,
  `DayOfYear` smallint unsigned DEFAULT NULL,
  `ISOYear` smallint unsigned DEFAULT NULL,
  `ISOWeekNumber` tinyint unsigned DEFAULT NULL,
  `IsWeekend` tinyint(1) DEFAULT NULL,
  `IsLeapYear` tinyint(1) DEFAULT NULL,
  `IsUnknown` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`DateKey`),
  UNIQUE KEY `uq_dim_date_calendar_date` (`CalendarDate`),
  KEY `ix_dim_date_year_month` (`CalendarYear`,`MonthNumber`),
  KEY `ix_dim_date_iso_year_week` (`ISOYear`,`ISOWeekNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Calendar dimension; DateKey uses YYYYMMDD, 0 is Unknown';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_time`
--

DROP TABLE IF EXISTS `dim_time`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_time` (
  `TimeKey` smallint unsigned NOT NULL,
  `MinuteOfDay` smallint unsigned DEFAULT NULL,
  `TimeValue` time DEFAULT NULL COMMENT 'NULL for Unknown and 24:00 sentinel',
  `TimeLabel` varchar(8) NOT NULL,
  `HourOfDay` tinyint unsigned DEFAULT NULL,
  `MinuteOfHour` tinyint unsigned DEFAULT NULL,
  `IsEndOfDay` tinyint(1) NOT NULL DEFAULT '0',
  `IsUnknown` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`TimeKey`),
  UNIQUE KEY `uq_dim_time_minute_of_day` (`MinuteOfDay`),
  KEY `ix_dim_time_hour_minute` (`HourOfDay`,`MinuteOfHour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Minute-of-day dimension; includes 24:00 and Unknown members';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_airport_weather_hourly`
--

DROP TABLE IF EXISTS `fact_airport_weather_hourly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_airport_weather_hourly` (
  `WeatherKey` bigint unsigned NOT NULL AUTO_INCREMENT,
  `AirportKey` int unsigned NOT NULL,
  `OurAirportsID` int unsigned NOT NULL,
  `WeatherDateKey` int unsigned NOT NULL,
  `WeatherTimeKey` smallint unsigned NOT NULL,
  `WeatherHourUTC` datetime NOT NULL COMMENT 'UTC hour; DATETIME is stored as UTC by convention',
  `Temperature2m` decimal(6,2) NOT NULL,
  `RelativeHumidity2m` tinyint unsigned NOT NULL,
  `Precipitation` decimal(8,2) NOT NULL,
  `WindSpeed10m` decimal(7,2) NOT NULL,
  `WindDirection10m` smallint unsigned NOT NULL COMMENT 'Preserve 360 as supplied; circular variable',
  `WindGusts10m` decimal(7,2) NOT NULL,
  `CloudCover` tinyint unsigned NOT NULL,
  `WeatherCode` smallint unsigned NOT NULL COMMENT 'Preserve source code 96 and all other values',
  `Latitude` decimal(12,9) NOT NULL COMMENT 'Open-Meteo request coordinate provenance',
  `Longitude` decimal(12,9) NOT NULL COMMENT 'Open-Meteo request coordinate provenance',
  `TimezoneRequested` varchar(16) NOT NULL COMMENT 'Request timezone metadata; expected UTC',
  `WeatherModelRequested` varchar(32) NOT NULL COMMENT 'Requested model metadata, not proof of served model',
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`WeatherKey`),
  UNIQUE KEY `uq_fact_weather_airport_utc` (`OurAirportsID`,`WeatherHourUTC`),
  UNIQUE KEY `uq_fact_weather_airportkey_utc` (`AirportKey`,`WeatherHourUTC`),
  KEY `ix_fact_weather_date_airport` (`WeatherDateKey`,`AirportKey`),
  KEY `ix_fact_weather_time` (`WeatherTimeKey`),
  KEY `fk_fact_weather_airport` (`AirportKey`,`OurAirportsID`),
  CONSTRAINT `fk_fact_weather_airport` FOREIGN KEY (`AirportKey`, `OurAirportsID`) REFERENCES `dim_airport` (`AirportKey`, `OurAirportsID`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_weather_date` FOREIGN KEY (`WeatherDateKey`) REFERENCES `dim_date` (`DateKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_weather_time` FOREIGN KEY (`WeatherTimeKey`) REFERENCES `dim_time` (`TimeKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `chk_fact_weather_cloud_cover` CHECK ((`CloudCover` <= 100)),
  CONSTRAINT `chk_fact_weather_humidity` CHECK ((`RelativeHumidity2m` <= 100)),
  CONSTRAINT `chk_fact_weather_latitude` CHECK ((`Latitude` between -(90) and 90)),
  CONSTRAINT `chk_fact_weather_longitude` CHECK ((`Longitude` between -(180) and 180)),
  CONSTRAINT `chk_fact_weather_wind_direction` CHECK ((`WindDirection10m` <= 360))
) ENGINE=InnoDB AUTO_INCREMENT=3083521 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC COMMENT='One row per canonical airport and UTC hour; expected 3,083,520 rows';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_flight`
--

DROP TABLE IF EXISTS `fact_flight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_flight` (
  `FlightKey` bigint unsigned NOT NULL COMMENT 'Deterministic 1..7,001,619 key from STEP 29C2 order',
  `FlightDateKey` int unsigned NOT NULL,
  `AirlineKey` int unsigned NOT NULL,
  `OriginAirportKey` int unsigned NOT NULL,
  `DestinationAirportKey` int unsigned NOT NULL,
  `CRSDepTimeKey` smallint unsigned NOT NULL DEFAULT '0',
  `DepTimeKey` smallint unsigned NOT NULL DEFAULT '0',
  `CRSArrTimeKey` smallint unsigned NOT NULL DEFAULT '0',
  `ArrTimeKey` smallint unsigned NOT NULL DEFAULT '0',
  `SourceFile` varchar(32) NOT NULL,
  `SourceMonth` char(2) NOT NULL,
  `SourceRowNumber` int unsigned NOT NULL,
  `FlightNumberReportingAirline` varchar(8) NOT NULL,
  `TailNumber` varchar(8) DEFAULT NULL,
  `DepDelay` int DEFAULT NULL COMMENT 'Minutes; signed, early departures are negative',
  `DepDelayMinutes` int unsigned DEFAULT NULL COMMENT 'Non-negative delay minutes',
  `DepDel15` tinyint unsigned DEFAULT NULL,
  `DepartureDelayGroups` smallint DEFAULT NULL,
  `TaxiOut` smallint unsigned DEFAULT NULL,
  `ArrDelay` int DEFAULT NULL COMMENT 'Minutes; signed, early arrivals are negative',
  `ArrDelayMinutes` int unsigned DEFAULT NULL,
  `ArrDel15` tinyint unsigned DEFAULT NULL,
  `ArrivalDelayGroups` smallint DEFAULT NULL,
  `TaxiIn` smallint unsigned DEFAULT NULL,
  `Cancelled` tinyint unsigned NOT NULL,
  `CancellationCode` char(1) DEFAULT NULL,
  `Diverted` tinyint unsigned NOT NULL,
  `CRSElapsedTime` smallint DEFAULT NULL COMMENT 'Signed to preserve source value -99',
  `ActualElapsedTime` smallint unsigned DEFAULT NULL,
  `AirTime` smallint unsigned DEFAULT NULL,
  `FlightCount` tinyint unsigned NOT NULL DEFAULT '1',
  `Distance` smallint unsigned NOT NULL,
  `DistanceGroup` tinyint unsigned NOT NULL,
  `CarrierDelay` int unsigned DEFAULT NULL,
  `WeatherDelay` int unsigned DEFAULT NULL,
  `NASDelay` int unsigned DEFAULT NULL,
  `SecurityDelay` int unsigned DEFAULT NULL,
  `LateAircraftDelay` int unsigned DEFAULT NULL,
  `DivAirportLandings` tinyint unsigned DEFAULT NULL,
  `DivReachedDest` tinyint unsigned DEFAULT NULL,
  `DivActualElapsedTime` smallint DEFAULT NULL COMMENT 'Signed to preserve source negative values',
  `DivArrDelay` smallint DEFAULT NULL COMMENT 'Signed diversion arrival delay',
  `DivDistance` smallint unsigned DEFAULT NULL,
  `DQ_FlightDate_Status` varchar(24) NOT NULL,
  `DQ_CRSDepTime_Status` varchar(24) NOT NULL,
  `DQ_DepTime_Status` varchar(24) NOT NULL,
  `DQ_CRSArrTime_Status` varchar(24) NOT NULL,
  `DQ_ArrTime_Status` varchar(24) NOT NULL,
  `DQ_Cancelled_Status` varchar(24) NOT NULL,
  `DQ_Diverted_Status` varchar(24) NOT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`FlightKey`),
  UNIQUE KEY `uq_fact_flight_source_lineage` (`SourceFile`,`SourceRowNumber`),
  KEY `ix_fact_flight_date_airline` (`FlightDateKey`,`AirlineKey`),
  KEY `ix_fact_flight_origin_date` (`OriginAirportKey`,`FlightDateKey`),
  KEY `ix_fact_flight_destination_date` (`DestinationAirportKey`,`FlightDateKey`),
  KEY `ix_fact_flight_cancel_divert_date` (`Cancelled`,`Diverted`,`FlightDateKey`),
  KEY `fk_fact_flight_airline` (`AirlineKey`),
  KEY `fk_fact_flight_crs_dep_time` (`CRSDepTimeKey`),
  KEY `fk_fact_flight_actual_dep_time` (`DepTimeKey`),
  KEY `fk_fact_flight_crs_arr_time` (`CRSArrTimeKey`),
  KEY `fk_fact_flight_actual_arr_time` (`ArrTimeKey`),
  CONSTRAINT `fk_fact_flight_actual_arr_time` FOREIGN KEY (`ArrTimeKey`) REFERENCES `dim_time` (`TimeKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_actual_dep_time` FOREIGN KEY (`DepTimeKey`) REFERENCES `dim_time` (`TimeKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_airline` FOREIGN KEY (`AirlineKey`) REFERENCES `dim_airline` (`AirlineKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_crs_arr_time` FOREIGN KEY (`CRSArrTimeKey`) REFERENCES `dim_time` (`TimeKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_crs_dep_time` FOREIGN KEY (`CRSDepTimeKey`) REFERENCES `dim_time` (`TimeKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_date` FOREIGN KEY (`FlightDateKey`) REFERENCES `dim_date` (`DateKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_destination_airport` FOREIGN KEY (`DestinationAirportKey`) REFERENCES `dim_airport` (`AirportKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_fact_flight_origin_airport` FOREIGN KEY (`OriginAirportKey`) REFERENCES `dim_airport` (`AirportKey`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `chk_fact_flight_arrdel15` CHECK (((`ArrDel15` is null) or (`ArrDel15` in (0,1)))),
  CONSTRAINT `chk_fact_flight_cancelled` CHECK ((`Cancelled` in (0,1))),
  CONSTRAINT `chk_fact_flight_count_positive` CHECK ((`FlightCount` > 0)),
  CONSTRAINT `chk_fact_flight_depdel15` CHECK (((`DepDel15` is null) or (`DepDel15` in (0,1)))),
  CONSTRAINT `chk_fact_flight_diverted` CHECK ((`Diverted` in (0,1))),
  CONSTRAINT `chk_fact_flight_divreached` CHECK (((`DivReachedDest` is null) or (`DivReachedDest` in (0,1)))),
  CONSTRAINT `chk_fact_flight_source_month` CHECK ((`SourceMonth` between _utf8mb4'01' and _utf8mb4'12'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC COMMENT='One row per BTS 2025 flight record; FlightKey reproduces STEP 29C2 key order';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `map_bts_airport`
--

DROP TABLE IF EXISTS `map_bts_airport`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `map_bts_airport` (
  `BTSAirportCode` char(3) NOT NULL,
  `AirportKey` int unsigned NOT NULL,
  `OurAirportsID` int unsigned NOT NULL,
  `MatchStatus` varchar(30) NOT NULL,
  `OriginalMatchStatus` varchar(30) DEFAULT NULL,
  `ResolvedMatchStatus` varchar(30) NOT NULL,
  `MappingMethod` varchar(40) NOT NULL,
  `OriginFlightRows` int unsigned NOT NULL DEFAULT '0',
  `DestFlightRows` int unsigned NOT NULL DEFAULT '0',
  `TotalOccurrences` int unsigned NOT NULL DEFAULT '0',
  `HistoricalAliasValidTo` date DEFAULT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `UpdatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`BTSAirportCode`),
  KEY `ix_map_bts_airport_key` (`AirportKey`),
  KEY `ix_map_bts_ourairports_id` (`OurAirportsID`),
  KEY `fk_map_bts_airport_dim_airport` (`AirportKey`,`OurAirportsID`),
  CONSTRAINT `fk_map_bts_airport_dim_airport` FOREIGN KEY (`AirportKey`, `OurAirportsID`) REFERENCES `dim_airport` (`AirportKey`, `OurAirportsID`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `chk_map_bts_airport_code_length` CHECK ((char_length(`BTSAirportCode`) = 3))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='BTS airport IATA code to canonical OurAirports dimension mapping';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping events for database 'flight_dw_2025'
--

--
-- Dumping routines for database 'flight_dw_2025'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-11-13 16:11:58
