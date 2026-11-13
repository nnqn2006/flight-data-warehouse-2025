# Flight DW 2025 — Gold Layer SQL Views

## Contents
- `01_create_gold_views.sql`: creates six aggregated logical views from `fact_flight` and conformed dimensions.
- `02_validate_gold_views.sql`: metadata-only check that the views exist; it does not scan the fact table.

## Scope and prerequisites
- MySQL 8.0+.
- Database: `flight_dw_2025`.
- Source: BTS `fact_flight` for calendar year 2025; dimensions `dim_date`, `dim_airline`, `dim_airport`, `dim_time`.
- The script uses the column names documented in `DW_Data_Dictionary.xlsx` / `INFORMATION_SCHEMA.COLUMNS` metadata provided for this project.
- No DML is performed. No RAW, STAGING, CLEAN, Fact, Dimension, or Bridge rows are changed.

## Views
1. `vw_gold_flight_kpi` — one row of overall 2025 KPIs.
2. `vw_gold_monthly_summary` — monthly volume, cancellation/diversion counts, departure/arrival delay rates and signed average delays.
3. `vw_gold_airline_performance` — airline-level performance.
4. `vw_gold_airport_role_performance` — airport performance by ORIGIN and DESTINATION role.
5. `vw_gold_departure_hour_performance` — scheduled departure-hour profile; excludes Unknown time and preserves the 24:00 sentinel separately.
6. `vw_gold_monthly_airline_performance` — monthly-by-airline matrix/heatmap source.

## Metric definitions
- Departure eligibility: `Cancelled = 0` and `DepDel15 IS NOT NULL`.
- Delayed departure: `Cancelled = 0` and `DepDel15 = 1`.
- Arrival eligibility: `Cancelled = 0`, `Diverted = 0`, and `ArrDel15 IS NOT NULL`.
- Delayed arrival: `Cancelled = 0`, `Diverted = 0`, and `ArrDel15 = 1`.
- Signed average delay uses `DepDelay` / `ArrDelay`, so early flights (negative minutes) remain negative. Do not replace with non-negative `DepDelayMinutes` / `ArrDelayMinutes` when the intended KPI is signed average delay.
- Positive delay totals use non-negative delay-minute fields.
- `FlightCount` is used as the additive flight measure; `FlightRows` is also exposed in the overall/monthly/airline-level views for reconciliation.

## Run order
1. In MySQL Workbench, select `flight_dw_2025`.
2. Open and execute `01_create_gold_views.sql` once.
3. Open and execute `02_validate_gold_views.sql`.
4. Expected metadata result: six view rows and `GoldViewsCreated = 6` (assuming no other `vw_gold_%` views already existed).

## Performance note
These are logical views, not materialized tables. Creating them is metadata work; querying a view performs its aggregation against `fact_flight` (about 7 million rows). Avoid repeatedly running all views while the Bridge loader is active. In Power BI, start with `vw_gold_monthly_summary`, `vw_gold_airline_performance`, `vw_gold_airport_role_performance`, and `vw_gold_departure_hour_performance`; add other views only when needed.

## Weather analysis boundary
No weather view is created here. Weather-exposure analysis must wait until STEP 30E3 (Bridge load) and STEP 30E4 (validation) are PASS, the loader is stopped, and all 48 source partitions are committed and validated. A partial-year Bridge must not be represented as full-year weather coverage.
