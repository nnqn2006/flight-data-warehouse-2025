/* STEP 30C.0 - Preserve the full resolved mapping status value.
   MATCHED_HISTORICAL_ALIAS is 24 characters and must not be truncated.
   This alters only the empty/controlled target dimension table schema;
   it does not delete or rewrite source files or rows.
*/
USE flight_dw_2025;

ALTER TABLE Map_BTS_Airport
    MODIFY COLUMN MatchStatus VARCHAR(30) NOT NULL;

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'Map_BTS_Airport'
  AND COLUMN_NAME IN ('MatchStatus', 'ResolvedMatchStatus')
ORDER BY ORDINAL_POSITION;
