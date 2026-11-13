-- Flight DW 2025 | Read-only schema and constraint inventory
-- Safe to run while Bridge ETL is in progress: reads metadata only.
USE flight_dw_2025;

-- 1. Connection and server context
SELECT
    DATABASE() AS CurrentDatabase,
    VERSION() AS MySQLVersion,
    @@version_comment AS ServerDistribution,
    NOW() AS CapturedAtLocalServerTime;

-- 2. Table inventory. TABLE_ROWS is approximate for InnoDB; do not treat it as exact row count.
SELECT
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS AS ApproximateRows_InnoDB,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS TotalSizeMB,
    TABLE_COLLATION
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- 3. Column inventory and nullability
SELECT
    TABLE_NAME,
    ORDINAL_POSITION,
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_KEY,
    COLUMN_DEFAULT,
    EXTRA,
    COLUMN_COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- 4. Foreign-key mappings (18 column mappings grouped into 15 constraints in the current design)
SELECT
    TABLE_NAME,
    CONSTRAINT_NAME,
    ORDINAL_POSITION,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION;

-- 5. Foreign-key constraint count (expected 15 grouped constraints in the documented schema)
SELECT
    COUNT(DISTINCT CONCAT(TABLE_NAME, '|', CONSTRAINT_NAME)) AS ForeignKeyConstraintCount,
    COUNT(*) AS ForeignKeyColumnMappingCount
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND REFERENCED_TABLE_NAME IS NOT NULL;

-- 6. Referential actions
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    REFERENCED_TABLE_NAME,
    UPDATE_RULE,
    DELETE_RULE
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, CONSTRAINT_NAME;
