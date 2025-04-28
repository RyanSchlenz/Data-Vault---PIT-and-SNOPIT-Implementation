# Data Vault: Point-in-Time (PIT) and SNOPIT Tables

## Modules

- [1. Introduction](#1-introduction)
- [2. PIT Table Types](#2-pit-table-types)
- [3. AS_OF_DATE Tables](#3-as_of_date-tables)
- [4. Standard PIT Tables](#4-standard-pit-tables)
- [5. SNOPIT Tables](#5-snopit-tables)
- [6. Implementation Examples](#6-implementation-examples)
- [7. Information Mart Integration](#7-information-mart-integration)
- [8. Best Practices](#8-best-practices)

## 1. Introduction

Point-in-Time (PIT) tables are specialized structures within the Data Vault methodology that serve as temporal indices or "snapshots" of data at specific moments. They function like a GPS system for your data warehouse, allowing you to navigate to the correct version of data as it existed at a particular point in time.

### Key Benefits

- **Performance Optimization**: Pre-calculating temporal relationships dramatically improves query performance
- **Simplified Querying**: Reduces the complexity of SQL joins needed to retrieve historical data
- **Consistent Snapshots**: Ensures consistent point-in-time views across multiple related entities
- **Business Vault Foundation**: Forms a critical component in the Business Vault layer of a Data Vault architecture

## 2. PIT Table Types

### Standard PIT Tables

PIT tables track both hash keys and load dates, serving as lookup mechanisms to determine which record was active at a specific point in time. These tables commonly come in different temporal granularities:

- **Daily PIT**: Contains snapshots for each day
- **Weekly PIT**: Contains snapshots for each week
- **Monthly PIT**: Contains snapshots for each month
- **Current PIT**: Contains only the most recent snapshot

### SNOPIT Tables

SNOPIT (Sequence Number Only Point-in-Time) tables are an optimization of standard PIT tables that use sequence numbers instead of load dates. This simplifies joins by requiring comparison of only one column (sequence number) instead of two columns (hash key and load date), making queries more efficient.

## 3. AS_OF_DATE Tables

These serve as date dimensions for PIT queries, containing dates and various date attributes for time-based analysis.

### Structure

```sql
CREATE TABLE queryassistance.as_of_date (
    as_of DATE NOT NULL,              -- Primary date field for point-in-time reference
    year SMALLINT NOT NULL,           -- Year component
    month SMALLINT NOT NULL,          -- Month number (1-12)
    month_name CHAR(10),              -- Month name 
    day_of_month SMALLINT NOT NULL,   -- Day of month (1-31)
    day_of_week VARCHAR(9) NOT NULL,  -- Day of week (0-6)
    day_name CHAR(10),                -- Name of day
    week_of_year SMALLINT NOT NULL,   -- Week number within year
    day_of_year SMALLINT NOT NULL,    -- Day number within year
    month_lastday SMALLINT NOT NULL,  -- Flag for last day of month
    week_lastday SMALLINT NOT NULL,   -- Flag for last day of week
    week_firstday SMALLINT NOT NULL   -- Flag for first day of week
);
```

### Population Example

```sql
-- Generate a sequence of dates for the AS_OF_DATE table
WITH date_generator AS (
    SELECT dateadd(day, seq4(0), '2022-01-01') AS as_of
    FROM table(generator(rowcount=>181))
)
SELECT as_of,
    year(as_of),
    month(as_of),
    monthname(as_of),
    day(as_of),
    dayofweek(as_of),
    dayname(as_of),
    weekofyear(as_of),
    dayofyear(as_of),
    CASE WHEN last_day(as_of) = as_of THEN 1 ELSE 0 END AS month_lastday,
    CASE WHEN last_day(as_of, 'week') = as_of THEN 1 ELSE 0 END AS week_lastday,
    CASE WHEN dayname(as_of) = 'Mon' THEN 1 ELSE 0 END AS week_firstday
FROM date_generator;
```

## 4. Standard PIT Tables

### Structure

A typical PIT table contains:
- Hash keys from hubs and links
- Load dates corresponding to when records were loaded
- Snapshot date indicating when the PIT snapshot was created

```sql
CREATE TABLE queryassistance.pit_gamelogs_daily (
    dv_hashkey_hub_account BINARY(20),    -- Hash key for entity
    LOG_FILE_NAME VARCHAR(100),           -- Source identifier
    LOG_FILE_ROW_ID NUMBER(18,0),         -- Row identifier
    LOAD_LTZ TIMESTAMP_LTZ(0),            -- Load timestamp
    DATETIME_ISO8601 TIMESTAMP_NTZ(9),    -- Event timestamp
    USER_EVENT VARCHAR(25),               -- Event type
    USER_LOGIN VARCHAR(100),              -- User identifier
    IP_ADDRESS VARCHAR(100),              -- IP address
    snapshotdate DATE                     -- Snapshot date
);
```

### Population Method

PIT tables are typically populated by:
1. Selecting hash keys and load dates from satellites
2. Computing a snapshot date (often current date for daily runs)
3. Inserting the records into the PIT table

```sql
INSERT INTO queryassistance.pit_gamelogs_daily (
    snapshotdate,
    dv_hashkey_hub_account,
    log_file_name,
    load_ltz
)
SELECT 
    CURRENT_DATE AS snapshotdate,
    MD5(LOG_FILE_NAME) AS dv_hashkey_hub_account,
    LOG_FILE_NAME,
    LOAD_LTZ
FROM source_data_table;
```

## 5. SNOPIT Tables

### Structure

Similar to PIT tables but with sequence numbers:

```sql
CREATE TABLE queryassistance.snopit_gamelogs_daily (
    dv_hashkey_hub_account BINARY(20),    -- Hash key for entity
    sequence_number INT NOT NULL,         -- Sequence number for temporal tracking
    LOG_FILE_NAME VARCHAR(100),           -- Source identifier
    LOG_FILE_ROW_ID NUMBER(18,0),         -- Row identifier
    LOAD_LTZ TIMESTAMP_LTZ(0),            -- Load timestamp
    DATETIME_ISO8601 TIMESTAMP_NTZ(9),    -- Event timestamp
    USER_EVENT VARCHAR(25),               -- Event type
    USER_LOGIN VARCHAR(100),              -- User identifier
    IP_ADDRESS VARCHAR(100),              -- IP address
    snapshotdate DATE                     -- Snapshot date
);
```

### Population Method

Similar to PIT tables but adding sequence numbers:

```sql
INSERT INTO queryassistance.snopit_gamelogs_daily (
    snapshotdate,
    dv_hashkey_hub_account,
    sequence_number,
    log_file_name,
    load_ltz
)
SELECT 
    CURRENT_DATE AS snapshotdate,
    MD5(LOG_FILE_NAME) AS dv_hashkey_hub_account,
    ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) AS sequence_number,
    LOG_FILE_NAME,
    LOAD_LTZ
FROM source_data_table;
```

### Key Comparison: PIT vs SNOPIT

The main difference between PIT and SNOPIT tables is in how they are joined with other tables:

**PIT Join Example:**
```sql
-- PIT tables require two-column joins
JOIN satellite_table
  ON pit.dv_hashkey_hub_account = satellite_table.dv_hashkey_hub_account
  AND pit.load_ltz = satellite_table.load_ltz
```

**SNOPIT Join Example:**
```sql
-- SNOPIT tables simplify to one-column joins (after the hash key)
JOIN satellite_table
  ON snopit.dv_hashkey_hub_account = satellite_table.dv_hashkey_hub_account
  AND snopit.sequence_number = satellite_table.sequence_number
```

This optimization can significantly improve query performance, especially for complex joins.

## 6. Implementation Examples

### Example 1: Creating a Daily PIT Table

```sql
-- Step 1: Create PIT table structure
CREATE OR REPLACE TABLE queryassistance.pit_gamelogs_daily (
    dv_hashkey_hub_account BINARY(20),
    LOG_FILE_NAME VARCHAR(100),
    LOAD_LTZ TIMESTAMP_LTZ(0),
    snapshotdate DATE
);

-- Step 2: Populate with current day's snapshot
INSERT INTO queryassistance.pit_gamelogs_daily
SELECT 
    MD5(LOG_FILE_NAME) AS dv_hashkey_hub_account,
    LOG_FILE_NAME,
    LOAD_LTZ,
    CURRENT_DATE AS snapshotdate
FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;
```

### Example 2: Creating a Weekly PIT Table

```sql
CREATE OR REPLACE TABLE queryassistance.pit_gamelogs_weekly AS
SELECT *
FROM queryassistance.pit_gamelogs_daily
WHERE snapshotdate IN (
    SELECT as_of
    FROM queryassistance.as_of_date
    WHERE week_lastday = 1  -- Only include dates that are the last day of the week
);
```

### Example 3: Creating a SNOPIT Table

```sql
-- Step 1: Create SNOPIT table structure
CREATE OR REPLACE TABLE queryassistance.snopit_gamelogs_daily (
    dv_hashkey_hub_account BINARY(20),
    sequence_number INT NOT NULL,
    LOG_FILE_NAME VARCHAR(100),
    LOAD_LTZ TIMESTAMP_LTZ(0),
    snapshotdate DATE
);

-- Step 2: Populate with sequence numbers
INSERT INTO queryassistance.snopit_gamelogs_daily
SELECT 
    MD5(LOG_FILE_NAME) AS dv_hashkey_hub_account,
    ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) AS sequence_number,
    LOG_FILE_NAME,
    LOAD_LTZ,
    CURRENT_DATE AS snapshotdate
FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;
```

## 7. Information Mart Integration

PIT and SNOPIT tables are most valuable when used to create business-friendly views in information marts.

### Standard PIT-Based Information Mart

```sql
CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_pit AS 
SELECT 
    pit.snapshotdate,
    pit.log_file_name AS dv_hashkey_hub_account,
    pit.log_file_name,
    s1.LOG_FILE_ROW_ID AS log_file_row_id,
    s1.DATETIME_ISO8601 AS datetime_iso8601,
    s1.IP_ADDRESS AS ip_address,
    s2.USER_EVENT AS user_event,
    s2.USER_LOGIN AS user_login
FROM 
    queryassistance.pit_gamelogs_daily pit
INNER JOIN AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS s1
    ON pit.log_file_name = s1.LOG_FILE_NAME
    AND pit.LOAD_LTZ = s1.LOAD_LTZ
INNER JOIN AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY s2
    ON pit.log_file_name = s2.LOG_FILE_NAME
    AND pit.LOAD_LTZ = s2.LOAD_LTZ;
```

### SNOPIT-Based Information Mart

```sql
CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_snopit AS 
SELECT 
    snopit.snapshotdate,
    snopit.dv_hashkey_hub_account,
    snopit.log_file_name,
    snopit.sequence_number,
    s1.LOG_FILE_ROW_ID AS log_file_row_id,
    s1.DATETIME_ISO8601 AS datetime_iso8601,
    s1.IP_ADDRESS AS ip_address,
    s2.USER_EVENT AS user_event,
    s2.USER_LOGIN AS user_login
FROM 
    queryassistance.snopit_gamelogs_daily snopit
INNER JOIN (
    SELECT 
        LOG_FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) AS sequence_number,
        LOG_FILE_ROW_ID,
        DATETIME_ISO8601,
        IP_ADDRESS,
        LOAD_LTZ
    FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
) s1
    ON MD5(s1.LOG_FILE_NAME) = snopit.dv_hashkey_hub_account
    AND s1.sequence_number = snopit.sequence_number
INNER JOIN (
    SELECT 
        LOG_FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) AS sequence_number,
        USER_EVENT,
        USER_LOGIN,
        LOAD_LTZ
    FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY
) s2
    ON MD5(s2.LOG_FILE_NAME) = snopit.dv_hashkey_hub_account
    AND s2.sequence_number = snopit.sequence_number;
```

### Parameterized Point-in-Time Queries

You can also create functions to simplify point-in-time access for business users:

```sql
CREATE OR REPLACE FUNCTION AGS_GAME_AUDIENCE.INFORMATION_MARTS.fn_game_logs_as_of(as_of_date DATE)
RETURNS TABLE (
    as_of DATE,
    dv_hashkey_hub_account BINARY(20),
    log_file_name VARCHAR(100),
    log_file_row_id NUMBER(18,0),
    datetime_iso8601 TIMESTAMP_NTZ(9),
    ip_address VARCHAR(100),
    user_event VARCHAR(25),
    user_login VARCHAR(100)
)
AS
$
    SELECT 
        :as_of_date AS as_of,
        pit.dv_hashkey_hub_account,
        pit.log_file_name,
        s1.LOG_FILE_ROW_ID,
        s1.DATETIME_ISO8601,
        s1.IP_ADDRESS,
        s2.USER_EVENT,
        s2.USER_LOGIN
    FROM 
        queryassistance.pit_gamelogs_daily pit
    INNER JOIN AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS s1
        ON pit.log_file_name = s1.LOG_FILE_NAME
        AND pit.LOAD_LTZ = s1.LOAD_LTZ
    INNER JOIN AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY s2
        ON pit.log_file_name = s2.LOG_FILE_NAME
        AND pit.LOAD_LTZ = s2.LOAD_LTZ
    WHERE 
        pit.snapshotdate = :as_of_date
$;
```

## 8. Best Practices

### Performance Optimization

1. **Granularity Selection**: Create PIT tables at appropriate time granularities based on business requirements (daily, weekly, monthly)
2. **Consider SNOPIT**: Use SNOPIT tables for query optimization when applicable
3. **Indexing Strategy**: Implement proper indexing on hash keys and sequence numbers
4. **Partitioning**: Consider partitioning large PIT tables by snapshot date

### Maintenance

1. **Refresh Cycles**: Maintain PIT tables through regular refresh cycles
2. **Retention Policy**: Implement a retention policy for historical PIT snapshots
3. **Audit Trail**: Maintain metadata about when PIT tables were last refreshed

### Design Principles

1. **Consistent Hash Keys**: Ensure consistent hash key generation across the data vault
2. **Documentation**: Document the temporal semantics clearly for business users
3. **Minimize Joins**: Design information mart views to minimize the number of joins

### Testing and Validation

1. **Point-in-Time Testing**: Test queries for multiple points in time to ensure consistency
2. **Sequence Validation**: Verify sequence numbers have no gaps
3. **Performance Testing**: Benchmark query performance with and without PIT/SNOPIT tables

### Business Integration

1. **Business Calendars**: Align PIT table snapshots with business calendars (fiscal periods)
2. **Date Dimension Integration**: Integrate with date dimension tables for enhanced reporting
3. **Training**: Educate business users on how to leverage PIT tables for historical analysis
