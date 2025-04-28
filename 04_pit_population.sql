/*
Script: 04_pit_population.sql
Purpose: Populates the PIT (Point-In-Time) table with data
         This script demonstrates how to create daily snapshots
*/

-- Step 1: Populate the PIT table with current day's snapshot
-- This inserts data from the source table into the PIT table
INSERT INTO queryassistance.pit_gamelogs_daily (
    snapshotdate,
    dv_hashkey_hub_account,
    log_file_name,
    log_file_row_id,
    load_ltz
)
SELECT 
    CURRENT_DATE as snapshotdate,   -- Set snapshot date to current date
    MD5(LOG_FILE_NAME) as dv_hashkey_hub_account,  -- Generate hash key using MD5
    LOG_FILE_NAME,                  -- Copy the log file name
    LOG_FILE_ROW_ID,                -- Copy the log file row ID
    LOAD_LTZ                        -- Copy the load timestamp
FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

-- Step 2: Verify the data was inserted correctly
SELECT 
    COUNT(*) as record_count, 
    MIN(snapshotdate) as snapshot_date,
    COUNT(DISTINCT dv_hashkey_hub_account) as distinct_entities
FROM queryassistance.pit_gamelogs_daily;

/*
Step 3: Optional - Create additional temporal granularity PIT tables
        You can create weekly, monthly, or current PIT tables based on your needs
*/

-- Example: Create a weekly PIT table by filtering the daily PIT
CREATE OR REPLACE TABLE queryassistance.pit_gamelogs_weekly AS
SELECT *
FROM queryassistance.pit_gamelogs_daily
WHERE snapshotdate IN (
    SELECT as_of
    FROM queryassistance.as_of_date
    WHERE week_lastday = 1  -- Only include dates that are the last day of the week
);

-- Example: Create a monthly PIT table
CREATE OR REPLACE TABLE queryassistance.pit_gamelogs_monthly AS
SELECT *
FROM queryassistance.pit_gamelogs_daily
WHERE snapshotdate IN (
    SELECT as_of
    FROM queryassistance.as_of_date
    WHERE month_lastday = 1  -- Only include dates that are the last day of the month
);

-- Example: Create a current PIT table (most recent snapshot only)
CREATE OR REPLACE TABLE queryassistance.pit_gamelogs_current AS
SELECT *
FROM queryassistance.pit_gamelogs_daily
WHERE snapshotdate = (
    SELECT MAX(snapshotdate)
    FROM queryassistance.pit_gamelogs_daily
);
