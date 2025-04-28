/*
Script: 05_snopit_population.sql
Purpose: Populates the SNOPIT (Sequence Number Only Point-In-Time) table 
         Demonstrates how to generate and use sequence numbers for optimization
*/

-- Step 1: Populate the SNOPIT table with data and sequence numbers
-- This inserts data from the source table and generates sequence numbers
INSERT INTO queryassistance.snopit_gamelogs_daily (
    snapshotdate,
    dv_hashkey_hub_account,
    sequence_number,                  -- Add sequence number
    log_file_name,
    log_file_row_id,
    load_ltz
)
SELECT 
    CURRENT_DATE as snapshotdate,
    MD5(LOG_FILE_NAME) as dv_hashkey_hub_account,
    -- Generate sequence numbers partitioned by the hash key
    -- This ensures each entity has its own sequence of numbers
    ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) as sequence_number,
    LOG_FILE_NAME,
    LOG_FILE_ROW_ID,
    LOAD_LTZ
FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

-- Step 2: Verify the data was inserted correctly
SELECT 
    COUNT(*) as record_count, 
    COUNT(DISTINCT dv_hashkey_hub_account) as distinct_entities,
    MIN(sequence_number) as min_sequence,
    MAX(sequence_number) as max_sequence
FROM queryassistance.snopit_gamelogs_daily;

-- Step 3: Examine sequence number distribution
-- This helps verify that sequence numbers are being assigned correctly
SELECT 
    dv_hashkey_hub_account,
    COUNT(*) as record_count,
    MIN(sequence_number) as min_sequence,
    MAX(sequence_number) as max_sequence
FROM queryassistance.snopit_gamelogs_daily
GROUP BY dv_hashkey_hub_account
ORDER BY record_count DESC
LIMIT 10;

/*
Step 4: Optional - Create additional temporal granularity SNOPIT tables
        Similar to PIT tables, you can create weekly, monthly, or current SNOPITs
*/

-- Example: Create a weekly SNOPIT table
CREATE OR REPLACE TABLE queryassistance.snopit_gamelogs_weekly AS
SELECT *
FROM queryassistance.snopit_gamelogs_daily
WHERE snapshotdate IN (
    SELECT as_of
    FROM queryassistance.as_of_date
    WHERE week_lastday = 1
);

-- Example: Create a current SNOPIT table (most recent snapshot only)
CREATE OR REPLACE TABLE queryassistance.snopit_gamelogs_current AS
SELECT *
FROM queryassistance.snopit_gamelogs_daily
WHERE snapshotdate = (
    SELECT MAX(snapshotdate)
    FROM queryassistance.snopit_gamelogs_daily
);
