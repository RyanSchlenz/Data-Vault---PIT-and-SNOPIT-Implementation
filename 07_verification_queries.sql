/*
Script: 07_verification_queries.sql
Purpose: Verifies that PIT and SNOPIT tables were created and populated correctly
         Includes diagnostic queries for troubleshooting
*/

-- Step 1: Verify the AS_OF_DATE table
SELECT 
    COUNT(*) as total_dates,
    MIN(as_of) as start_date,
    MAX(as_of) as end_date,
    SUM(month_lastday) as month_end_count,
    SUM(week_lastday) as week_end_count,
    SUM(week_firstday) as week_start_count
FROM queryassistance.as_of_date;

-- Step 2: Verify the PIT table
SELECT 
    COUNT(*) as pit_record_count,
    COUNT(DISTINCT dv_hashkey_hub_account) as pit_entity_count,
    MIN(snapshotdate) as pit_snapshot_date,
    MAX(snapshotdate) as pit_latest_date
FROM queryassistance.pit_gamelogs_daily;

-- Step 3: Verify the SNOPIT table
SELECT 
    COUNT(*) as snopit_record_count,
    COUNT(DISTINCT dv_hashkey_hub_account) as snopit_entity_count,
    MIN(sequence_number) as min_sequence,
    MAX(sequence_number) as max_sequence,
    MIN(snapshotdate) as snopit_snapshot_date,
    MAX(snapshotdate) as snopit_latest_date
FROM queryassistance.snopit_gamelogs_daily;

-- Step 4: Compare counts between source and PIT tables
SELECT 
    (SELECT COUNT(*) FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS) as source_count,
    (SELECT COUNT(*) FROM queryassistance.pit_gamelogs_daily) as pit_count,
    (SELECT COUNT(*) FROM queryassistance.snopit_gamelogs_daily) as snopit_count;

-- Step 5: Examine a sample entity's history in PIT table
WITH sample_entity AS (
    SELECT dv_hashkey_hub_account
    FROM queryassistance.pit_gamelogs_daily
    LIMIT 1
)
SELECT 
    p.dv_hashkey_hub_account,
    p.log_file_name,
    p.load_ltz,
    p.snapshotdate
FROM queryassistance.pit_gamelogs_daily p
JOIN sample_entity s ON p.dv_hashkey_hub_account = s.dv_hashkey_hub_account
ORDER BY p.load_ltz;

-- Step 6: Examine a sample entity's sequence numbers in SNOPIT table
WITH sample_entity AS (
    SELECT dv_hashkey_hub_account
    FROM queryassistance.snopit_gamelogs_daily
    LIMIT 1
)
SELECT 
    s.dv_hashkey_hub_account,
    s.log_file_name,
    s.sequence_number,
    s.load_ltz,
    s.snapshotdate
FROM queryassistance.snopit_gamelogs_daily s
JOIN sample_entity e ON s.dv_hashkey_hub_account = e.dv_hashkey_hub_account
ORDER BY s.sequence_number;

-- Step 7: Verify the information mart views
SELECT 
    (SELECT COUNT(*) FROM AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_pit) as pit_mart_count,
    (SELECT COUNT(*) FROM AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_snopit) as snopit_mart_count;

-- Step 8: Check for potential data quality issues
-- Identifies entities with multiple records for the same timestamp
SELECT 
    dv_hashkey_hub_account,
    load_ltz,
    COUNT(*) as record_count
FROM queryassistance.pit_gamelogs_daily
GROUP BY dv_hashkey_hub_account, load_ltz
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-- Step 9: Check for sequence number consistency
-- Identifies any gaps in sequence numbers
WITH sequence_analysis AS (
    SELECT 
        dv_hashkey_hub_account,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY dv_hashkey_hub_account ORDER BY sequence_number) as prev_sequence
    FROM queryassistance.snopit_gamelogs_daily
)
SELECT 
    dv_hashkey_hub_account,
    prev_sequence as gap_before,
    sequence_number as gap_after,
    sequence_number - prev_sequence as gap_size
FROM sequence_analysis
WHERE sequence_number - prev_sequence > 1
ORDER BY gap_size DESC;
