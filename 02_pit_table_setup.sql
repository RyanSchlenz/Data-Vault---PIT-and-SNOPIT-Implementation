/*
Script: 02_pit_table_setup.sql
Purpose: Creates the PIT (Point-In-Time) table structure
         PIT tables provide a snapshot of entities at specific points in time
*/

-- Create the PIT (Point-In-Time) table for tracking temporal data
create or replace table queryassistance.pit_gamelogs_daily
(
  -- Primary entity identifier
  dv_hashkey_hub_account binary(20)   -- Hash key for the hub account (business key)
  
  -- Source system identifiers
  , LOG_FILE_NAME varchar(100)        -- Name of the log file (source system identifier)
  , LOG_FILE_ROW_ID number(18,0)      -- Row ID from the log file
  
  -- Temporal tracking fields
  , LOAD_LTZ TIMESTAMP_LTZ(0)         -- Load timestamp with local time zone (when data was loaded)
  , DATETIME_ISO8601 TIMESTAMP_NTZ(9) -- Event timestamp in ISO8601 format
  
  -- Business data fields
  , USER_EVENT VARCHAR(25)            -- Type of user event
  , USER_LOGIN VARCHAR(100)           -- User login identifier
  , IP_ADDRESS VARCHAR(100)           -- IP address of the user
  
  -- PIT-specific fields
  , snapshotdate date                 -- The date when this snapshot was taken
);

-- Optional: Create indexes to improve query performance
-- Note: In Snowflake, explicit indexes aren't created, but you can use clustering keys
-- alter table queryassistance.pit_gamelogs_daily cluster by (snapshotdate, dv_hashkey_hub_account);

-- Verify the table was created
describe table queryassistance.pit_gamelogs_daily;
