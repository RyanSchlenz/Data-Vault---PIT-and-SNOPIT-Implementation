/*
Script: 03_snopit_table_setup.sql
Purpose: Creates the SNOPIT (Sequence Number Only Point-In-Time) table structure
         SNOPIT tables optimize PIT tables by using sequence numbers instead of timestamps
*/

-- Create the SNOPIT (Sequence Number Only Point-In-Time) table
create or replace table queryassistance.snopit_gamelogs_daily
(
  -- Primary entity identifier
  dv_hashkey_hub_account binary(20)   -- Hash key for the hub account
  
  -- Sequence identifier for temporal tracking (key optimization)
  , sequence_number int not null      -- Sequence number for temporal tracking
  
  -- Source system identifiers
  , LOG_FILE_NAME varchar(100)        -- Name of the log file
  , LOG_FILE_ROW_ID number(18,0)      -- Row ID from the log file
  
  -- Temporal tracking fields (stored but not used for joins)
  , LOAD_LTZ TIMESTAMP_LTZ(0)         -- Load timestamp
  , DATETIME_ISO8601 TIMESTAMP_NTZ(9) -- Event timestamp
  
  -- Business data fields
  , USER_EVENT VARCHAR(25)            -- Type of user event
  , USER_LOGIN VARCHAR(100)           -- User login identifier
  , IP_ADDRESS VARCHAR(100)           -- IP address of the user
  
  -- SNOPIT-specific fields
  , snapshotdate date                 -- The date when this snapshot was taken
);

-- Optional: Create indexes to improve query performance
-- Note: In Snowflake, explicit indexes aren't created, but you can use clustering keys
-- alter table queryassistance.snopit_gamelogs_daily cluster by (snapshotdate, dv_hashkey_hub_account, sequence_number);

-- Verify the table was created
describe table queryassistance.snopit_gamelogs_daily;

-- Commentary on SNOPIT vs PIT
/*
Note the key difference between PIT and SNOPIT tables:

1. PIT tables use both a hash key and a load timestamp for joins, requiring
   comparison of two columns in join conditions:
   
   ON pit.dv_hashkey_hub_account = sat.dv_hashkey_hub_account
   AND pit.load_ltz = sat.load_ltz

2. SNOPIT tables use a hash key and a sequence number, which simplifies joins:

   ON snopit.dv_hashkey_hub_account = sat.dv_hashkey_hub_account
   AND snopit.sequence_number = sat.sequence_number
   
This optimization can significantly improve query performance, especially
for complex joins across multiple tables.
*/
