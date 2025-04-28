/*
Script: 06_information_mart_views.sql
Purpose: Creates business-friendly information mart views using PIT tables
         Demonstrates how to leverage PIT tables for simplified business access
*/

-- Step 1: Create a schema for information marts if it doesn't exist
create schema if not exists AGS_GAME_AUDIENCE.INFORMATION_MARTS;

-- Step 2: Create an information mart view using the PIT table
-- This view joins the PIT table with source tables to provide a business-friendly view
create or replace view AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_pit as 
select 
    -- Temporal context
    pit.snapshotdate,               
    
    -- Entity identifiers 
    pit.dv_hashkey_hub_account,
    pit.log_file_name,
    
    -- Technical metadata (optional in business views)
    pit.load_ltz,
    
    -- Business data from first source
    s1.LOG_FILE_ROW_ID as log_file_row_id,
    s1.DATETIME_ISO8601 as datetime_iso8601,
    s1.IP_ADDRESS as ip_address,
    
    -- Business data from second source
    s2.USER_EVENT as user_event,
    s2.USER_LOGIN as user_login
from 
    queryassistance.pit_gamelogs_daily pit
-- Join with the first source table using hash key and load timestamp
inner join AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS s1
    on pit.log_file_name = s1.LOG_FILE_NAME
    and pit.LOAD_LTZ = s1.LOAD_LTZ
-- Join with the second source table using hash key and load timestamp
inner join AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY s2
    on pit.log_file_name = s2.LOG_FILE_NAME
    and pit.LOAD_LTZ = s2.LOAD_LTZ;

-- Step 3: Create an information mart view using the SNOPIT table
-- This demonstrates the simplified join syntax with sequence numbers
create or replace view AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_snopit as 
select 
    -- Temporal context
    snopit.snapshotdate,
    
    -- Entity identifiers
    snopit.dv_hashkey_hub_account,
    snopit.log_file_name,
    
    -- Sequence number (for reference)
    snopit.sequence_number,
    
    -- Business data from first source
    s1.LOG_FILE_ROW_ID as log_file_row_id,
    s1.DATETIME_ISO8601 as datetime_iso8601,
    s1.IP_ADDRESS as ip_address,
    
    -- Business data from second source
    s2.USER_EVENT as user_event,
    s2.USER_LOGIN as user_login
from 
    queryassistance.snopit_gamelogs_daily snopit
-- Join with sequence number lookup for first source
inner join (
    select 
        LOG_FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) as sequence_number,
        LOG_FILE_ROW_ID,
        DATETIME_ISO8601,
        IP_ADDRESS,
        LOAD_LTZ
    from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
) s1
    on MD5(s1.LOG_FILE_NAME) = snopit.dv_hashkey_hub_account
    and s1.sequence_number = snopit.sequence_number
-- Join with sequence number lookup for second source
inner join (
    select 
        LOG_FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY MD5(LOG_FILE_NAME) ORDER BY LOAD_LTZ) as sequence_number,
        USER_EVENT,
        USER_LOGIN,
        LOAD_LTZ
    from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY
) s2
    on MD5(s2.LOG_FILE_NAME) = snopit.dv_hashkey_hub_account
    and s2.sequence_number = snopit.sequence_number;

-- Step 4: Create a point-in-time query function (optional)
-- This demonstrates how to create a parameterized function for point-in-time queries
create or replace function AGS_GAME_AUDIENCE.INFORMATION_MARTS.fn_game_logs_as_of(as_of_date date)
returns table (
    as_of date,
    dv_hashkey_hub_account binary(20),
    log_file_name varchar(100),
    log_file_row_id number(18,0),
    datetime_iso8601 timestamp_ntz(9),
    ip_address varchar(100),
    user_event varchar(25),
    user_login varchar(100)
)
as
$$
    select 
        :as_of_date as as_of,
        pit.dv_hashkey_hub_account,
        pit.log_file_name,
        s1.LOG_FILE_ROW_ID,
        s1.DATETIME_ISO8601,
        s1.IP_ADDRESS,
        s2.USER_EVENT,
        s2.USER_LOGIN
    from 
        queryassistance.pit_gamelogs_daily pit
    inner join AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS s1
        on pit.log_file_name = s1.LOG_FILE_NAME
        and pit.LOAD_LTZ = s1.LOAD_LTZ
    inner join AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS_COPY s2
        on pit.log_file_name = s2.LOG_FILE_NAME
        and pit.LOAD_LTZ = s2.LOAD_LTZ
    where 
        pit.snapshotdate = :as_of_date
$$;

-- Step 5: Test the information mart views
select * from AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_pit
limit 10;

select * from AGS_GAME_AUDIENCE.INFORMATION_MARTS.gamelogs_daily_mart_snopit
limit 10;

-- Step 6: Test the point-in-time function (if created)
select * from table(AGS_GAME_AUDIENCE.INFORMATION_MARTS.fn_game_logs_as_of('2022-01-15'))
limit 10;
