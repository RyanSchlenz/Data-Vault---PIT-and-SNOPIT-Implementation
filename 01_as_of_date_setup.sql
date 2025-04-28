/*
Script: 01_as_of_date_setup.sql
Purpose: Creates and populates the AS_OF_DATE table for Point-in-Time queries
         This table serves as a date dimension for temporal analysis
*/

-- Step 1: Create the AS_OF_DATE table
create or replace table queryassistance.as_of_date
(
as_of date not null                -- The primary date field used for point-in-time reference
, year smallint not null           -- Year component of the date
, month smallint not null          -- Month number (1-12)
, month_name char(10)              -- Month name (January, February, etc.)
, day_of_month smallint not null   -- Day of the month (1-31)
, day_of_week varchar(9) not null  -- Day of week (0-6, with 0 being Sunday)
, day_name char(10)                -- Name of the day (Monday, Tuesday, etc.)
, week_of_year smallint not null   -- The week number within the year
, day_of_year smallint not null    -- Day number within the year (1-366)
, month_lastday smallint not null  -- Flag indicating if date is the last day of the month (1=yes, 0=no)
, week_lastday smallint not null   -- Flag indicating if date is the last day of the week (1=yes, 0=no)
, week_firstday smallint not null  -- Flag indicating if date is the first day of the week (1=yes, 0=no)
);

-- Step 2: Populate the AS_OF_DATE table
-- This example generates dates for 181 days starting from Jan 1, 2022
insert into queryassistance.as_of_date
with date_generator as (
    select dateadd(day, seq4(0), '2022-01-01') as as_of
    from table(generator(rowcount=>181))
)
select as_of
    , year(as_of)                                  -- Extract year
    , month(as_of)                                 -- Extract month number
    , monthname(as_of)                             -- Extract month name
    , day(as_of)                                   -- Extract day of month
    , dayofweek(as_of)                             -- Extract day of week
    , dayname(as_of)                               -- Extract day name
    , weekofyear(as_of)                            -- Extract week of year
    , dayofyear(as_of)                             -- Extract day of year
    , case when last_day(as_of) = as_of then 1     -- Check if date is last day of month
      else 0 end as month_lastday
    , case when last_day(as_of, 'week') = as_of then 1  -- Check if date is last day of week
      else 0 end as week_lastday
    , case when dayname(as_of) = 'Mon' then 1      -- Check if date is first day of week
      else 0 end as week_firstday
from date_generator;

-- Step 3: Verify the AS_OF_DATE table was populated correctly
select 
    count(*) as row_count,
    min(as_of) as start_date,
    max(as_of) as end_date,
    sum(month_lastday) as month_end_count,
    sum(week_firstday) as week_start_count
from queryassistance.as_of_date;
