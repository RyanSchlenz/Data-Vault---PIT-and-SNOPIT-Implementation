# Data Vault PIT and SNOPIT Tables Implementation

This repository contains modular SQL scripts for implementing Point-in-Time (PIT) and Sequence Number Only Point-in-Time (SNOPIT) tables in a Data Vault architecture.

## What are PIT Tables?

Point-in-Time (PIT) tables are specialized structures within the Data Vault methodology that serve as temporal indices or "snapshots" of data at specific moments. They function like a GPS system for your data warehouse, allowing you to navigate to the correct version of data as it existed at a particular point in time.

## Implementation Scripts

Follow these scripts in order for a complete implementation:

1. [`01_as_of_date_setup.sql`](./01_as_of_date_setup.sql) - Creates and populates the AS_OF_DATE table (date dimension)
2. [`02_pit_table_setup.sql`](./02_pit_table_setup.sql) - Creates the standard PIT table structure
3. [`03_snopit_table_setup.sql`](./03_snopit_table_setup.sql) - Creates the SNOPIT table structure
4. [`04_pit_population.sql`](./04_pit_population.sql) - Populates the PIT table
5. [`05_snopit_population.sql`](./05_snopit_population.sql) - Populates the SNOPIT table
6. [`06_information_mart_views.sql`](./06_information_mart_views.sql) - Creates information mart views using PIT tables
7. [`07_verification_queries.sql`](./07_verification_queries.sql) - Verifies that tables were created and populated correctly

## Technical Documentation

For detailed information on the concepts and implementation, see the [Technical Documentation](pit_tables_technical_documentation.md).

## Benefits of PIT and SNOPIT Tables

- **Performance Optimization**: Pre-calculating temporal relationships dramatically improves query performance
- **Simplified Querying**: Reduces the complexity of SQL joins needed to retrieve historical data
- **Consistent Snapshots**: Ensures consistent point-in-time views across multiple related entities
- **Efficient Temporal Joins**: SNOPIT tables further optimize queries by using sequence numbers instead of load dates

## Usage

These scripts are designed to be modular. You can run them sequentially for a complete implementation, or selectively implement certain components based on your needs.

## Requirements

- These scripts are optimized for Snowflake but can be adapted for other database platforms
- Basic understanding of Data Vault concepts
- Appropriate permissions to create and populate tables

## Contributing

Feel free to submit issues or pull requests with improvements or extensions to these scripts.
