# Database Validation Scripts

Scripts for validating data integrity between Oracle and Snowflake databases.

## Main Scripts

### Core Validation Tools
- **ValidateAllTables.py** - Validates all tables by comparing Oracle and Snowflake data
- **ValidateOneTable.py** - Validates a single specified table
- **ValidateRowCount.py** - Compares row counts between Oracle and Snowflake tables
- **validate.py** - General validation script for table data
- **Template.py** - Template/shell script for validation operations

### Database Comparison
- **countTables.py** - Counts and compares table counts in both databases
- **firstFiveTables.py** - Displays first five tables from each database
- **tableMatch.py** - Checks if table names match between databases
- **checkOutlier.py** - Checks for table existence and outliers

## Usage
These scripts typically require Oracle and Snowflake credentials configured in environment variables.
