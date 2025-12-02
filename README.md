# Database Validation Scripts

Scripts for validating data integrity between Oracle and Snowflake databases.

## Main Scripts

### Core Validation Tools
- **ValidateAllTables.py** - Validates all tables by comparing Oracle and Snowflake data
- **ValidateOneTable.py** - Validates a single specified table
- **ValidateRowCount.py** - Compares row counts between Oracle and Snowflake tables
- **validate.py** - General validation script for table data
- **emptyShell.py** - Template/shell script for validation operations

### Testing & Development
- **test_validate.py** - Test version of validation script
- **test_validateNew.py** - Updated test validation script
- **validateOLD.py** - Legacy/deprecated validation code

### Database Comparison
- **countTables.py** - Counts and compares table counts in both databases
- **firstFiveTables.py** - Displays first five tables from each database
- **sameTables.py** - Checks if table names match between databases
- **checkOutlier.py** - Checks for table existence and outliers

## Usage
These scripts typically require Oracle and Snowflake credentials configured in environment variables.
