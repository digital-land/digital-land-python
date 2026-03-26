# Phases

This directory contains transformation phases used in the digital-land data pipeline. Phases are modular processing steps that transform and validate data.

## Transform Phases

The `transform` folder contains the core data transformation phases executed in sequence:

### Data Transformation Pipeline

1. **01_convert.py** - Convert data types and formats
2. **02_normalise.py** - Normalize data values and structure
3. **03_parse.py** - Parse and extract data from raw inputs
4. **04_concat_field.py** - Concatenate multiple fields
5. **05_filter.py** - Filter records based on criteria
6. **06_map.py** - Map values between different formats
7. **07_patch.py** - Apply patches to data records
8. **08_validate.py** - Validate data against schema
9. **09_set_default.py** - Set default values for missing data
10. **10_migrate.py** - Migrate data structure/format
11. **11_resolve_organisation.py** - Resolve and enrich organisation references
12. **12_field_prune.py** - Remove unnecessary fields
13. **13_entity_reference.py** - Handle entity references
14. **14_entity_lookup.py** - Lookup and enrich entity data
15. **15_pivot.py** - Pivot data structure
16. **16_fact_hash.py** - Generate fact hashes for deduplication
17. **17_flatten.py** - Flatten nested data structures

## Load Phases

The `load` folder contains phases for saving and storing data:

1. **01_save_file.py** - Save data to file storage
2. **02_save_database.py** - Save data to database

## Overview

Each phase is designed to be:
- **Modular** - Can be used independently or in sequence
- **Configurable** - Parameters can be customized via configuration
- **Reusable** - Shared across different pipelines and workflows
