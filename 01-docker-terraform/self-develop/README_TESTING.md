# Testing Documentation

## Overview
This directory contains unit tests for the data ingestion scripts (`ingest_green_data.py` and `ingest_yellow_data.py`).

## Test Coverage

The test suite (`test_ingest.py`) covers the following scenarios:

### Green Taxi Data (`ingest_green_data.py`)
1. **Test 1: Ingest into new table** - Verifies that data is successfully ingested using `if_exists='replace'` when the target table does not exist
2. **Test 2: Append to existing table** - Verifies that data is successfully appended using `if_exists='append'` in chunks when the target table already exists

### Yellow Taxi Data (`ingest_yellow_data.py`)
3. **Test 3: Ingest into new table** - Verifies that data is successfully ingested using `if_exists='replace'` when the target table does not exist
4. **Test 4: Append to existing table** - Verifies that data is successfully appended using `if_exists='append'` in chunks when the target table already exists

## Setup

### Install Testing Dependencies

```bash
pip install -r requirements-test.txt
```

This will install:
- `pytest>=7.4.0` - Testing framework
- `pytest-mock>=3.12.0` - Mocking utilities for pytest

## Running Tests

### Run all tests
```bash
pytest test_ingest.py -v
```

### Run specific test class
```bash
# Green taxi tests only
pytest test_ingest.py::TestIngestGreenData -v

# Yellow taxi tests only
pytest test_ingest.py::TestIngestYellowData -v
```

### Run specific test case
```bash
pytest test_ingest.py::TestIngestGreenData::test_ingest_new_table -v
```

### Run with coverage report
```bash
pip install pytest-cov
pytest test_ingest.py --cov=. --cov-report=term-missing
```

## Test Implementation Details

### Mocking Strategy
The tests use Python's `unittest.mock` to mock external dependencies:
- **Database connections** (`create_engine`) - Prevents actual database connections
- **Data reading** (`pd.read_parquet`) - Uses sample DataFrames instead of downloading real data
- **Database inspection** (`inspect`) - Simulates table existence checks
- **Data writing** (`DataFrame.to_sql`) - Verifies method calls without actual database writes

### Test Fixtures
- `sample_dataframe`: Creates a minimal DataFrame that mimics taxi data structure
- `mock_engine`: Provides a mocked SQLAlchemy engine
- `mock_inspector`: Provides a mocked SQLAlchemy inspector

### Assertions
Each test verifies:
1. **Exit code**: Ensures the CLI command completes successfully
2. **Database connection**: Verifies correct connection string is used
3. **Data source**: Confirms the correct parquet file URL is constructed
4. **Table existence check**: Validates that the script checks if the table exists
5. **Write operation**: Ensures `to_sql()` is called with correct parameters (`if_exists='replace'` or `'append'`)
6. **Output messages**: Checks that appropriate messages are printed

## Expected Output

When all tests pass, you should see:
```
====================== test session starts ======================
...
test_ingest.py::TestIngestGreenData::test_ingest_new_table PASSED
test_ingest.py::TestIngestGreenData::test_append_existing_table PASSED
test_ingest.py::TestIngestYellowData::test_ingest_new_table PASSED
test_ingest.py::TestIngestYellowData::test_append_existing_table PASSED

====================== 4 passed in X.XXs ======================
```
