# Digital Land Python Tests

This directory contains all automated tests for the Digital Land Python project. Tests are organized by type and scope.

## Quick Start

### 1. First-Time Setup

```bash
# From the project root directory
./setup_venv.sh
source venv/bin/activate
```

This creates a virtual environment and installs all dependencies including test dependencies.

### 2. Run All Tests

```bash
# Run everything
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=digital_land --cov-report=html
```

## Test Organization

### Unit Tests (`unit/`)
Fast, isolated tests for individual functions and classes.

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific module tests
pytest tests/unit/phase/ -v
pytest tests/unit/test_cli.py -v

# Run a specific test
pytest tests/unit/phase/test_harmonise.py::test_harmonise -v
```

**Key modules:**
- `datatype/` - Data type conversion tests
- `phase/` - Pipeline phase tests (harmonise, parse, convert, etc.)
- `test_collection.py` - Collection logic tests
- `test_cli.py` - Command-line interface tests
- `test_api.py` - API functionality tests

### Integration Tests (`integration/`)
Tests that verify components work together with realistic data flows.

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific integration test
pytest tests/integration/test_collect.py -v

# Run with output logging
pytest tests/integration/ -v --log-cli-level=INFO
```

**Key areas:**
- `phase/` - Phase integration (complex workflows)
- `test_add_data.py` - Data addition workflows
- `test_collection.py` - Full collection workflows
- `test_pipeline/` - End-to-end pipeline tests
- `test_api.py` - API integration tests

### Acceptance Tests (`acceptance/`)
High-level tests with production-like scenarios using actual commands and data.

```bash
# Run all acceptance tests
pytest tests/acceptance/ -v

# Run specific acceptance test
pytest tests/acceptance/test_commands.py -v
```

**Coverage:**
- `test_commands.py` - CLI command execution
- `test_pipeline_command.py` - Pipeline workflows
- `test_add_data.py` - Data addition flows
- `test_dataset_create.py` - Dataset creation

<!-- ⚠️ These may take longer to run -->

### End-to-End Tests (`e2e/`)
Full system tests that exercise complete workflows.

```bash
# Run all e2e tests
pytest tests/e2e/ -v --log-cli-level=INFO

# Run specific e2e test
pytest tests/e2e/test_workflow.py -v

# Run with timeout (some e2e tests are long-running)
pytest tests/e2e/ -v --timeout=300
```

**Key tests:**
- `test_workflow.py` - Complete pipeline workflows
- `test_state.py` - State management workflows
- `test_assign_entities.py` - Entity assignment
- `test_add_endpoints_and_lookups.py` - Endpoint configuration

### Performance Tests (`performance/`)
Benchmarks and performance validation tests.

```bash
# Run performance tests
pytest tests/performance/ -v

# Run with timing output
pytest tests/performance/ -v --durations=10
```

## Common Commands

### Run Specific Test Categories

```bash
# Unit tests only (fast)
pytest tests/unit/ -v

# Unit + Integration (medium)
pytest tests/unit/ tests/integration/ -v

# Everything (comprehensive)
pytest -v

# Without e2e (skip slow tests)
pytest -v --ignore=tests/e2e/ --ignore=tests/acceptance/
```

### Debugging & Detailed Output

```bash
# Show print statements
pytest tests/unit/phase/test_harmonise.py -v -s

# Show detailed logs
pytest tests/integration/ -v --log-cli-level=DEBUG

# Stop on first failure
pytest -x -v

# Show slowest tests
pytest --durations=10 -v

# Run with full traceback
pytest --tb=long -v
```

### Filtered Runs

```bash
# Run only tests matching a pattern
pytest -k "harmonise" -v

# Run tests excluding certain patterns
pytest --ignore=tests/e2e/ -v

# Run by marker
pytest -m "unit" -v
```

### Coverage Reports

```bash
# Generate HTML coverage report
pytest --cov=digital_land --cov-report=html tests/unit/

# View coverage for specific module
pytest --cov=digital_land.phase tests/unit/phase/ -v

# Coverage with terminal report
pytest --cov=digital_land --cov-report=term-missing tests/unit/
```

## Test Data

Test data is stored in `tests/data/` including:
- CSV files for transformation testing
- JSON schema definitions
- Fixture data for various scenarios
- Expected output for validation

## Configuration

### pytest.ini
Located in project root - configures pytest behavior.

### conftest.py Files
- `tests/conftest.py` - Session-level fixtures (specification downloads)
- `tests/unit/conftest.py` - Unit test fixtures
- `tests/acceptance/conftest.py` - Acceptance test fixtures

**Key fixtures:**
- `specification_dir` - Downloads test specifications from remote sources
- Various phase fixtures in unit tests

## Performance Monitoring

All phases now include built-in timing via the `@timer` decorator. This logs:
- Phase start/end messages
- Total elapsed time (seconds)
- Number of rows processed
- Processing rate (rows/second)

Example output:
```
INFO Starting Harmonise phase...
INFO Harmonise phase completed in 0.05 seconds (1000 rows processed, 20000.00 rows/sec)
```

View timing output with:
```bash
pytest tests/integration/ -v --log-cli-level=INFO
```

## Troubleshooting

### Tests fail with import errors
```bash
# Reinstall in development mode
pip install -e .
```

### Specification downloads fail
```bash
# Check internet connection
# Rerun with clearer logging
pytest -v --log-cli-level=DEBUG

# Tests cache specs in tmp directories, clean with:
rm -rf /tmp/pytest-*
```

### Virtual environment issues
```bash
# Rerun setup
./setup_venv.sh

# Or manually:
python3 -m venv venv
source venv/bin/activate
pip install -e ".[test]"
```

### Slow e2e tests
```bash
# Skip long-running tests during development
pytest tests/unit/ tests/integration/ -v

# Run specific e2e test only when needed
pytest tests/e2e/test_workflow.py -v
```

## Best Practices

### When Writing Tests
1. **Unit tests** - Fast, no I/O, isolated functionality
2. **Integration tests** - Real data flows, multiple components
3. **Acceptance tests** - Command-line execution, realistic usage
4. **E2E tests** - Complete workflows, final validation

### When Running Tests
- Run unit tests frequently during development (`pytest tests/unit/`)
- Run integration tests before commit (`pytest tests/unit/ tests/integration/`)
- Run full suite before pushing (`pytest -v`)
- Use `-k` flag to run specific tests while developing

### Performance Tips
- Run unit tests first (fastest feedback)
- Skip e2e tests during development unless needed
- Use `-x` to stop on first failure for quick fixes
- Use `-v -s` to see detailed output when debugging

## Resources

- **Test Framework**: [pytest documentation](https://docs.pytest.org/)
- **Coverage**: [pytest-cov](https://pytest-cov.readthedocs.io/)
- **Mocking**: [pytest-mock](https://github.com/pytest-dev/pytest-mock)
- **Project CI**: GitHub Actions (see `.github/workflows/`)

