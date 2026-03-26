# Testing Guidelines

## Test Structure

This repository follows a structured testing approach that mirrors the codebase organization to ensure maintainability and clarity.

### Directory Structure

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── phase/              # Tests for legacy phase modules
│   └── phase_polars/       # Tests for new phase_polars framework
│       ├── load/           # Load phase unit tests
│       └── transform/      # Transform phase unit tests
├── integration/            # Integration tests for component interactions
│   ├── phase/              # Legacy phase integration tests
│   └── phase_polars/       # Phase_polars integration tests
│       ├── load/           # Load phase integration tests
│       └── transform/      # Transform phase integration tests
├── e2e/                    # End-to-end tests
├── acceptance/             # Acceptance tests
└── performance/            # Performance tests
```

### Test Organization Principles

1. **Mirror Source Structure**: Test directories should mirror the source code structure
2. **Separation of Concerns**: Unit, integration, and e2e tests are clearly separated
3. **Framework Isolation**: Legacy `phase/` and new `phase_polars/` tests are isolated

### Phase_polars Test Structure

The `phase_polars` test structure follows the modular design of the new ETL framework:

#### Unit Tests (`tests/unit/phase_polars/`)
- **Load Tests**: `load/test_save_database.py`, `load/test_save_file.py`
- **Transform Tests**: Individual test files for each transform phase:
  - `transform/test_convert.py`
  - `transform/test_normalise.py`
  - `transform/test_filter.py`
  - `transform/test_validate.py`
  - And 14 additional transform phase test files

#### Integration Tests (`tests/integration/phase_polars/`)
- **Load Integration**: `load/test_load_integration.py`
- **Transform Integration**: `transform/test_transform_integration.py`
- **Individual Phase Integration**: Separate files for each transform phase

### Naming Conventions

- Test files: `test_<module_name>.py`
- Test classes: `Test<ClassName>`
- Test methods: `test_<functionality>`
- Integration test files: `test_<component>_integration.py`

### Running Tests

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run phase_polars tests only
pytest tests/unit/phase_polars/ tests/integration/phase_polars/

# Run specific test file
pytest tests/unit/phase_polars/transform/test_convert.py

# Run with verbose output
pytest -v tests/unit/phase_polars/
```

### Test Implementation Status

Currently, the phase_polars test files contain placeholder structures ready for implementation once the corresponding phase modules are developed. This ensures:

1. **Consistent Structure**: All test files follow the same organizational pattern
2. **Ready for Development**: Test structure is prepared for parallel development
3. **Clear Separation**: Unit and integration tests are clearly distinguished

### Dependencies

- **pytest**: Primary testing framework
- **pytest-mock**: For mocking dependencies
- **coverage**: For test coverage reporting

### Future Considerations

- Tests will be implemented as phase_polars modules are developed
- Integration tests will validate data flow between phases
- Performance tests may be added for large dataset processing