# Agent Instructions for pydrime

This document provides guidelines for AI agents working on this repository.

## Project Overview

Instructions:

- Never use git commands, you must not commit code

## Installation

### For Development

```bash
pre-commit run --all-files
```

## Documentation

### Building Documentation

The project documentation is built using Sphinx and hosted on ReadTheDocs.

To build the documentation locally:

```bash
cd docs && make clean && make html
```

The generated documentation will be available in the `docs/_build/html` directory.

### API Documentation

When working on the code, please follow these documentation guidelines:

- Use docstrings for all public classes, methods, and functions
- Follow the NumPy docstring format
- Include type hints in function signatures
- Document parameters, return values, and raised exceptions

## Development

### Running Tests

Run all tests using `pytest`.

To run tests with coverage: `pytest --cov=pydrime --cov-report=term`

### Linting and Formatting

This project uses `ruff` for linting and formatting, and `prettier` for other file
types. These are enforced by pre-commit hooks.

Run linting and formatting:
`ruff check --fix --exit-non-zero-on-fix --config=.ruff.toml`

### cli

Do not use `python -m pydrime` to run the cli but `pydrime` directly!

### pip

Do not use `pip install` but `uv pip install`! pydrime is installed with `-e .` .

## Project Structure

The project is organized as follows:

```
pydrime/                   # Main package
tests/                     # Test directory
pyproject.toml             # Project metadata and build configuration
README.md                  # Project README
```

### Key Modules

- `pydrime/__init__.py`: Package initialization and public API exports
- `pydrime/cli.py`: Command-line interface implementation

## Code Style

- **Formatting**: Adhere to the `ruff` and `prettier` configurations. Maximum line
  length is 88 characters.
- **Imports**: Follow the `isort` configuration in `.ruff.toml`. Imports are grouped
  into `future`, `standard-library`, `third-party`, `first-party`, and `local-folder`.
- **Naming**: Use `snake_case` for functions and variables, and `PascalCase` for
  classes.
- **Types**: Add type hints for all new functions and methods.
- **Error Handling**: Use standard `try...except` blocks for error handling.

## Contribution Guidelines

### Common Issues

When working on the code, be aware of these common issues:

4. Type hints: Ensure proper typing for all functions, especially for context
   dictionaries and layout configurations
