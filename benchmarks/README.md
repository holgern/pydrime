# PyDrime Sync Benchmarks

This directory contains benchmark scripts to validate PyDrime sync functionality.

## Scripts

### `benchmark_timestamp_preservation.py`

**NEW** - Tests if uploaded files preserve their creation and modification timestamps:

- Creates test files with specific modification timestamps (recent, old, nested)
- Uploads files to cloud using CLI
- Downloads files to a different location
- Compares original and downloaded timestamps (with 2 second tolerance)
- Reports pass/fail for each file

This benchmark helps verify that file metadata (timestamps) are correctly preserved
through the upload/download cycle.

### `benchmark_sync_modes.py`

Validates basic sync mode behavior:

**Test 1: Cloud Upload (localToCloud)**

- Creates 10 test files (1KB each) with random content
- Uploads to a unique UUID-named folder in cloud
- Attempts second upload to verify idempotency (should upload 0 files)

**Test 2: Cloud Download (cloudToLocal)**

- Creates a second local directory
- Downloads files from the cloud folder created in Test 1
- Attempts second download to verify idempotency (should download 0 files)

## Usage

### Prerequisites

1. PyDrime must be installed and configured:

   ```bash
   pydrime init
   ```

2. You need a valid API key and access to a workspace

### Running Benchmarks

```bash
# Run all benchmarks
python benchmarks/run_benchmarks.py

# Run a specific benchmark
python benchmarks/run_benchmarks.py timestamp_preservation

# List available benchmarks
python benchmarks/run_benchmarks.py --list

# Generate a markdown report
python benchmarks/run_benchmarks.py --report

# Run specific benchmarks directly
python benchmarks/benchmark_timestamp_preservation.py
python benchmarks/benchmark_sync_modes.py
```

### What to Expect

The script will:

1. Create a local `benchmark_temp` directory in your current workspace
2. Create a unique folder in your cloud workspace (e.g., `benchmark_abc123...`)
3. Run upload and download tests
4. Print detailed progress and statistics
5. Report pass/fail for each test

Example output:

```
================================================================================
PYDRIME SYNC MODE BENCHMARKS
================================================================================

[FOLDER] Test folder: benchmark_abc12345-6789-...
   This folder will be created in the cloud workspace root

================================================================================
TEST 1: CLOUD UPLOAD (localToCloud) MODE
================================================================================

[CREATE] Creating 10 test files (1KB each) in benchmark_temp/upload_test
  + Created: test_file_000.txt
  ...

[SYNC] First sync (should upload 10 files)...
[STATS] First sync stats: {'uploaded': 10, 'downloaded': 0, ...}
[PASS] First sync uploaded 10 files as expected

[SYNC] Second sync (should upload 0 files - idempotency test)...
[STATS] Second sync stats: {'uploaded': 0, 'downloaded': 0, ...}
[PASS] Second sync uploaded 0 files - idempotency confirmed

================================================================================
TEST 2: CLOUD DOWNLOAD (cloudToLocal) MODE
================================================================================

[SYNC] First download sync (should download 10 files)...
[STATS] First download sync stats: {'uploaded': 0, 'downloaded': 10, ...}
[PASS] First download sync downloaded 10 files as expected

[SYNC] Second download sync (should download 0 files - idempotency test)...
[STATS] Second download sync stats: {'uploaded': 0, 'downloaded': 0, ...}
[PASS] Second download sync downloaded 0 files - idempotency confirmed

================================================================================
[PASS] ALL TESTS PASSED
================================================================================
```

## Cleanup

The benchmark script leaves the test data in place for inspection:

- **Local**: `benchmark_temp/` directory (can be safely deleted)
- **Remote**: `benchmark_<uuid>/` folder in your cloud workspace (manually delete if
  needed)

## Adding New Benchmarks

To add new benchmark scripts:

1. Create a new Python file in this directory
2. Follow the same structure as `test_sync_modes.py`
3. Use subprocess to call `pydrime sync` CLI commands
4. Parse output to validate behavior
5. Update this README with documentation

## Notes

- All benchmarks use the `pydrime` CLI directly via subprocess
- Each test creates a unique UUID-named folder to avoid conflicts
- Tests verify both functionality and idempotency
- Statistics are parsed from CLI output
