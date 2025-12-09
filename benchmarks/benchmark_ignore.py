"""
Benchmark script for .pydrignore file functionality.

Tests the gitignore-style ignore file feature similar to Kopia's .kopiaignore.

Tests:
1. Basic patterns: Test *.log, *.tmp ignore patterns
2. Negation: Test !important.log to un-ignore specific files
3. Directory patterns: Test temp/ to ignore directories
4. Hierarchical: Test subdirectory .pydrignore overriding parent rules
5. Double wildcards: Test **/cache/** patterns
6. Anchored patterns: Test /logs for root-only matching
7. Performance: Test scanning with many files and complex patterns

All operations use local file system only (no cloud sync) to focus on
the ignore pattern matching functionality.
"""

import shutil
import sys
import tempfile
import time
from pathlib import Path

from syncengine.ignore import DEFAULT_IGNORE_FILE_NAME
from syncengine.scanner import DirectoryScanner


def create_test_directory(base_dir: Path) -> dict[str, list[Path]]:
    """Create a test directory structure for benchmarking.

    Creates a structure similar to Kopia documentation example:
    - thesis/
      - title.png
      - manuscript.tex
      - tmp.db
      - atmp.db
      - logs.dat
      - figures/
        - architecture.png
        - server.png
      - chapters/
        - introduction.tex
        - abstract.tex
        - logs/
          - chapter.log
      - logs/
        - gen.log
        - fail.log
        - tmp.db
      - cache/
        - data.bin
      - src/
        - cache/
          - nested.bin

    Returns:
        Dictionary with file categories for verification
    """
    base_dir.mkdir(parents=True, exist_ok=True)

    # Create files
    files_created = {
        "should_include": [],
        "should_exclude_by_extension": [],
        "should_exclude_by_name": [],
        "should_exclude_by_dir": [],
        "should_exclude_by_pattern": [],
    }

    # Files that should be included
    include_files = [
        "title.png",
        "manuscript.tex",
        "atmp.db",
        "abtmp.db",
        "figures/architecture.png",
        "figures/server.png",
        "chapters/introduction.tex",
        "chapters/abstract.tex",
        "chapters/conclusion.tex",
        "chapters/logs/chapter.log",  # Not in /logs, so should be included
        "src/main.py",
    ]

    for rel_path in include_files:
        file_path = base_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(f"Content of {rel_path}\n" + "x" * 100)
        files_created["should_include"].append(file_path)

    # Files excluded by extension (*.dat)
    exclude_by_ext = ["logs.dat", "data.dat"]
    for rel_path in exclude_by_ext:
        file_path = base_dir / rel_path
        file_path.write_text(f"Content of {rel_path}\n")
        files_created["should_exclude_by_extension"].append(file_path)

    # Files excluded by name (tmp.db)
    exclude_by_name = ["tmp.db", "subdir/tmp.db"]
    for rel_path in exclude_by_name:
        file_path = base_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(f"Content of {rel_path}\n")
        files_created["should_exclude_by_name"].append(file_path)

    # Files excluded by directory (/logs/*)
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    exclude_by_dir = ["logs/gen.log", "logs/fail.log", "logs/log.db"]
    for rel_path in exclude_by_dir:
        file_path = base_dir / rel_path
        file_path.write_text(f"Content of {rel_path}\n")
        files_created["should_exclude_by_dir"].append(file_path)

    # Files excluded by pattern (**/cache/**)
    cache_files = ["cache/data.bin", "src/cache/nested.bin", "deep/path/cache/file.txt"]
    for rel_path in cache_files:
        file_path = base_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(f"Content of {rel_path}\n")
        files_created["should_exclude_by_pattern"].append(file_path)

    return files_created


def create_pydrignore(base_dir: Path, patterns: list[str]) -> Path:
    """Create a .pydrignore file with the given patterns."""
    ignore_file = base_dir / DEFAULT_IGNORE_FILE_NAME
    content = "\n".join(patterns)
    ignore_file.write_text(content)
    return ignore_file


def test_basic_patterns(base_dir: Path) -> bool:
    """Test 1: Basic patterns - *.log, *.dat ignore patterns."""
    print("\n" + "=" * 80)
    print("TEST 1: BASIC PATTERNS - *.dat extension ignore")
    print("=" * 80)

    # Create test files
    (base_dir / "data.txt").write_text("keep this")
    (base_dir / "debug.dat").write_text("ignore this")
    (base_dir / "error.dat").write_text("ignore this too")

    # Create .pydrignore
    create_pydrignore(base_dir, ["# Ignore all .dat files", "*.dat"])

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "data.txt" not in paths:
        print("[FAIL] data.txt should be included")
        return False
    if "debug.dat" in paths or "error.dat" in paths:
        print("[FAIL] .dat files should be excluded")
        return False

    print("[PASS] Basic patterns work correctly")
    return True


def test_negation(base_dir: Path) -> bool:
    """Test 2: Negation - !important.log to un-ignore specific files."""
    print("\n" + "=" * 80)
    print("TEST 2: NEGATION - Un-ignore specific files with !")
    print("=" * 80)

    # Create test files
    (base_dir / "debug.log").write_text("ignore this")
    (base_dir / "error.log").write_text("ignore this")
    (base_dir / "important.log").write_text("keep this!")

    # Create .pydrignore with negation
    create_pydrignore(
        base_dir,
        [
            "# Ignore all .log files",
            "*.log",
            "# But keep important.log",
            "!important.log",
        ],
    )

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "debug.log" in paths or "error.log" in paths:
        print("[FAIL] debug.log and error.log should be excluded")
        return False
    if "important.log" not in paths:
        print("[FAIL] important.log should be included (negated)")
        return False

    print("[PASS] Negation works correctly")
    return True


def test_directory_patterns(base_dir: Path) -> bool:
    """Test 3: Directory patterns - temp/ to ignore directories."""
    print("\n" + "=" * 80)
    print("TEST 3: DIRECTORY PATTERNS - Ignore entire directories")
    print("=" * 80)

    # Create directory structure
    temp_dir = base_dir / "temp"
    temp_dir.mkdir()
    (temp_dir / "cache.txt").write_text("temp cache")
    (temp_dir / "data.bin").write_text("temp data")
    (base_dir / "keep.txt").write_text("keep this")

    # Create .pydrignore
    create_pydrignore(base_dir, ["# Ignore temp directory", "temp/"])

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "keep.txt" not in paths:
        print("[FAIL] keep.txt should be included")
        return False
    if any("temp/" in p for p in paths):
        print("[FAIL] temp/ files should be excluded")
        return False

    print("[PASS] Directory patterns work correctly")
    return True


def test_hierarchical_ignore(base_dir: Path) -> bool:
    """Test 4: Hierarchical - subdirectory .pydrignore overriding parent rules."""
    print("\n" + "=" * 80)
    print("TEST 4: HIERARCHICAL - Subdirectory rules override parent")
    print("=" * 80)

    # Create directory structure
    subdir = base_dir / "subdir"
    subdir.mkdir()
    (base_dir / "root.log").write_text("ignore at root")
    (base_dir / "root_debug.log").write_text("ignore at root")
    (subdir / "sub.log").write_text("ignore in subdir")
    (subdir / "debug.log").write_text("keep this - unignored in subdir")
    (subdir / "data.txt").write_text("keep this")

    # Root .pydrignore ignores all .log
    create_pydrignore(base_dir, ["*.log"])

    # Subdir .pydrignore un-ignores debug.log
    create_pydrignore(subdir, ["!debug.log"])

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "root.log" in paths or "root_debug.log" in paths:
        print("[FAIL] root .log files should be excluded")
        return False
    if "subdir/sub.log" in paths:
        print("[FAIL] subdir/sub.log should be excluded")
        return False
    if "subdir/debug.log" not in paths:
        print("[FAIL] subdir/debug.log should be included (negated in subdir)")
        return False
    if "subdir/data.txt" not in paths:
        print("[FAIL] subdir/data.txt should be included")
        return False

    print("[PASS] Hierarchical ignore works correctly")
    return True


def test_double_wildcards(base_dir: Path) -> bool:
    """Test 5: Double wildcards - **/cache/** patterns."""
    print("\n" + "=" * 80)
    print("TEST 5: DOUBLE WILDCARDS - **/cache/** pattern")
    print("=" * 80)

    # Create nested cache directories
    (base_dir / "cache").mkdir()
    (base_dir / "cache" / "data.bin").write_text("cache at root")
    (base_dir / "src").mkdir()
    (base_dir / "src" / "cache").mkdir()
    (base_dir / "src" / "cache" / "nested.bin").write_text("nested cache")
    (base_dir / "deep" / "path" / "cache").mkdir(parents=True)
    (base_dir / "deep" / "path" / "cache" / "file.txt").write_text("deep cache")
    (base_dir / "keep.txt").write_text("keep this")
    (base_dir / "src" / "main.py").write_text("keep this too")

    # Create .pydrignore
    create_pydrignore(base_dir, ["# Ignore all cache directories", "**/cache/**"])

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "keep.txt" not in paths:
        print("[FAIL] keep.txt should be included")
        return False
    if "src/main.py" not in paths:
        print("[FAIL] src/main.py should be included")
        return False
    if any("cache" in p for p in paths):
        found = [p for p in paths if "cache" in p]
        print(f"[FAIL] cache files should be excluded: {found}")
        return False

    print("[PASS] Double wildcards work correctly")
    return True


def test_anchored_patterns(base_dir: Path) -> bool:
    """Test 6: Anchored patterns - /logs for root-only matching."""
    print("\n" + "=" * 80)
    print("TEST 6: ANCHORED PATTERNS - /logs matches only at root")
    print("=" * 80)

    # Create logs at different levels
    (base_dir / "logs").mkdir()
    (base_dir / "logs" / "root.log").write_text("ignore - at root")
    (base_dir / "subdir").mkdir()
    (base_dir / "subdir" / "logs").mkdir()
    (base_dir / "subdir" / "logs" / "sub.log").write_text("keep - not at root")
    (base_dir / "keep.txt").write_text("keep this")

    # Create .pydrignore with anchored pattern
    create_pydrignore(base_dir, ["# Ignore logs only at root", "/logs/*"])

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = [f.relative_path for f in files]

    print(f"\n[RESULT] Found {len(files)} files: {paths}")

    # Verify
    if "keep.txt" not in paths:
        print("[FAIL] keep.txt should be included")
        return False
    if "logs/root.log" in paths:
        print("[FAIL] logs/root.log should be excluded (at root)")
        return False
    if "subdir/logs/sub.log" not in paths:
        print("[FAIL] subdir/logs/sub.log should be included (not at root)")
        return False

    print("[PASS] Anchored patterns work correctly")
    return True


def test_kopia_example(base_dir: Path) -> bool:
    """Test 7: Full Kopia documentation example."""
    print("\n" + "=" * 80)
    print("TEST 7: KOPIA EXAMPLE - Full thesis directory example")
    print("=" * 80)

    # Create Kopia's thesis example structure
    create_test_directory(base_dir)

    # Create .pydrignore matching Kopia example
    create_pydrignore(
        base_dir,
        [
            "# Ignoring all files that end with .dat",
            "*.dat",
            "",
            "# Ignoring all files and folders within thesis/logs directory",
            "/logs/*",
            "",
            "# Ignoring tmp.db files within the whole directory",
            "tmp.db",
            "",
            "# Ignore all cache directories",
            "**/cache/**",
        ],
    )

    # Scan
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    paths = sorted(f.relative_path for f in files)

    print(f"\n[RESULT] Found {len(files)} files:")
    for p in paths:
        print(f"  - {p}")

    # Count expected inclusions/exclusions
    expected_includes = [
        "title.png",
        "manuscript.tex",
        "atmp.db",
        "abtmp.db",
        "figures/architecture.png",
        "figures/server.png",
        "chapters/introduction.tex",
        "chapters/abstract.tex",
        "chapters/conclusion.tex",
        "chapters/logs/chapter.log",
        "src/main.py",
    ]

    # Verify includes
    missing = []
    for expected in expected_includes:
        if expected not in paths:
            missing.append(expected)

    if missing:
        print(f"\n[FAIL] Missing expected files: {missing}")
        return False

    # Verify excludes
    unexpected = []
    for p in paths:
        if p.endswith(".dat"):
            unexpected.append(p)
        elif p == "tmp.db" or p.endswith("/tmp.db"):
            unexpected.append(p)
        elif p.startswith("logs/"):
            unexpected.append(p)
        elif "/cache/" in p or p.startswith("cache/"):
            unexpected.append(p)

    if unexpected:
        print(f"\n[FAIL] Unexpected files found: {unexpected}")
        return False

    print("[PASS] Kopia example works correctly")
    return True


def test_performance(base_dir: Path, file_count: int = 1000) -> bool:
    """Test 8: Performance - scanning with many files and complex patterns."""
    print("\n" + "=" * 80)
    print(f"TEST 8: PERFORMANCE - Scanning {file_count} files with complex patterns")
    print("=" * 80)

    # Create many files in various directories
    print(f"\n[SETUP] Creating {file_count} test files...")
    start_create = time.time()

    extensions = [".txt", ".log", ".tmp", ".dat", ".py", ".md", ".json", ".xml"]
    dirs = ["", "src/", "src/sub/", "logs/", "temp/", "cache/", "docs/", "tests/"]

    for i in range(file_count):
        ext = extensions[i % len(extensions)]
        dir_prefix = dirs[i % len(dirs)]
        rel_path = f"{dir_prefix}file_{i:04d}{ext}"
        file_path = base_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(f"Content {i}\n")

    create_time = time.time() - start_create
    print(f"  Created {file_count} files in {create_time:.2f}s")

    # Create complex .pydrignore
    create_pydrignore(
        base_dir,
        [
            "# Complex ignore patterns",
            "*.log",
            "*.tmp",
            "*.dat",
            "/logs/*",
            "temp/",
            "**/cache/**",
            "!important.log",
            "[0-9]*.xml",
            "file_000?.txt",
        ],
    )

    # Benchmark scanning
    print("\n[BENCHMARK] Scanning with ignore patterns...")
    start_scan = time.time()
    scanner = DirectoryScanner(use_ignore_files=True)
    files = scanner.scan_source(base_dir)
    scan_time = time.time() - start_scan

    print(f"  Scanned and filtered to {len(files)} files in {scan_time:.3f}s")
    print(f"  Files per second: {file_count / scan_time:.0f}")

    # Verify some files are excluded
    paths = [f.relative_path for f in files]
    excluded_count = file_count - len(files)
    print(
        f"  Excluded {excluded_count} files ({100 * excluded_count / file_count:.1f}%)"
    )

    # Sanity checks
    log_files = [p for p in paths if p.endswith(".log")]
    if log_files:
        print(f"[WARN] Found {len(log_files)} .log files (should be excluded)")

    # Performance threshold (should be fast for 1000 files)
    if scan_time > 5.0:
        print(f"[WARN] Scan took {scan_time:.2f}s - may be slow")

    print("[PASS] Performance test completed")
    return True


def main() -> None:
    """Main benchmark function for .pydrignore functionality."""
    print("\n" + "=" * 80)
    print("PYDRIME BENCHMARK: .pydrignore IGNORE FILE FUNCTIONALITY")
    print("=" * 80)
    print("\nTests gitignore-style ignore file feature similar to Kopia's .kopiaignore")
    print(f"Ignore file name: {DEFAULT_IGNORE_FILE_NAME}")

    # Track results
    results = {}
    test_dirs = []

    try:
        # Test 1: Basic patterns
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test1_"))
        test_dirs.append(test_dir)
        results["basic_patterns"] = test_basic_patterns(test_dir)

        # Test 2: Negation
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test2_"))
        test_dirs.append(test_dir)
        results["negation"] = test_negation(test_dir)

        # Test 3: Directory patterns
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test3_"))
        test_dirs.append(test_dir)
        results["directory_patterns"] = test_directory_patterns(test_dir)

        # Test 4: Hierarchical
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test4_"))
        test_dirs.append(test_dir)
        results["hierarchical"] = test_hierarchical_ignore(test_dir)

        # Test 5: Double wildcards
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test5_"))
        test_dirs.append(test_dir)
        results["double_wildcards"] = test_double_wildcards(test_dir)

        # Test 6: Anchored patterns
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test6_"))
        test_dirs.append(test_dir)
        results["anchored_patterns"] = test_anchored_patterns(test_dir)

        # Test 7: Kopia example
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test7_"))
        test_dirs.append(test_dir)
        results["kopia_example"] = test_kopia_example(test_dir)

        # Test 8: Performance
        test_dir = Path(tempfile.mkdtemp(prefix="pydrignore_test8_"))
        test_dirs.append(test_dir)
        results["performance"] = test_performance(test_dir, file_count=1000)

        # Summary
        print("\n" + "=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)

        passed = sum(1 for v in results.values() if v)
        total = len(results)

        for name, passed_test in results.items():
            status = "[PASS]" if passed_test else "[FAIL]"
            print(f"  {status} {name}")

        print(f"\nTotal: {passed}/{total} tests passed")

        if passed == total:
            print("\n" + "=" * 80)
            print("ALL TESTS PASSED FOR .pydrignore FUNCTIONALITY")
            print("=" * 80)
        else:
            print("\n[FAIL] Some tests failed")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n[ABORT] Benchmark interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup
        print("\n[CLEANUP] Removing temporary directories...")
        for test_dir in test_dirs:
            if test_dir.exists():
                shutil.rmtree(test_dir)
        print("  Done")


if __name__ == "__main__":
    main()
