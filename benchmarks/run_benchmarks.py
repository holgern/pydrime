#!/usr/bin/env python3
"""
Benchmark runner for PyDrime.

Runs all benchmark scripts in the benchmarks/ directory and provides
a summary report. Optionally writes results to a markdown file.

Usage:
    python benchmarks/run_benchmarks.py                   # Run all benchmarks
    python benchmarks/run_benchmarks.py --report          # Generate markdown report
    python benchmarks/run_benchmarks.py --report FILE.md  # Specify output file
    python benchmarks/run_benchmarks.py --list            # List available benchmarks
    python benchmarks/run_benchmarks.py benchmark_ignore  # Run specific benchmark
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    success: bool
    duration: float
    output: str
    error: str = ""
    tests_passed: int = 0
    tests_total: int = 0


@dataclass
class BenchmarkSummary:
    """Summary of all benchmark runs."""

    results: list[BenchmarkResult] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None

    @property
    def total_duration(self) -> float:
        """Total duration of all benchmarks."""
        return sum(r.duration for r in self.results)

    @property
    def passed_count(self) -> int:
        """Number of passed benchmarks."""
        return sum(1 for r in self.results if r.success)

    @property
    def failed_count(self) -> int:
        """Number of failed benchmarks."""
        return sum(1 for r in self.results if not r.success)

    @property
    def all_passed(self) -> bool:
        """Whether all benchmarks passed."""
        return all(r.success for r in self.results)


def get_benchmark_files(benchmarks_dir: Path) -> list[Path]:
    """Get all benchmark files in the directory."""
    return sorted(
        f
        for f in benchmarks_dir.glob("benchmark_*.py")
        if f.name != "run_benchmarks.py"
    )


def run_benchmark(
    benchmark_file: Path,
    verbose: bool = True,
    current: int = 0,
    total: int = 0,
) -> BenchmarkResult:
    """Run a single benchmark script.

    Args:
        benchmark_file: Path to the benchmark script
        verbose: Whether to print output in real-time
        current: Current benchmark number (1-based, for progress display)
        total: Total number of benchmarks to run

    Returns:
        BenchmarkResult with success status and details
    """
    name = benchmark_file.stem.replace("benchmark_", "")
    start_time = time.time()

    if verbose:
        print(f"\n{'=' * 80}")
        progress = f"[{current}/{total}] " if current and total else ""
        print(f"{progress}RUNNING: {benchmark_file.name}")
        print(f"{'=' * 80}\n")
        sys.stdout.flush()

    try:
        # Run the benchmark script
        # Merge stderr into stdout to avoid potential deadlock from separate
        # pipe buffers filling up. This ensures all output is streamed in order.
        process = subprocess.Popen(
            [sys.executable, str(benchmark_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr into stdout
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd=benchmark_file.parent.parent,  # Run from project root
        )

        output_lines = []

        # Stream stdout (which now includes stderr) in real-time if verbose
        if process.stdout:
            for line in process.stdout:
                if verbose:
                    print(line, end="")
                    sys.stdout.flush()
                output_lines.append(line)

        exit_code = process.wait()
        duration = time.time() - start_time

        output = "".join(output_lines)
        error = ""  # stderr is now merged with stdout

        # Count tests from output
        tests_passed, tests_total = parse_test_counts(output)

        success = exit_code == 0

        if verbose:
            status = "PASSED" if success else "FAILED"
            print(f"\n[{status}] {name} completed in {duration:.2f}s")
            if error:
                print(f"STDERR: {error}")

        return BenchmarkResult(
            name=name,
            success=success,
            duration=duration,
            output=output,
            error=error,
            tests_passed=tests_passed,
            tests_total=tests_total,
        )

    except Exception as e:
        duration = time.time() - start_time
        if verbose:
            print(f"\n[ERROR] {name} failed with exception: {e}")

        return BenchmarkResult(
            name=name,
            success=False,
            duration=duration,
            output="",
            error=str(e),
        )


def parse_test_counts(output: str) -> tuple[int, int]:
    """Parse test counts from benchmark output.

    Looks for patterns like "Total: 8/8 tests passed" or "[PASS]" markers.

    Returns:
        Tuple of (passed, total)
    """
    import re

    # Look for "Total: X/Y tests passed"
    match = re.search(r"Total:\s*(\d+)/(\d+)\s*tests?\s*passed", output, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    # Count [PASS] and [FAIL] markers
    passed = len(re.findall(r"\[PASS\]", output))
    failed = len(re.findall(r"\[FAIL\]", output))

    if passed > 0 or failed > 0:
        return passed, passed + failed

    # Check for "ALL TESTS PASSED"
    if re.search(r"ALL\s*TESTS\s*PASSED", output, re.IGNORECASE):
        # Count TEST N: headers
        test_count = len(re.findall(r"TEST\s*\d+:", output))
        return test_count, test_count

    return 0, 0


def generate_markdown_report(summary: BenchmarkSummary) -> str:
    """Generate a markdown report from the summary."""
    lines = []

    # Header
    lines.append("# PyDrime Benchmark Report")
    lines.append("")
    lines.append(f"**Generated:** {summary.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Total Duration:** {summary.total_duration:.2f}s")
    lines.append("")

    # Summary table
    lines.append("## Summary")
    lines.append("")
    status = "PASSED" if summary.all_passed else "FAILED"
    status_emoji = "[PASS]" if summary.all_passed else "[FAIL]"
    lines.append(f"**Overall Status:** {status_emoji} {status}")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Benchmarks | {len(summary.results)} |")
    lines.append(f"| Passed | {summary.passed_count} |")
    lines.append(f"| Failed | {summary.failed_count} |")
    lines.append(f"| Duration | {summary.total_duration:.2f}s |")
    lines.append("")

    # Results table
    lines.append("## Results")
    lines.append("")
    lines.append("| Benchmark | Status | Duration | Tests |")
    lines.append("|-----------|--------|----------|-------|")

    for result in summary.results:
        status = "[PASS]" if result.success else "[FAIL]"
        tests = (
            f"{result.tests_passed}/{result.tests_total}" if result.tests_total else "-"
        )
        lines.append(f"| {result.name} | {status} | {result.duration:.2f}s | {tests} |")

    lines.append("")

    # Detailed results
    lines.append("## Details")
    lines.append("")

    for result in summary.results:
        status = "[PASS]" if result.success else "[FAIL]"
        lines.append(f"### {result.name} {status}")
        lines.append("")
        lines.append(f"- **Duration:** {result.duration:.2f}s")
        if result.tests_total:
            lines.append(f"- **Tests:** {result.tests_passed}/{result.tests_total}")
        lines.append("")

        if result.error:
            lines.append("**Errors:**")
            lines.append("```")
            lines.append(result.error.strip())
            lines.append("```")
            lines.append("")

        # Extract key output lines (last section with summary)
        key_output = extract_key_output(result.output)
        if key_output:
            lines.append("<details>")
            lines.append("<summary>Output Summary</summary>")
            lines.append("")
            lines.append("```")
            lines.append(key_output)
            lines.append("```")
            lines.append("</details>")
            lines.append("")

    return "\n".join(lines)


def extract_key_output(output: str) -> str:
    """Extract key output sections (summaries, results)."""
    lines = output.split("\n")

    # Find the last "SUMMARY" or "ALL TESTS" section
    result_lines = []

    for i, line in enumerate(lines):
        if "SUMMARY" in line.upper() or "ALL TESTS" in line.upper():
            # Include a few lines before for context
            start = max(0, i - 2)
            result_lines = lines[start:]
            break

    if result_lines:
        # Trim trailing empty lines
        while result_lines and not result_lines[-1].strip():
            result_lines.pop()
        return "\n".join(result_lines[-30:])  # Limit to last 30 lines

    # Fallback: return last 20 lines
    return "\n".join(lines[-20:])


def print_summary(summary: BenchmarkSummary) -> None:
    """Print a summary of the benchmark results."""
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    for result in summary.results:
        status = "[PASS]" if result.success else "[FAIL]"
        tests = (
            f" ({result.tests_passed}/{result.tests_total})"
            if result.tests_total
            else ""
        )
        print(f"  {status} {result.name}{tests} - {result.duration:.2f}s")

    print("-" * 80)
    passed = summary.passed_count
    total = len(summary.results)
    duration = summary.total_duration
    print(f"  Total: {passed}/{total} passed in {duration:.2f}s")
    print("=" * 80)

    if summary.all_passed:
        print("\n[PASS] ALL BENCHMARKS PASSED")
    else:
        print("\n[FAIL] SOME BENCHMARKS FAILED")
        for result in summary.results:
            if not result.success:
                print(f"   - {result.name}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run PyDrime benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "benchmarks",
        nargs="*",
        help="Specific benchmarks to run (without 'benchmark_' prefix)",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List available benchmarks",
    )
    parser.add_argument(
        "--report",
        "-r",
        nargs="?",
        const="benchmark_report.md",
        help="Generate markdown report (default: benchmark_report.md)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress real-time output",
    )

    args = parser.parse_args()

    # Find benchmarks directory
    script_dir = Path(__file__).parent
    benchmarks_dir = script_dir

    # Get all benchmark files
    all_benchmarks = get_benchmark_files(benchmarks_dir)

    if not all_benchmarks:
        print("No benchmark files found!")
        return 1

    # List mode
    if args.list:
        print("Available benchmarks:")
        for f in all_benchmarks:
            name = f.stem.replace("benchmark_", "")
            print(f"  - {name}")
        return 0

    # Filter benchmarks if specified
    if args.benchmarks:
        filtered = []
        for name in args.benchmarks:
            # Support both "ignore" and "benchmark_ignore" formats
            if not name.startswith("benchmark_"):
                name = f"benchmark_{name}"
            matching = [f for f in all_benchmarks if f.stem == name]
            if matching:
                filtered.extend(matching)
            else:
                print(f"Warning: Benchmark '{name}' not found")
        benchmarks_to_run = filtered
    else:
        benchmarks_to_run = all_benchmarks

    if not benchmarks_to_run:
        print("No benchmarks to run!")
        return 1

    # Run benchmarks
    print(f"\n{'=' * 80}")
    print("PYDRIME BENCHMARK RUNNER")
    print(f"{'=' * 80}")
    print(f"Running {len(benchmarks_to_run)} benchmark(s):\n")
    for i, f in enumerate(benchmarks_to_run, 1):
        print(f"  {i:2}. {f.name}")
    print()

    summary = BenchmarkSummary(start_time=datetime.now())

    total = len(benchmarks_to_run)
    for i, benchmark_file in enumerate(benchmarks_to_run, 1):
        result = run_benchmark(
            benchmark_file,
            verbose=not args.quiet,
            current=i,
            total=total,
        )
        summary.results.append(result)

    summary.end_time = datetime.now()

    # Print summary
    print_summary(summary)

    # Generate report if requested
    if args.report:
        report_path = Path(args.report)
        report_content = generate_markdown_report(summary)
        report_path.write_text(report_content)
        print(f"\n[REPORT] Report written to: {report_path}")

    return 0 if summary.all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
