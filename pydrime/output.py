"""Output formatting utilities for Drime CLI."""

import json
import shutil
import sys
from typing import Any, Optional

from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.syntax import Syntax

from .utils import format_size as _format_size


class OutputFormatter:
    """Handle different output formats for CLI commands."""

    def __init__(
        self, json_output: bool = False, quiet: bool = False, no_color: bool = False
    ):
        """Initialize output formatter.

        Args:
            json_output: Output in JSON format
            quiet: Suppress non-essential output
            no_color: Disable colored output
        """
        self.json_output = json_output
        self.quiet = quiet
        self.no_color = no_color or not sys.stdout.isatty()
        self.console = Console(
            stderr=False,
            force_terminal=not self.no_color,
            no_color=self.no_color,
        )
        self.console_err = Console(
            stderr=True,
            force_terminal=not self.no_color,
            no_color=self.no_color,
        )

    def print(self, message: str, file: Any = None) -> None:
        """Print a message (respects quiet mode).

        Args:
            message: Message to print
            file: Output file (default: stdout)
        """
        if not self.quiet:
            self.console.print(message)

    def error(self, message: str) -> None:
        """Print an error message to stderr.

        Args:
            message: Error message
        """
        self.console_err.print(f"[bold red]Error:[/bold red] {escape(message)}")

    def warning(self, message: str) -> None:
        """Print a warning message to stderr.

        Args:
            message: Warning message
        """
        if not self.quiet:
            self.console_err.print(
                f"[bold yellow]Warning:[/bold yellow] {escape(message)}"
            )

    def success(self, message: str) -> None:
        """Print a success message.

        Args:
            message: Success message
        """
        if not self.quiet:
            self.console.print(f"[bold green]{escape(message)}[/bold green]")

    def info(self, message: str) -> None:
        """Print an info message.

        Args:
            message: Info message
        """
        if not self.quiet:
            self.console_err.print(f"[cyan]{escape(message)}[/cyan]")

    def output_json(self, data: Any) -> None:
        """Output data as JSON.

        Args:
            data: Data to output
        """
        json_str = json.dumps(data, indent=2)
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=False)
        self.console.print(syntax)

    def output_table(
        self,
        data: list[dict],
        columns: list[str],
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Output data as a table in the specified format.

        Args:
            data: List of dictionaries containing row data
            columns: List of column keys to display
            headers: Optional mapping of column keys to display names
        """
        if not data:
            if self.json_output:
                self.output_json([])
            return

        if self.json_output:
            self.output_json(data)
            return

        # Text format - simple output
        self._output_text_simple(data)

    def _output_text_simple(self, data: list[dict]) -> None:
        """Output data as simple text (just names, like ls).

        Args:
            data: List of dictionaries containing row data
        """
        # Check if this data has a 'name' field
        if not data or "name" not in data[0]:
            # Fall back to simple columnar output
            self._output_text_columns(data)
            return

        # Check if data has multiple important columns beyond typical
        # file listing columns. For file listings, we typically have:
        # id, name, type, size, hash
        # For other data (like workspaces), we have: id, name, role, owner, etc.
        typical_file_keys = {"id", "name", "type", "size", "hash"}
        actual_keys = set(data[0].keys())

        # If there are keys beyond typical file listing keys, show columns
        extra_keys = actual_keys - typical_file_keys
        if extra_keys:
            # This has non-file-listing columns, show full table
            self._output_text_columns(data)
            return

        # Get terminal width for column formatting
        term_width = shutil.get_terminal_size().columns

        # Extract names and apply colors based on type
        names = []
        for row in data:
            name = row.get("name", "")
            file_type = row.get("type", "").lower()

            # Apply colors like ls does
            if file_type == "folder" or file_type == "directory":
                colored_name = f"[bold blue]{name}[/bold blue]"
            elif name.endswith((".py", ".sh", ".exe", ".bat")):
                colored_name = f"[bold green]{name}[/bold green]"
            else:
                colored_name = name

            names.append(colored_name)

        # Calculate column width based on longest name (without color codes)
        max_len = max(len(row.get("name", "")) for row in data) if data else 0
        col_width = max_len + 2  # Add padding

        # Calculate number of columns that fit in terminal
        num_cols = max(1, term_width // col_width)

        # Print names in columns
        output_lines = []
        for i in range(0, len(names), num_cols):
            row_names = names[i : i + num_cols]
            # Pad each name to column width (need to account for color codes)
            padded = []
            for j, colored_name in enumerate(row_names):
                # Get the actual name without colors for padding calculation
                actual_name = data[i + j].get("name", "")
                padding = " " * (col_width - len(actual_name))
                padded.append(colored_name + padding)
            output_lines.append("".join(padded))

        # Print all lines at once
        for line in output_lines:
            self.console.print(line.rstrip())

    def _output_text_columns(self, data: list[dict]) -> None:
        """Output data as simple columns (fallback for non-file data).

        Args:
            data: List of dictionaries containing row data
        """
        if not data:
            return

        # Get all keys from first row
        keys = list(data[0].keys())

        # Calculate column widths
        widths = {key: len(str(key)) for key in keys}
        for row in data:
            for key in keys:
                value = str(row.get(key, ""))
                widths[key] = max(widths[key], len(value))

        # Print header
        header_parts = [str(key).ljust(widths[key]) for key in keys]
        self.console.print("  ".join(header_parts))

        # Print separator
        separator_parts = ["-" * widths[key] for key in keys]
        self.console.print("  ".join(separator_parts))

        # Print rows
        for row in data:
            row_parts = [str(row.get(key, "")).ljust(widths[key]) for key in keys]
            self.console.print("  ".join(row_parts))

    def format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format.

        Args:
            size_bytes: Size in bytes

        Returns:
            Formatted size string
        """
        return _format_size(size_bytes)

    def progress_message(self, message: str) -> None:
        """Print a progress message to stderr.

        Args:
            message: Progress message
        """
        if not self.quiet:
            self.console_err.print(f"[blue]{escape(message)}[/blue]")

    def print_summary(self, title: str, items: list[tuple[str, str]]) -> None:
        """Print a summary section.

        Args:
            title: Summary title
            items: List of (key, value) tuples
        """
        if self.quiet:
            return

        # Create a formatted summary panel
        summary_text = "\n".join(
            [f"[bold]{escape(key)}:[/bold] {escape(value)}" for key, value in items]
        )
        panel = Panel(
            summary_text,
            title=f"[bold cyan]{escape(title)}[/bold cyan]",
            border_style="cyan",
            expand=False,
        )
        self.console_err.print(panel)
