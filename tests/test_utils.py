"""Unit tests for utility functions."""

from unittest.mock import MagicMock

import pytest

from pydrime.utils import (
    RemoteFileVerificationResult,
    calculate_drime_hash,
    decode_drime_hash,
    format_size,
    glob_match,
    glob_match_entries,
    glob_to_regex,
    is_file_id,
    is_glob_pattern,
    normalize_to_hash,
    parse_iso_timestamp,
    verify_remote_files_have_users,
)


class TestCalculateDrimeHash:
    """Tests for calculate_drime_hash function."""

    def test_basic_hash_calculation(self):
        """Test basic hash calculation with known values."""
        assert calculate_drime_hash(480424796) == "NDgwNDI0Nzk2fA"
        assert calculate_drime_hash(480424802) == "NDgwNDI0ODAyfA"
        assert calculate_drime_hash(480432024) == "NDgwNDMyMDI0fA"

    def test_small_id(self):
        """Test hash calculation with small ID."""
        result = calculate_drime_hash(123)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_large_id(self):
        """Test hash calculation with large ID."""
        result = calculate_drime_hash(999999999999)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_zero_id(self):
        """Test hash calculation with zero ID."""
        result = calculate_drime_hash(0)
        assert isinstance(result, str)
        assert len(result) > 0


class TestDecodeDrimeHash:
    """Tests for decode_drime_hash function."""

    def test_basic_hash_decoding(self):
        """Test basic hash decoding with known values."""
        assert decode_drime_hash("NDgwNDI0Nzk2fA") == 480424796
        assert decode_drime_hash("NDgwNDI0ODAyfA") == 480424802
        assert decode_drime_hash("NDgwNDMyMDI0fA") == 480432024

    def test_roundtrip_conversion(self):
        """Test that encoding and decoding are reversible."""
        test_ids = [123, 480424796, 999999999, 1]
        for file_id in test_ids:
            hash_value = calculate_drime_hash(file_id)
            decoded_id = decode_drime_hash(hash_value)
            assert decoded_id == file_id

    def test_hash_with_padding(self):
        """Test decoding hash that needs padding."""
        # The function should handle hashes with or without padding
        hash_without_padding = "NDgwNDI0Nzk2fA"
        assert decode_drime_hash(hash_without_padding) == 480424796

    def test_invalid_hash_raises_error(self):
        """Test that invalid hash raises ValueError."""
        with pytest.raises(ValueError, match="Invalid Drime hash"):
            decode_drime_hash("invalid!@#$%")

    def test_empty_hash_raises_error(self):
        """Test that empty hash raises ValueError."""
        with pytest.raises(ValueError):
            decode_drime_hash("")

    def test_non_base64_hash_raises_error(self):
        """Test that non-base64 string raises ValueError."""
        with pytest.raises(ValueError):
            decode_drime_hash("this is not base64!@#")


class TestIsFileId:
    """Tests for is_file_id function."""

    def test_numeric_strings_are_ids(self):
        """Test that numeric strings are identified as IDs."""
        assert is_file_id("480424796") is True
        assert is_file_id("123") is True
        assert is_file_id("0") is True
        assert is_file_id("999999999") is True

    def test_alphanumeric_strings_are_not_ids(self):
        """Test that alphanumeric strings are not identified as IDs."""
        assert is_file_id("NDgwNDI0Nzk2fA") is False
        assert is_file_id("abc123") is False
        assert is_file_id("123abc") is False
        assert is_file_id("hash") is False

    def test_special_characters_are_not_ids(self):
        """Test that strings with special chars are not IDs."""
        assert is_file_id("123-456") is False
        assert is_file_id("123.456") is False
        assert is_file_id("123_456") is False
        assert is_file_id("123 456") is False

    def test_empty_string_is_not_id(self):
        """Test that empty string is not an ID."""
        assert is_file_id("") is False

    def test_negative_numbers_are_not_ids(self):
        """Test that negative numbers are not identified as IDs."""
        assert is_file_id("-123") is False

    def test_float_strings_are_not_ids(self):
        """Test that float strings are not identified as IDs."""
        assert is_file_id("123.45") is False


class TestNormalizeToHash:
    """Tests for normalize_to_hash function."""

    def test_id_is_converted_to_hash(self):
        """Test that numeric ID is converted to hash."""
        result = normalize_to_hash("480424796")
        assert result == "NDgwNDI0Nzk2fA"

    def test_hash_is_returned_unchanged(self):
        """Test that hash is returned unchanged."""
        hash_value = "NDgwNDI0Nzk2fA"
        result = normalize_to_hash(hash_value)
        assert result == hash_value

    def test_multiple_ids(self):
        """Test normalization of multiple IDs."""
        assert normalize_to_hash("480424796") == "NDgwNDI0Nzk2fA"
        assert normalize_to_hash("480424802") == "NDgwNDI0ODAyfA"
        assert normalize_to_hash("480432024") == "NDgwNDMyMDI0fA"

    def test_mixed_inputs(self):
        """Test that function handles both IDs and hashes correctly."""
        # ID input
        id_result = normalize_to_hash("480424796")
        assert id_result == "NDgwNDI0Nzk2fA"

        # Hash input
        hash_input = "NDgwNDI0Nzk2fA"
        hash_result = normalize_to_hash(hash_input)
        assert hash_result == hash_input

    def test_small_id_normalization(self):
        """Test normalization of small ID."""
        result = normalize_to_hash("123")
        assert isinstance(result, str)
        # Verify it can be decoded back
        assert decode_drime_hash(result) == 123

    def test_zero_id_normalization(self):
        """Test normalization of zero ID."""
        result = normalize_to_hash("0")
        assert isinstance(result, str)
        assert decode_drime_hash(result) == 0


class TestIntegration:
    """Integration tests for utility functions."""

    def test_full_workflow_with_id(self):
        """Test complete workflow: ID -> hash -> download -> decode."""
        file_id = 480424796

        # Convert ID to hash
        hash_value = calculate_drime_hash(file_id)
        assert hash_value == "NDgwNDI0Nzk2fA"

        # Verify we can decode it back
        decoded_id = decode_drime_hash(hash_value)
        assert decoded_id == file_id

        # Test normalize function
        normalized = normalize_to_hash(str(file_id))
        assert normalized == hash_value

    def test_idempotent_normalization(self):
        """Test that normalizing multiple times is idempotent."""
        hash_value = "NDgwNDI0Nzk2fA"

        # Normalizing a hash multiple times should return the same value
        result1 = normalize_to_hash(hash_value)
        result2 = normalize_to_hash(result1)
        result3 = normalize_to_hash(result2)

        assert result1 == result2 == result3 == hash_value

    def test_various_id_sizes(self):
        """Test with various ID sizes."""
        test_cases = [
            (1, "MXw"),
            (12, "MTJ8"),
            (123, "MTIzfA"),
            (1234, "MTIzNHw"),
            (12345, "MTIzNDV8"),
            (123456, "MTIzNDU2fA"),
        ]

        for file_id, expected_hash in test_cases:
            # Calculate hash
            calculated_hash = calculate_drime_hash(file_id)
            assert calculated_hash == expected_hash

            # Verify roundtrip
            decoded = decode_drime_hash(calculated_hash)
            assert decoded == file_id

            # Test normalization
            normalized = normalize_to_hash(str(file_id))
            assert normalized == expected_hash


class TestIsGlobPattern:
    """Tests for is_glob_pattern function."""

    def test_asterisk_is_glob(self):
        """Test that * is recognized as a glob pattern."""
        assert is_glob_pattern("*.txt") is True
        assert is_glob_pattern("file*") is True
        assert is_glob_pattern("*") is True
        assert is_glob_pattern("bench*") is True
        assert is_glob_pattern("*test*") is True

    def test_question_mark_is_glob(self):
        """Test that ? is recognized as a glob pattern."""
        assert is_glob_pattern("file?.txt") is True
        assert is_glob_pattern("?file") is True
        assert is_glob_pattern("???") is True

    def test_bracket_is_glob(self):
        """Test that [] is recognized as a glob pattern."""
        assert is_glob_pattern("[abc].txt") is True
        assert is_glob_pattern("file[0-9].txt") is True
        assert is_glob_pattern("[!abc]file") is True

    def test_plain_names_are_not_glob(self):
        """Test that plain filenames are not glob patterns."""
        assert is_glob_pattern("file.txt") is False
        assert is_glob_pattern("my_document") is False
        assert is_glob_pattern("test123") is False
        assert is_glob_pattern("benchmark.py") is False
        assert is_glob_pattern("") is False

    def test_paths_without_glob_chars(self):
        """Test that paths without glob chars are not patterns."""
        assert is_glob_pattern("folder/file.txt") is False
        assert is_glob_pattern("a/b/c/d.txt") is False


class TestGlobMatch:
    """Tests for glob_match function."""

    def test_asterisk_matches_any_sequence(self):
        """Test that * matches any sequence of characters."""
        assert glob_match("*.txt", "file.txt") is True
        assert glob_match("*.txt", "document.txt") is True
        assert glob_match("*.txt", "file.py") is False
        assert glob_match("bench*", "benchmark.py") is True
        assert glob_match("bench*", "benchmark_test.py") is True
        assert glob_match("bench*", "test.py") is False
        assert glob_match("*test*", "my_test_file.py") is True

    def test_question_mark_matches_single_char(self):
        """Test that ? matches exactly one character."""
        assert glob_match("file?.txt", "file1.txt") is True
        assert glob_match("file?.txt", "file2.txt") is True
        assert glob_match("file?.txt", "file12.txt") is False
        assert glob_match("file?.txt", "file.txt") is False
        assert glob_match("???.txt", "abc.txt") is True
        assert glob_match("???.txt", "ab.txt") is False

    def test_bracket_matches_character_set(self):
        """Test that [seq] matches any character in seq."""
        assert glob_match("[abc].txt", "a.txt") is True
        assert glob_match("[abc].txt", "b.txt") is True
        assert glob_match("[abc].txt", "d.txt") is False
        assert glob_match("file[0-9].txt", "file1.txt") is True
        assert glob_match("file[0-9].txt", "file9.txt") is True
        assert glob_match("file[0-9].txt", "filea.txt") is False
        assert glob_match("[a-z]*.py", "api.py") is True
        assert glob_match("[a-z]*.py", "Api.py") is False

    def test_negated_bracket(self):
        """Test that [!seq] matches any character not in seq."""
        assert glob_match("[!abc].txt", "d.txt") is True
        assert glob_match("[!abc].txt", "a.txt") is False

    def test_case_sensitivity(self):
        """Test that glob matching is case-sensitive."""
        assert glob_match("*.TXT", "file.TXT") is True
        assert glob_match("*.TXT", "file.txt") is False
        assert glob_match("File*", "File.txt") is True
        assert glob_match("File*", "file.txt") is False

    def test_exact_match_without_glob(self):
        """Test that patterns without glob chars match exactly."""
        assert glob_match("file.txt", "file.txt") is True
        assert glob_match("file.txt", "other.txt") is False

    def test_empty_pattern_and_name(self):
        """Test empty pattern and name handling."""
        assert glob_match("", "") is True
        assert glob_match("*", "") is True
        assert glob_match("", "file") is False


class TestGlobToRegex:
    """Tests for glob_to_regex function."""

    def test_compiles_to_regex(self):
        """Test that glob pattern compiles to regex."""
        import re

        regex = glob_to_regex("*.txt")
        assert isinstance(regex, re.Pattern)

    def test_regex_matches_correctly(self):
        """Test that compiled regex matches correctly."""
        regex = glob_to_regex("*.txt")
        assert regex.match("file.txt") is not None
        assert regex.match("document.txt") is not None
        assert regex.match("file.py") is None

    def test_complex_pattern(self):
        """Test complex pattern conversion."""
        regex = glob_to_regex("test_[0-9]*.py")
        assert regex.match("test_1.py") is not None
        assert regex.match("test_123.py") is not None
        assert regex.match("test_abc.py") is None

    def test_question_mark_regex(self):
        """Test that ? converts correctly to regex."""
        regex = glob_to_regex("file?.txt")
        assert regex.match("file1.txt") is not None
        assert regex.match("file12.txt") is None


class TestParseIsoTimestamp:
    """Tests for parse_iso_timestamp function."""

    def test_parse_valid_iso_timestamp_with_z(self):
        """Test parsing ISO timestamp with Z suffix."""
        result = parse_iso_timestamp("2025-01-15T10:30:00Z")
        assert result is not None
        # Check it's a datetime (exact time depends on local timezone)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_parse_valid_iso_timestamp_with_microseconds(self):
        """Test parsing ISO timestamp with microseconds."""
        result = parse_iso_timestamp("2025-01-15T10:30:00.123456Z")
        assert result is not None
        assert result.year == 2025

    def test_parse_timestamp_with_timezone(self):
        """Test parsing ISO timestamp with timezone offset."""
        result = parse_iso_timestamp("2025-01-15T10:30:00+02:00")
        assert result is not None
        assert result.year == 2025

    def test_parse_timestamp_without_timezone(self):
        """Test parsing ISO timestamp without timezone."""
        result = parse_iso_timestamp("2025-01-15T10:30:00")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1

    def test_parse_none_returns_none(self):
        """Test that None input returns None."""
        result = parse_iso_timestamp(None)
        assert result is None

    def test_parse_empty_string_returns_none(self):
        """Test that empty string returns None."""
        result = parse_iso_timestamp("")
        assert result is None

    def test_parse_invalid_timestamp_returns_none(self):
        """Test that invalid timestamp returns None."""
        result = parse_iso_timestamp("not a timestamp")
        assert result is None

    def test_parse_invalid_format_returns_none(self):
        """Test that invalid format returns None."""
        result = parse_iso_timestamp("2025/01/15 10:30:00")
        assert result is None

    def test_parse_timestamp_with_malformed_data(self):
        """Test parsing timestamp with various malformed formats."""
        # The function is quite forgiving, so test cases that actually fail
        # Completely invalid format
        result = parse_iso_timestamp("not-a-timestamp")
        assert result is None

        # Valid formats that should parse
        result = parse_iso_timestamp("2025-01-15T10:30:00.123456Z")
        assert result is not None


class TestFormatSize:
    """Tests for format_size function."""

    def test_format_bytes(self):
        """Test formatting sizes less than 1 KB."""
        assert format_size(0) == "0 B"
        assert format_size(100) == "100 B"
        assert format_size(1023) == "1023 B"

    def test_format_kilobytes(self):
        """Test formatting sizes in KB range."""
        assert format_size(1024) == "1.00 KB"
        assert format_size(2048) == "2.00 KB"
        assert format_size(1536) == "1.50 KB"

    def test_format_megabytes(self):
        """Test formatting sizes in MB range."""
        assert format_size(1024 * 1024) == "1.00 MB"
        assert format_size(1024 * 1024 * 2) == "2.00 MB"
        assert format_size(int(1024 * 1024 * 1.5)) == "1.50 MB"

    def test_format_gigabytes(self):
        """Test formatting sizes in GB range."""
        assert format_size(1024 * 1024 * 1024) == "1.00 GB"
        assert format_size(1024 * 1024 * 1024 * 2) == "2.00 GB"


class TestRemoteFileVerificationResult:
    """Tests for RemoteFileVerificationResult class."""

    def test_basic_initialization(self):
        """Test basic initialization."""
        result = RemoteFileVerificationResult(
            all_verified=True,
            verified_count=5,
            total_count=5,
        )
        assert result.all_verified is True
        assert result.verified_count == 5
        assert result.total_count == 5
        assert result.expected_count is None
        assert result.unverified_files == []
        assert result.errors == []

    def test_initialization_with_all_params(self):
        """Test initialization with all parameters."""
        result = RemoteFileVerificationResult(
            all_verified=False,
            verified_count=3,
            total_count=5,
            expected_count=10,
            unverified_files=["file1.txt", "file2.txt"],
            errors=["Error 1", "Error 2"],
        )
        assert result.all_verified is False
        assert result.verified_count == 3
        assert result.total_count == 5
        assert result.expected_count == 10
        assert result.unverified_files == ["file1.txt", "file2.txt"]
        assert result.errors == ["Error 1", "Error 2"]

    def test_bool_returns_all_verified(self):
        """Test that bool() returns all_verified value."""
        result_true = RemoteFileVerificationResult(True, 5, 5)
        result_false = RemoteFileVerificationResult(False, 3, 5)

        assert bool(result_true) is True
        assert bool(result_false) is False

        # Can use in if statements
        if result_true:
            pass  # Should enter
        else:
            pytest.fail("Should have been truthy")

        if not result_false:
            pass  # Should enter
        else:
            pytest.fail("Should have been falsy")

    def test_repr(self):
        """Test string representation."""
        result = RemoteFileVerificationResult(True, 5, 10)
        repr_str = repr(result)

        assert "RemoteFileVerificationResult" in repr_str
        assert "verified=5/10" in repr_str
        assert "all_verified=True" in repr_str


class TestGlobMatchEntries:
    """Tests for glob_match_entries function."""

    def test_filter_entries_by_pattern(self):
        """Test filtering entries by glob pattern."""
        # Create mock FileEntry objects
        entries = []
        for name in ["file1.txt", "file2.txt", "data.csv", "readme.md"]:
            entry = MagicMock()
            entry.name = name
            entries.append(entry)

        # Filter for .txt files
        result = glob_match_entries("*.txt", entries)
        assert len(result) == 2
        assert all(e.name.endswith(".txt") for e in result)

    def test_filter_no_matches(self):
        """Test filtering with no matches."""
        entries = []
        for name in ["file1.txt", "file2.txt"]:
            entry = MagicMock()
            entry.name = name
            entries.append(entry)

        result = glob_match_entries("*.csv", entries)
        assert len(result) == 0

    def test_filter_all_match(self):
        """Test filtering where all entries match."""
        entries = []
        for name in ["test1.py", "test2.py", "test_utils.py"]:
            entry = MagicMock()
            entry.name = name
            entries.append(entry)

        result = glob_match_entries("*.py", entries)
        assert len(result) == 3

    def test_filter_empty_entries(self):
        """Test filtering empty entries list."""
        result = glob_match_entries("*.txt", [])
        assert len(result) == 0


class TestVerifyRemoteFilesHaveUsers:
    """Tests for verify_remote_files_have_users function."""

    def test_folder_not_found(self):
        """Test when remote folder is not found."""
        mock_client = MagicMock()
        mock_client.get_file_entries.return_value = {"data": []}

        result = verify_remote_files_have_users(
            mock_client, "nonexistent_folder", verbose=False
        )

        assert result.all_verified is False
        assert "Folder not found" in result.errors[0]

    def test_no_files_in_folder(self):
        """Test when folder exists but has no files."""
        mock_client = MagicMock()

        # First call finds the folder
        folder_entry = {
            "name": "test_folder",
            "hash": "abc123",
            "type": "folder",
            "id": 1,
        }
        mock_client.get_file_entries.side_effect = [
            {"data": [folder_entry]},  # First call - folder search
            {"data": []},  # Second call - folder contents
        ]

        result = verify_remote_files_have_users(
            mock_client, "test_folder", verbose=False
        )

        assert result.all_verified is False
        assert "No files found" in result.errors[0]

    def test_all_files_verified(self):
        """Test when all files are verified."""
        mock_client = MagicMock()

        folder_entry = {
            "name": "test_folder",
            "hash": "abc123",
            "type": "folder",
            "id": 1,
        }
        file_entry = {
            "name": "test.txt",
            "hash": "file123",
            "type": "file",
            "id": 2,
            "file_size": 100,
            "users": [{"id": 1, "name": "user1"}],
        }

        mock_client.get_file_entries.side_effect = [
            {"data": [folder_entry]},  # First call - folder search
            {"data": [file_entry]},  # Second call - folder contents
        ]

        result = verify_remote_files_have_users(
            mock_client, "test_folder", verbose=False
        )

        assert result.all_verified is True
        assert result.verified_count == 1
        assert result.total_count == 1

    def test_files_with_missing_users(self):
        """Test when some files are missing users."""
        mock_client = MagicMock()

        folder_entry = {
            "name": "test_folder",
            "hash": "abc123",
            "type": "folder",
            "id": 1,
        }
        verified_file = {
            "name": "verified.txt",
            "hash": "file123",
            "type": "file",
            "id": 2,
            "file_size": 100,
            "users": [{"id": 1}],
        }
        unverified_file = {
            "name": "unverified.txt",
            "hash": "file456",
            "type": "file",
            "id": 3,
            "file_size": 100,
            "users": [],  # No users
        }

        mock_client.get_file_entries.side_effect = [
            {"data": [folder_entry]},
            {"data": [verified_file, unverified_file]},
        ]

        result = verify_remote_files_have_users(
            mock_client, "test_folder", verbose=False
        )

        assert result.all_verified is False
        assert result.verified_count == 1
        assert result.total_count == 2
        assert "unverified.txt" in result.unverified_files

    def test_expected_count_mismatch(self):
        """Test when expected count doesn't match."""
        mock_client = MagicMock()

        folder_entry = {
            "name": "test_folder",
            "hash": "abc123",
            "type": "folder",
            "id": 1,
        }
        file_entry = {
            "name": "test.txt",
            "hash": "file123",
            "type": "file",
            "id": 2,
            "file_size": 100,
            "users": [{"id": 1}],
        }

        mock_client.get_file_entries.side_effect = [
            {"data": [folder_entry]},
            {"data": [file_entry]},
        ]

        result = verify_remote_files_have_users(
            mock_client, "test_folder", expected_count=5, verbose=False
        )

        assert result.all_verified is False
        assert "Expected 5 files" in result.errors[0]

    def test_api_exception(self):
        """Test when API raises an exception."""
        mock_client = MagicMock()
        mock_client.get_file_entries.side_effect = Exception("API error")

        result = verify_remote_files_have_users(
            mock_client, "test_folder", verbose=False
        )

        assert result.all_verified is False
        assert "Error verifying files" in result.errors[0]

    def test_verbose_output(self, capsys):
        """Test verbose output."""
        mock_client = MagicMock()
        mock_client.get_file_entries.return_value = {"data": []}

        verify_remote_files_have_users(mock_client, "test_folder", verbose=True)

        captured = capsys.readouterr()
        assert "VERIFY" in captured.out
