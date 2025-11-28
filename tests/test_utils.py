"""Unit tests for utility functions."""

import pytest

from pydrime.utils import (
    calculate_drime_hash,
    decode_drime_hash,
    glob_match,
    glob_to_regex,
    is_file_id,
    is_glob_pattern,
    normalize_to_hash,
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
