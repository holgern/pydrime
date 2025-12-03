"""
Benchmark script to test Unicode normalization issues with folder names.

This script demonstrates the bug where creating folders with similar Unicode
characters (e.g., "u" and "ü") results in them being treated as duplicates
after uploading due to Unicode normalization.

The issue arises because:
- Some file systems use NFD (decomposed) form: "ü" = "u" + combining umlaut
- Others use NFC (composed) form: "ü" = single character
- Cloud storage may normalize differently than local file system

Usage:
    python benchmarks/benchmark_unicode_normalization.py
"""

import io
import logging
import os
import sys
import tempfile
import time
import unicodedata
import uuid
from pathlib import Path

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Configure logging - set to WARNING to reduce verbosity
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pydrime").setLevel(logging.WARNING)

from pydrime.api import DrimeClient  # noqa: E402

logger = logging.getLogger(__name__)


def print_separator(title: str, char: str = "=") -> None:
    """Print a separator line with title."""
    print(f"\n{char * 80}")
    print(f" {title}")
    print(f"{char * 80}")


def print_debug(msg: str, indent: int = 0) -> None:
    """Print a debug message with optional indentation."""
    prefix = "  " * indent
    print(f"{prefix}[DEBUG] {msg}")


def print_info(msg: str, indent: int = 0) -> None:
    """Print an info message."""
    prefix = "  " * indent
    print(f"{prefix}[INFO] {msg}")


def print_success(msg: str) -> None:
    """Print a success message."""
    print(f"[SUCCESS] {msg}")


def print_error(msg: str) -> None:
    """Print an error message."""
    print(f"[ERROR] {msg}")


def print_warning(msg: str) -> None:
    """Print a warning message."""
    print(f"[WARNING] {msg}")


def analyze_unicode(s: str) -> dict:
    """Analyze Unicode properties of a string.

    Args:
        s: The string to analyze

    Returns:
        Dictionary with Unicode analysis
    """
    return {
        "original": s,
        "repr": repr(s),
        "len": len(s),
        "codepoints": [f"U+{ord(c):04X}" for c in s],
        "names": [unicodedata.name(c, "UNKNOWN") for c in s],
        "nfc": unicodedata.normalize("NFC", s),
        "nfd": unicodedata.normalize("NFD", s),
        "nfc_len": len(unicodedata.normalize("NFC", s)),
        "nfd_len": len(unicodedata.normalize("NFD", s)),
        "is_nfc": unicodedata.is_normalized("NFC", s),
        "is_nfd": unicodedata.is_normalized("NFD", s),
    }


def print_unicode_analysis(name: str, s: str, indent: int = 0) -> None:
    """Print detailed Unicode analysis of a string."""
    analysis = analyze_unicode(s)
    prefix = "  " * indent
    print(f"{prefix}{name}:")
    print(f"{prefix}  Original: '{analysis['original']}' (len={analysis['len']})")
    print(f"{prefix}  Repr: {analysis['repr']}")
    print(f"{prefix}  Codepoints: {' '.join(analysis['codepoints'])}")
    print(f"{prefix}  Names: {analysis['names']}")
    print(f"{prefix}  Is NFC: {analysis['is_nfc']}, Is NFD: {analysis['is_nfd']}")
    print(f"{prefix}  NFC len: {analysis['nfc_len']}, NFD len: {analysis['nfd_len']}")


def create_test_file(
    content: bytes | None = None,
    size_kb: int = 1,
    suffix: str = ".txt",
) -> Path:
    """Create a temporary test file.

    Args:
        content: Specific content to use (if None, random content is generated)
        size_kb: Size in KB if generating random content
        suffix: File suffix

    Returns:
        Path to the created temp file
    """
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as f:
        if content is not None:
            f.write(content)
        else:
            f.write(os.urandom(size_kb * 1024))
        return Path(f.name)


def cleanup_file(file_path: Path) -> None:
    """Clean up a temporary file."""
    try:
        if file_path.exists():
            os.unlink(file_path)
    except Exception as e:
        print_warning(f"Could not delete temp file {file_path}: {e}")


def test_unicode_folder_pairs(client: DrimeClient, workspace_id: int) -> dict:
    """Test creating folders with Unicode characters that may normalize to same value.

    This is THE BUG: Creating folder "u" and then folder "ü" results in them
    being seen as duplicates after uploading.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: Unicode Folder Pairs (THE BUG)", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Test content")
    results = {"pairs": [], "duplicates_found": 0}

    # Pairs of folder names that may be confused due to normalization
    # Format: (folder1, folder2, description)
    confusing_pairs = [
        ("u", "ü", "ASCII 'u' vs Latin Small Letter U with Diaeresis"),
        ("a", "ä", "ASCII 'a' vs Latin Small Letter A with Diaeresis"),
        ("o", "ö", "ASCII 'o' vs Latin Small Letter O with Diaeresis"),
        ("u", "\u0075\u0308", "ASCII 'u' vs 'u' + combining diaeresis (NFD form)"),
        (
            "ü",
            "\u0075\u0308",
            "Precomposed 'ü' (NFC) vs decomposed 'u+diaeresis' (NFD)",
        ),
        ("n", "ñ", "ASCII 'n' vs Latin Small Letter N with Tilde"),
        ("e", "é", "ASCII 'e' vs Latin Small Letter E with Acute"),
        ("c", "ç", "ASCII 'c' vs Latin Small Letter C with Cedilla"),
        ("A", "Ä", "ASCII 'A' vs Latin Capital Letter A with Diaeresis"),
        ("ss", "ß", "ASCII 'ss' vs Latin Small Letter Sharp S"),
        ("fi", "\ufb01", "ASCII 'fi' vs Latin Small Ligature FI"),
    ]

    for folder1, folder2, desc in confusing_pairs:
        print(f"\n  Testing pair: {desc}")
        print_unicode_analysis("Folder 1", folder1, indent=2)
        print_unicode_analysis("Folder 2", folder2, indent=2)

        base_path = f"unicode_test_{unique_id}"
        path1 = f"{base_path}/{folder1}/file.txt"
        path2 = f"{base_path}/{folder2}/file.txt"

        pair_result = {
            "folder1": folder1,
            "folder2": folder2,
            "description": desc,
            "upload1": None,
            "upload2": None,
            "is_duplicate": False,
        }

        # Upload first folder
        print(f"    Uploading to '{folder1}'... ", end="", flush=True)
        file_id1 = None
        parent_id1 = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path1,
                workspace_id=workspace_id,
            )
            file_id1 = result.get("fileEntry", {}).get("id")
            parent_id1 = result.get("fileEntry", {}).get("parent_id")
            print(f"SUCCESS (id={file_id1}, parent={parent_id1})")
            pair_result["upload1"] = {
                "success": True,
                "file_id": file_id1,
                "parent_id": parent_id1,
            }
        except Exception as e:
            print(f"FAILED: {e}")
            pair_result["upload1"] = {"success": False, "error": str(e)}

        time.sleep(0.5)

        # Upload second folder
        print(f"    Uploading to '{folder2}'... ", end="", flush=True)
        file_id2 = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path2,
                workspace_id=workspace_id,
            )
            file_id2 = result.get("fileEntry", {}).get("id")
            parent_id2 = result.get("fileEntry", {}).get("parent_id")
            print(f"SUCCESS (id={file_id2}, parent={parent_id2})")
            pair_result["upload2"] = {
                "success": True,
                "file_id": file_id2,
                "parent_id": parent_id2,
            }

            # Check if they ended up in the same parent (duplicate detection)
            if pair_result["upload1"] and pair_result["upload1"].get("success"):
                if parent_id1 == parent_id2:
                    print_warning(
                        f"    DUPLICATE DETECTED: Both files have same "
                        f"parent_id={parent_id1}"
                    )
                    pair_result["is_duplicate"] = True
                    results["duplicates_found"] += 1
                else:
                    print_success(
                        f"    Different parents: {parent_id1} vs {parent_id2}"
                    )
        except Exception as e:
            error_str = str(e)
            print(f"FAILED: {e}")
            pair_result["upload2"] = {"success": False, "error": error_str}

            # Check if failure is due to duplicate
            if "duplicate" in error_str.lower() or "exists" in error_str.lower():
                print_warning(f"    DUPLICATE ERROR: {error_str}")
                pair_result["is_duplicate"] = True
                results["duplicates_found"] += 1

        results["pairs"].append(pair_result)

        # Cleanup
        for file_id in [file_id1, file_id2]:
            if file_id:
                try:
                    client.delete_file_entries([file_id], delete_forever=True)
                except Exception:
                    pass

        time.sleep(0.5)

    cleanup_file(temp_file)
    return results


def test_same_folder_nfc_nfd(client: DrimeClient, workspace_id: int) -> dict:
    """Test creating folders with NFC and NFD normalized versions of same character.

    This tests the specific case where the same visual character has different
    Unicode representations.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: NFC vs NFD Normalization", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Test content")
    results = {"tests": [], "issues_found": 0}

    # Characters that have different NFC and NFD forms
    # Format: (visual_char, nfc_form, nfd_form, description)
    test_chars = [
        ("ü", "\u00fc", "\u0075\u0308", "u with diaeresis"),
        ("ñ", "\u00f1", "\u006e\u0303", "n with tilde"),
        ("é", "\u00e9", "\u0065\u0301", "e with acute"),
        ("ö", "\u00f6", "\u006f\u0308", "o with diaeresis"),
        ("ä", "\u00e4", "\u0061\u0308", "a with diaeresis"),
        ("ç", "\u00e7", "\u0063\u0327", "c with cedilla"),
        ("å", "\u00e5", "\u0061\u030a", "a with ring above"),
        ("ø", "\u00f8", None, "o with stroke (no decomposition)"),
    ]

    for visual, nfc, nfd, desc in test_chars:
        if nfd is None:
            continue  # Skip characters without decomposed form

        print(f"\n  Testing: {desc}")
        print_unicode_analysis("NFC form", nfc, indent=2)
        print_unicode_analysis("NFD form", nfd, indent=2)

        # Verify they are different byte sequences but same visual
        print(f"    NFC == NFD visually: {nfc == nfd}")
        print(f"    NFC bytes: {nfc.encode('utf-8').hex()}")
        print(f"    NFD bytes: {nfd.encode('utf-8').hex()}")

        base_path = f"nfc_nfd_test_{unique_id}"
        path_nfc = f"{base_path}/folder_{nfc}/file.txt"
        path_nfd = f"{base_path}/folder_{nfd}/file.txt"

        test_result = {
            "character": visual,
            "description": desc,
            "nfc_upload": None,
            "nfd_upload": None,
            "same_parent": False,
        }

        # Upload NFC version
        print("    Uploading NFC folder... ", end="", flush=True)
        file_id_nfc = None
        parent_id_nfc = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path_nfc,
                workspace_id=workspace_id,
            )
            file_id_nfc = result.get("fileEntry", {}).get("id")
            parent_id_nfc = result.get("fileEntry", {}).get("parent_id")
            print(f"SUCCESS (parent={parent_id_nfc})")
            test_result["nfc_upload"] = {"success": True, "parent_id": parent_id_nfc}
        except Exception as e:
            print(f"FAILED: {e}")
            test_result["nfc_upload"] = {"success": False, "error": str(e)}

        time.sleep(0.5)

        # Upload NFD version
        print("    Uploading NFD folder... ", end="", flush=True)
        file_id_nfd = None
        parent_id_nfd = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path_nfd,
                workspace_id=workspace_id,
            )
            file_id_nfd = result.get("fileEntry", {}).get("id")
            parent_id_nfd = result.get("fileEntry", {}).get("parent_id")
            print(f"SUCCESS (parent={parent_id_nfd})")
            test_result["nfd_upload"] = {"success": True, "parent_id": parent_id_nfd}

            if parent_id_nfc == parent_id_nfd:
                print_warning("    ISSUE: NFC and NFD ended up in same parent folder!")
                test_result["same_parent"] = True
                results["issues_found"] += 1
            else:
                print_success("    Different parent folders (as expected)")
        except Exception as e:
            print(f"FAILED: {e}")
            test_result["nfd_upload"] = {"success": False, "error": str(e)}
            if "duplicate" in str(e).lower():
                print_warning("    ISSUE: Duplicate error - normalization collision!")
                test_result["same_parent"] = True
                results["issues_found"] += 1

        results["tests"].append(test_result)

        # Cleanup
        for file_id in [file_id_nfc, file_id_nfd]:
            if file_id:
                try:
                    client.delete_file_entries([file_id], delete_forever=True)
                except Exception:
                    pass

        time.sleep(0.5)

    cleanup_file(temp_file)
    return results


def test_ascii_vs_lookalike(client: DrimeClient, workspace_id: int) -> dict:
    """Test ASCII characters vs their Unicode lookalikes.

    This tests potential confusion between ASCII and visually similar Unicode chars.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: ASCII vs Unicode Lookalikes", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Test content")
    results = {"pairs": [], "confusions_found": 0}

    # ASCII vs Unicode lookalikes
    # Format: (ascii_char, unicode_char, unicode_name)
    lookalikes = [
        ("a", "\u0430", "CYRILLIC SMALL LETTER A"),
        ("e", "\u0435", "CYRILLIC SMALL LETTER IE"),
        ("o", "\u043e", "CYRILLIC SMALL LETTER O"),
        ("p", "\u0440", "CYRILLIC SMALL LETTER ER"),
        ("c", "\u0441", "CYRILLIC SMALL LETTER ES"),
        ("x", "\u0445", "CYRILLIC SMALL LETTER HA"),
        ("A", "\u0391", "GREEK CAPITAL LETTER ALPHA"),
        ("B", "\u0392", "GREEK CAPITAL LETTER BETA"),
        ("O", "\u039f", "GREEK CAPITAL LETTER OMICRON"),
        ("1", "\u0661", "ARABIC-INDIC DIGIT ONE"),
        ("2", "\u0662", "ARABIC-INDIC DIGIT TWO"),
    ]

    for ascii_char, unicode_char, unicode_name in lookalikes:
        print(f"\n  Testing: ASCII '{ascii_char}' vs {unicode_name}")
        print(f"    ASCII: U+{ord(ascii_char):04X} ({unicodedata.name(ascii_char)})")
        print(f"    Unicode: U+{ord(unicode_char):04X} ({unicode_name})")

        base_path = f"lookalike_test_{unique_id}"
        path_ascii = f"{base_path}/folder_{ascii_char}/file.txt"
        path_unicode = f"{base_path}/folder_{unicode_char}/file.txt"

        pair_result = {
            "ascii": ascii_char,
            "unicode": unicode_char,
            "unicode_name": unicode_name,
            "ascii_upload": None,
            "unicode_upload": None,
            "confused": False,
        }

        # Upload ASCII version
        file_id_ascii = None
        parent_id_ascii = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path_ascii,
                workspace_id=workspace_id,
            )
            file_id_ascii = result.get("fileEntry", {}).get("id")
            parent_id_ascii = result.get("fileEntry", {}).get("parent_id")
            pair_result["ascii_upload"] = {
                "success": True,
                "parent_id": parent_id_ascii,
            }
        except Exception as e:
            pair_result["ascii_upload"] = {"success": False, "error": str(e)}

        time.sleep(0.3)

        # Upload Unicode version
        file_id_unicode = None
        parent_id_unicode = None
        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path_unicode,
                workspace_id=workspace_id,
            )
            file_id_unicode = result.get("fileEntry", {}).get("id")
            parent_id_unicode = result.get("fileEntry", {}).get("parent_id")
            pair_result["unicode_upload"] = {
                "success": True,
                "parent_id": parent_id_unicode,
            }

            if parent_id_ascii == parent_id_unicode:
                print_warning("    CONFUSION: Same parent folder!")
                pair_result["confused"] = True
                results["confusions_found"] += 1
            else:
                print_success("    Correctly differentiated")
        except Exception as e:
            pair_result["unicode_upload"] = {"success": False, "error": str(e)}
            if "duplicate" in str(e).lower():
                pair_result["confused"] = True
                results["confusions_found"] += 1

        results["pairs"].append(pair_result)

        # Cleanup
        for file_id in [file_id_ascii, file_id_unicode]:
            if file_id:
                try:
                    client.delete_file_entries([file_id], delete_forever=True)
                except Exception:
                    pass

        time.sleep(0.3)

    cleanup_file(temp_file)
    return results


def test_list_after_upload(client: DrimeClient, workspace_id: int) -> dict:
    """Test listing folder contents after uploading Unicode folders.

    This verifies how the server stores and returns folder names.

    Returns:
        Dictionary with test results
    """
    print_separator("Test: List After Upload (Verify Storage)", "-")

    unique_id = uuid.uuid4().hex[:8]
    temp_file = create_test_file(content=b"Test content")
    results = {"uploads": [], "listing_issues": 0}

    # Create a base folder and upload files to folders with Unicode names
    base_path = f"list_test_{unique_id}"
    folders_to_create = [
        "folder_u",
        "folder_ü",
        "folder_\u0075\u0308",  # u + combining diaeresis (NFD)
    ]

    file_ids = []
    parent_ids = {}

    print_info("Uploading files to Unicode folders...")
    for folder in folders_to_create:
        path = f"{base_path}/{folder}/file.txt"
        print(f"  Uploading to '{folder}'... ", end="", flush=True)

        try:
            result = client.upload_file(
                file_path=temp_file,
                relative_path=path,
                workspace_id=workspace_id,
            )
            file_id = result.get("fileEntry", {}).get("id")
            parent_id = result.get("fileEntry", {}).get("parent_id")
            file_ids.append(file_id)
            parent_ids[folder] = parent_id
            print(f"SUCCESS (parent={parent_id})")
            results["uploads"].append(
                {
                    "folder": folder,
                    "folder_repr": repr(folder),
                    "success": True,
                    "parent_id": parent_id,
                }
            )
        except Exception as e:
            print(f"FAILED: {e}")
            results["uploads"].append(
                {
                    "folder": folder,
                    "folder_repr": repr(folder),
                    "success": False,
                    "error": str(e),
                }
            )

        time.sleep(0.3)

    # Check for parent_id collisions
    print_info("\nAnalyzing parent IDs...")
    seen_parents = {}
    for folder, parent_id in parent_ids.items():
        if parent_id in seen_parents:
            print_warning(
                f"  COLLISION: '{folder}' and '{seen_parents[parent_id]}' "
                f"have same parent_id={parent_id}"
            )
            results["listing_issues"] += 1
        else:
            seen_parents[parent_id] = folder

    # Try to list the base folder using parent IDs we collected
    print_info("\nAnalyzing uploaded folder names from server...")
    unique_parent_ids = list(set(parent_ids.values()))
    for parent_id in unique_parent_ids:
        try:
            listing = client.list_files(parent_id=parent_id)
            entries = listing.get("fileEntries", [])
            print(f"  Parent ID {parent_id} contains {len(entries)} entries:")
            for entry in entries:
                name = entry.get("name", "")
                entry_type = entry.get("type", "")
                print(f"    - '{name}' ({entry_type})")
                print(f"      repr: {repr(name)}")
                print(f"      NFC: {repr(unicodedata.normalize('NFC', name))}")
                print(f"      NFD: {repr(unicodedata.normalize('NFD', name))}")
        except Exception as e:
            print_warning(f"  Could not list parent {parent_id}: {e}")

    # Cleanup
    for file_id in file_ids:
        if file_id:
            try:
                client.delete_file_entries([file_id], delete_forever=True)
            except Exception:
                pass

    cleanup_file(temp_file)
    return results


def summarize_results(all_results: dict) -> None:
    """Print a summary of all test results."""
    print_separator("TEST SUMMARY", "=")

    total_issues = 0

    for test_name, results in all_results.items():
        print(f"\n{test_name}:")
        if isinstance(results, dict):
            if "duplicates_found" in results:
                count = results["duplicates_found"]
                total_issues += count
                status = "ISSUES FOUND" if count > 0 else "OK"
                print(f"  Duplicates/collisions: {count} ({status})")

            if "issues_found" in results:
                count = results["issues_found"]
                total_issues += count
                status = "ISSUES FOUND" if count > 0 else "OK"
                print(f"  Normalization issues: {count} ({status})")

            if "confusions_found" in results:
                count = results["confusions_found"]
                total_issues += count
                status = "ISSUES FOUND" if count > 0 else "OK"
                print(f"  Lookalike confusions: {count} ({status})")

            if "listing_issues" in results:
                count = results["listing_issues"]
                total_issues += count
                status = "ISSUES FOUND" if count > 0 else "OK"
                print(f"  Listing issues: {count} ({status})")

    print(f"\n{'=' * 80}")
    if total_issues > 0:
        print(f"TOTAL ISSUES FOUND: {total_issues}")
        print("\nThe Unicode normalization bug is CONFIRMED!")
        print(
            "Folders with similar Unicode characters are being treated as duplicates."
        )
    else:
        print("No Unicode normalization issues detected.")
    print(f"{'=' * 80}")


def main():
    """Main benchmark function."""
    print_separator("PYDRIME UNICODE NORMALIZATION BENCHMARK", "=")

    print_info("This benchmark tests the Unicode normalization bug where:")
    print_info("  - Creating folder 'u' and then folder 'ü' results in duplicates")
    print_info("  - NFC and NFD normalized forms may collide")
    print_info("  - ASCII and Unicode lookalikes may be confused")

    # Initialize client
    print_info("\nInitializing API client...")
    try:
        client = DrimeClient()
        user_info = client.get_logged_user()
        if user_info and user_info.get("user"):
            user = user_info["user"]
            print_success(f"Connected as: {user.get('email', 'unknown')}")
        else:
            print_error("Could not verify API connection")
            sys.exit(1)
    except Exception as e:
        print_error(f"Failed to initialize client: {e}")
        sys.exit(1)

    # Use workspace from config or default to 0
    from pydrime.config import Config

    config = Config()
    workspace_id = config.get_default_workspace() or 0
    print_info(f"Using workspace ID: {workspace_id}")

    all_results = {}

    try:
        # Run tests - most important first (the exact bug scenario)
        all_results["Unicode Folder Pairs"] = test_unicode_folder_pairs(
            client, workspace_id
        )

        all_results["NFC vs NFD"] = test_same_folder_nfc_nfd(client, workspace_id)

        all_results["ASCII vs Lookalikes"] = test_ascii_vs_lookalike(
            client, workspace_id
        )

        all_results["List After Upload"] = test_list_after_upload(client, workspace_id)

        # Print summary
        summarize_results(all_results)

    except KeyboardInterrupt:
        print_warning("\nBenchmark interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()

    print_separator("BENCHMARK COMPLETE", "=")


if __name__ == "__main__":
    main()
