"""Vault encryption and decryption utilities.

This module provides client-side encryption for the Drime vault using:
- PBKDF2 for key derivation (250,000 iterations, SHA-256)
- AES-256-GCM for encryption/decryption

The encryption scheme is compatible with the web client's JavaScript implementation.
"""

import base64
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .exceptions import DrimeAPIError

# Constants matching the JavaScript implementation
PBKDF2_ITERATIONS = 250_000
PBKDF2_HASH = hashes.SHA256()
AES_KEY_LENGTH = 256  # bits
SALT_LENGTH = 16  # bytes
IV_LENGTH = 12  # bytes (96 bits for GCM)
VAULT_CHECK_PLAINTEXT = b"vault-unlock"


class VaultCryptoError(DrimeAPIError):
    """Error during vault encryption/decryption operations."""

    pass


class VaultPasswordError(VaultCryptoError):
    """Invalid vault password."""

    pass


@dataclass
class VaultKey:
    """Represents a derived vault encryption key."""

    key: bytes  # The raw 256-bit AES key
    salt: bytes  # The salt used for key derivation

    def encrypt(self, plaintext: bytes) -> tuple[bytes, bytes]:
        """Encrypt data using AES-256-GCM.

        Args:
            plaintext: Data to encrypt

        Returns:
            Tuple of (ciphertext, iv)
        """
        iv = secrets.token_bytes(IV_LENGTH)
        aesgcm = AESGCM(self.key)
        ciphertext = aesgcm.encrypt(iv, plaintext, None)
        return ciphertext, iv

    def decrypt(self, ciphertext: bytes, iv: bytes) -> bytes:
        """Decrypt data using AES-256-GCM.

        Args:
            ciphertext: Encrypted data (includes GCM tag)
            iv: Initialization vector used during encryption

        Returns:
            Decrypted plaintext

        Raises:
            VaultCryptoError: If decryption fails
        """
        try:
            aesgcm = AESGCM(self.key)
            return aesgcm.decrypt(iv, ciphertext, None)
        except Exception as e:
            raise VaultCryptoError(f"Decryption failed: {e}") from e

    def encrypt_file(
        self, input_path: Path, output_path: Optional[Path] = None
    ) -> Path:
        """Encrypt a file.

        Args:
            input_path: Path to the file to encrypt
            output_path: Path to save encrypted file (default: input_path + .enc)

        Returns:
            Path to the encrypted file
        """
        if output_path is None:
            output_path = input_path.with_suffix(input_path.suffix + ".enc")

        plaintext = input_path.read_bytes()
        ciphertext, iv = self.encrypt(plaintext)

        # Store IV + ciphertext
        output_path.write_bytes(iv + ciphertext)
        return output_path

    def decrypt_file(
        self, input_path: Path, output_path: Optional[Path] = None
    ) -> Path:
        """Decrypt a file.

        Args:
            input_path: Path to the encrypted file
            output_path: Path to save decrypted file (default: removes .enc suffix)

        Returns:
            Path to the decrypted file
        """
        if output_path is None:
            if input_path.suffix == ".enc":
                output_path = input_path.with_suffix("")
            else:
                output_path = input_path.with_suffix(input_path.suffix + ".dec")

        encrypted_data = input_path.read_bytes()

        # Extract IV (first 12 bytes) and ciphertext
        iv = encrypted_data[:IV_LENGTH]
        ciphertext = encrypted_data[IV_LENGTH:]

        plaintext = self.decrypt(ciphertext, iv)
        output_path.write_bytes(plaintext)
        return output_path


@dataclass
class VaultSetupResult:
    """Result of setting up a new vault."""

    key: VaultKey
    salt_b64: str  # Base64 encoded salt for storage
    vault_check_b64: str  # Base64 encoded encrypted check value
    check_iv_b64: str  # Base64 encoded IV for check value


def _bytes_to_base64(data: bytes) -> str:
    """Convert bytes to base64 string."""
    return base64.b64encode(data).decode("ascii")


def _base64_to_bytes(data: str) -> bytes:
    """Convert base64 string to bytes."""
    return base64.b64decode(data)


def derive_key(password: str, salt: bytes) -> bytes:
    """Derive an AES-256 key from a password using PBKDF2.

    Args:
        password: The user's vault password
        salt: Random salt (16 bytes)

    Returns:
        256-bit (32 bytes) AES key
    """
    kdf = PBKDF2HMAC(
        algorithm=PBKDF2_HASH,
        length=AES_KEY_LENGTH // 8,  # Convert bits to bytes
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
        backend=default_backend(),
    )
    return kdf.derive(password.encode("utf-8"))


def setup_vault(password: str) -> VaultSetupResult:
    """Set up a new vault with a password.

    Creates encryption parameters for a new vault:
    - Generates a random salt
    - Derives an AES-256 key using PBKDF2
    - Creates an encrypted check value to verify the password later

    This is equivalent to the JavaScript `Ca` function.

    Args:
        password: The vault password

    Returns:
        VaultSetupResult containing the key and parameters to store
    """
    # Generate random salt (16 bytes)
    salt = secrets.token_bytes(SALT_LENGTH)

    # Derive the AES key
    key_bytes = derive_key(password, salt)
    vault_key = VaultKey(key=key_bytes, salt=salt)

    # Create the vault check value (encrypted "vault-unlock")
    check_ciphertext, check_iv = vault_key.encrypt(VAULT_CHECK_PLAINTEXT)

    return VaultSetupResult(
        key=vault_key,
        salt_b64=_bytes_to_base64(salt),
        vault_check_b64=_bytes_to_base64(check_ciphertext),
        check_iv_b64=_bytes_to_base64(check_iv),
    )


def unlock_vault(
    password: str, salt_b64: str, vault_check_b64: str, check_iv_b64: str
) -> VaultKey:
    """Unlock a vault by verifying the password.

    Derives the encryption key from the password and verifies it
    by decrypting the check value.

    Args:
        password: The vault password
        salt_b64: Base64 encoded salt from vault setup
        vault_check_b64: Base64 encoded encrypted check value
        check_iv_b64: Base64 encoded IV for check value

    Returns:
        VaultKey that can be used for encryption/decryption

    Raises:
        VaultPasswordError: If the password is incorrect
    """
    # Decode the stored parameters
    salt = _base64_to_bytes(salt_b64)
    vault_check = _base64_to_bytes(vault_check_b64)
    check_iv = _base64_to_bytes(check_iv_b64)

    # Derive the key
    key_bytes = derive_key(password, salt)
    vault_key = VaultKey(key=key_bytes, salt=salt)

    # Verify by decrypting the check value
    try:
        decrypted = vault_key.decrypt(vault_check, check_iv)
        if decrypted != VAULT_CHECK_PLAINTEXT:
            raise VaultPasswordError("Invalid vault password")
    except VaultCryptoError as e:
        raise VaultPasswordError("Invalid vault password") from e

    return vault_key


def encrypt_data(vault_key: VaultKey, plaintext: bytes) -> tuple[str, str]:
    """Encrypt data for vault storage.

    Args:
        vault_key: The vault encryption key
        plaintext: Data to encrypt

    Returns:
        Tuple of (ciphertext_b64, iv_b64)
    """
    ciphertext, iv = vault_key.encrypt(plaintext)
    return _bytes_to_base64(ciphertext), _bytes_to_base64(iv)


def decrypt_data(vault_key: VaultKey, ciphertext_b64: str, iv_b64: str) -> bytes:
    """Decrypt data from vault storage.

    Args:
        vault_key: The vault encryption key
        ciphertext_b64: Base64 encoded ciphertext
        iv_b64: Base64 encoded IV

    Returns:
        Decrypted plaintext
    """
    ciphertext = _base64_to_bytes(ciphertext_b64)
    iv = _base64_to_bytes(iv_b64)
    return vault_key.decrypt(ciphertext, iv)


def encrypt_filename(vault_key: VaultKey, filename: str) -> tuple[str, str]:
    """Encrypt a filename for vault storage.

    Args:
        vault_key: The vault encryption key
        filename: Filename to encrypt

    Returns:
        Tuple of (encrypted_filename_b64, iv_b64)
    """
    return encrypt_data(vault_key, filename.encode("utf-8"))


def decrypt_filename(
    vault_key: VaultKey, encrypted_filename_b64: str, iv_b64: str
) -> str:
    """Decrypt a filename from vault storage.

    Args:
        vault_key: The vault encryption key
        encrypted_filename_b64: Base64 encoded encrypted filename
        iv_b64: Base64 encoded IV

    Returns:
        Decrypted filename
    """
    plaintext = decrypt_data(vault_key, encrypted_filename_b64, iv_b64)
    return plaintext.decode("utf-8")


def decrypt_file_content(
    vault_key: VaultKey,
    encrypted_content: bytes,
    iv_b64: Optional[str] = None,
) -> bytes:
    """Decrypt file content from vault.

    Args:
        vault_key: The vault encryption key
        encrypted_content: Raw encrypted bytes (ciphertext only if iv_b64 provided,
                          otherwise IV + ciphertext)
        iv_b64: Optional base64-encoded IV from file entry. If provided, the
               encrypted_content is treated as ciphertext only. If not provided,
               the IV is expected to be prepended to the content.

    Returns:
        Decrypted file content
    """
    if iv_b64:
        # IV is provided separately (from file entry metadata)
        iv = _base64_to_bytes(iv_b64)
        ciphertext = encrypted_content
    else:
        # IV is prepended to content (legacy format)
        if len(encrypted_content) < IV_LENGTH:
            raise VaultCryptoError("Encrypted content too short")
        iv = encrypted_content[:IV_LENGTH]
        ciphertext = encrypted_content[IV_LENGTH:]

    return vault_key.decrypt(ciphertext, iv)


def encrypt_file_content(vault_key: VaultKey, content: bytes) -> bytes:
    """Encrypt file content for vault upload.

    Returns encrypted content in format: IV (12 bytes) + ciphertext

    Args:
        vault_key: The vault encryption key
        content: File content to encrypt

    Returns:
        Encrypted bytes (IV + ciphertext)
    """
    ciphertext, iv = vault_key.encrypt(content)
    return iv + ciphertext
