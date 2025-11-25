"""Tests for vault encryption and decryption functionality."""

import base64

import pytest

from pydrime.vault_crypto import (
    IV_LENGTH,
    VaultCryptoError,
    VaultKey,
    VaultPasswordError,
    decrypt_data,
    decrypt_file_content,
    decrypt_filename,
    derive_key,
    encrypt_data,
    encrypt_file_content,
    encrypt_filename,
    setup_vault,
    unlock_vault,
)


class TestDeriveKey:
    """Tests for key derivation."""

    def test_derive_key_produces_32_bytes(self):
        """Test that derive_key produces a 256-bit (32 bytes) key."""
        salt = b"1234567890123456"  # 16 bytes
        key = derive_key("password", salt)
        assert len(key) == 32

    def test_derive_key_is_deterministic(self):
        """Test that the same password and salt produce the same key."""
        salt = b"1234567890123456"
        key1 = derive_key("password", salt)
        key2 = derive_key("password", salt)
        assert key1 == key2

    def test_derive_key_different_passwords(self):
        """Test that different passwords produce different keys."""
        salt = b"1234567890123456"
        key1 = derive_key("password1", salt)
        key2 = derive_key("password2", salt)
        assert key1 != key2

    def test_derive_key_different_salts(self):
        """Test that different salts produce different keys."""
        key1 = derive_key("password", b"salt1234567890ab")
        key2 = derive_key("password", b"salt1234567890cd")
        assert key1 != key2

    def test_derive_key_empty_password(self):
        """Test key derivation with empty password."""
        salt = b"1234567890123456"
        key = derive_key("", salt)
        assert len(key) == 32

    def test_derive_key_unicode_password(self):
        """Test key derivation with unicode password."""
        salt = b"1234567890123456"
        key = derive_key("пароль日本語", salt)
        assert len(key) == 32


class TestVaultKey:
    """Tests for VaultKey class."""

    def test_encrypt_decrypt_roundtrip(self):
        """Test that encrypt/decrypt produces the original data."""
        salt = b"1234567890123456"
        key_bytes = derive_key("password", salt)
        vault_key = VaultKey(key=key_bytes, salt=salt)

        plaintext = b"Hello, World!"
        ciphertext, iv = vault_key.encrypt(plaintext)
        decrypted = vault_key.decrypt(ciphertext, iv)

        assert decrypted == plaintext

    def test_encrypt_produces_different_output(self):
        """Test that encrypting twice gives different ciphertext (random IV)."""
        salt = b"1234567890123456"
        key_bytes = derive_key("password", salt)
        vault_key = VaultKey(key=key_bytes, salt=salt)

        plaintext = b"Hello, World!"
        ciphertext1, iv1 = vault_key.encrypt(plaintext)
        ciphertext2, iv2 = vault_key.encrypt(plaintext)

        # IVs should be different
        assert iv1 != iv2
        # Ciphertext should be different (different IVs)
        assert ciphertext1 != ciphertext2

    def test_decrypt_with_wrong_key_fails(self):
        """Test that decryption with wrong key fails."""
        salt = b"1234567890123456"
        key1 = derive_key("password1", salt)
        key2 = derive_key("password2", salt)

        vault_key1 = VaultKey(key=key1, salt=salt)
        vault_key2 = VaultKey(key=key2, salt=salt)

        plaintext = b"Hello, World!"
        ciphertext, iv = vault_key1.encrypt(plaintext)

        with pytest.raises(VaultCryptoError):
            vault_key2.decrypt(ciphertext, iv)

    def test_decrypt_with_wrong_iv_fails(self):
        """Test that decryption with wrong IV fails."""
        salt = b"1234567890123456"
        key_bytes = derive_key("password", salt)
        vault_key = VaultKey(key=key_bytes, salt=salt)

        plaintext = b"Hello, World!"
        ciphertext, _ = vault_key.encrypt(plaintext)
        wrong_iv = b"123456789012"  # 12 bytes

        with pytest.raises(VaultCryptoError):
            vault_key.decrypt(ciphertext, wrong_iv)

    def test_encrypt_empty_data(self):
        """Test encryption of empty data."""
        salt = b"1234567890123456"
        key_bytes = derive_key("password", salt)
        vault_key = VaultKey(key=key_bytes, salt=salt)

        plaintext = b""
        ciphertext, iv = vault_key.encrypt(plaintext)
        decrypted = vault_key.decrypt(ciphertext, iv)

        assert decrypted == plaintext

    def test_iv_length(self):
        """Test that IV has correct length."""
        salt = b"1234567890123456"
        key_bytes = derive_key("password", salt)
        vault_key = VaultKey(key=key_bytes, salt=salt)

        _, iv = vault_key.encrypt(b"test")
        assert len(iv) == IV_LENGTH  # 12 bytes


class TestSetupVault:
    """Tests for vault setup."""

    def test_setup_vault_returns_valid_result(self):
        """Test that setup_vault returns all required components."""
        result = setup_vault("password")

        assert result.key is not None
        assert result.salt_b64 is not None
        assert result.vault_check_b64 is not None
        assert result.check_iv_b64 is not None

    def test_setup_vault_different_passwords_different_keys(self):
        """Test that different passwords produce different setup results."""
        result1 = setup_vault("password1")
        result2 = setup_vault("password2")

        # Keys should be different
        assert result1.key.key != result2.key.key

    def test_setup_vault_same_password_different_salts(self):
        """Test that same password produces different results (random salt)."""
        result1 = setup_vault("password")
        result2 = setup_vault("password")

        # Salts should be different (random)
        assert result1.salt_b64 != result2.salt_b64

    def test_setup_vault_base64_encoding(self):
        """Test that setup_vault returns valid base64 encoded values."""
        result = setup_vault("password")

        # Should not raise on decode
        salt = base64.b64decode(result.salt_b64)
        check = base64.b64decode(result.vault_check_b64)
        iv = base64.b64decode(result.check_iv_b64)

        assert len(salt) == 16  # Salt is 16 bytes
        assert len(iv) == 12  # IV is 12 bytes
        # Check is encrypted "vault-unlock" (12 bytes) + GCM tag (16 bytes)
        assert len(check) == 28

    def test_setup_vault_can_be_unlocked(self):
        """Test that a setup vault can be unlocked with the same password."""
        password = "my_secure_password"
        result = setup_vault(password)

        # Should not raise
        vault_key = unlock_vault(
            password=password,
            salt_b64=result.salt_b64,
            vault_check_b64=result.vault_check_b64,
            check_iv_b64=result.check_iv_b64,
        )

        assert vault_key.key == result.key.key


class TestUnlockVault:
    """Tests for vault unlock."""

    def test_unlock_vault_with_correct_password(self):
        """Test that correct password unlocks the vault."""
        # First, set up a vault
        setup_result = setup_vault("mypassword")

        # Now unlock with the same password
        vault_key = unlock_vault(
            password="mypassword",
            salt_b64=setup_result.salt_b64,
            vault_check_b64=setup_result.vault_check_b64,
            check_iv_b64=setup_result.check_iv_b64,
        )

        # Should return a valid key
        assert vault_key is not None
        assert vault_key.key == setup_result.key.key

    def test_unlock_vault_with_wrong_password(self):
        """Test that wrong password fails to unlock the vault."""
        setup_result = setup_vault("correctpassword")

        with pytest.raises(VaultPasswordError):
            unlock_vault(
                password="wrongpassword",
                salt_b64=setup_result.salt_b64,
                vault_check_b64=setup_result.vault_check_b64,
                check_iv_b64=setup_result.check_iv_b64,
            )

    def test_unlock_vault_with_empty_password(self):
        """Test vault with empty password."""
        setup_result = setup_vault("")

        vault_key = unlock_vault(
            password="",
            salt_b64=setup_result.salt_b64,
            vault_check_b64=setup_result.vault_check_b64,
            check_iv_b64=setup_result.check_iv_b64,
        )

        assert vault_key is not None

    def test_unlock_vault_with_unicode_password(self):
        """Test vault with unicode password."""
        password = "密码пароль🔐"
        setup_result = setup_vault(password)

        vault_key = unlock_vault(
            password=password,
            salt_b64=setup_result.salt_b64,
            vault_check_b64=setup_result.vault_check_b64,
            check_iv_b64=setup_result.check_iv_b64,
        )

        assert vault_key is not None

    def test_unlock_vault_with_corrupted_check(self):
        """Test that corrupted check value fails."""
        setup_result = setup_vault("password")

        with pytest.raises(VaultPasswordError):
            unlock_vault(
                password="password",
                salt_b64=setup_result.salt_b64,
                vault_check_b64="corrupted_base64_data==",
                check_iv_b64=setup_result.check_iv_b64,
            )


class TestEncryptDecryptData:
    """Tests for data encryption/decryption helpers."""

    def test_encrypt_decrypt_data_roundtrip(self):
        """Test encrypt_data and decrypt_data roundtrip."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        plaintext = b"Secret data"
        ciphertext_b64, iv_b64 = encrypt_data(vault_key, plaintext)
        decrypted = decrypt_data(vault_key, ciphertext_b64, iv_b64)

        assert decrypted == plaintext

    def test_encrypt_decrypt_filename_roundtrip(self):
        """Test encrypt_filename and decrypt_filename roundtrip."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        filename = "my_secret_document.pdf"
        encrypted_b64, iv_b64 = encrypt_filename(vault_key, filename)
        decrypted = decrypt_filename(vault_key, encrypted_b64, iv_b64)

        assert decrypted == filename

    def test_encrypt_decrypt_unicode_filename(self):
        """Test encryption/decryption of unicode filenames."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        filename = "documento_secreto_éàü.pdf"
        encrypted_b64, iv_b64 = encrypt_filename(vault_key, filename)
        decrypted = decrypt_filename(vault_key, encrypted_b64, iv_b64)

        assert decrypted == filename

    def test_encrypt_data_returns_base64(self):
        """Test that encrypt_data returns valid base64 strings."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        ciphertext_b64, iv_b64 = encrypt_data(vault_key, b"test")

        # Should not raise on decode
        base64.b64decode(ciphertext_b64)
        base64.b64decode(iv_b64)


class TestEncryptDecryptFileContent:
    """Tests for file content encryption/decryption."""

    def test_encrypt_decrypt_file_content_roundtrip(self):
        """Test encrypt_file_content and decrypt_file_content roundtrip."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        content = b"This is the file content with some binary data: \x00\x01\x02"
        encrypted = encrypt_file_content(vault_key, content)
        decrypted = decrypt_file_content(vault_key, encrypted)

        assert decrypted == content

    def test_encrypt_file_content_format(self):
        """Test that encrypted content has IV prefix."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        content = b"Test content"
        encrypted = encrypt_file_content(vault_key, content)

        # First 12 bytes should be the IV
        assert len(encrypted) > 12
        # Total size should be IV (12) + content + GCM tag (16)
        assert len(encrypted) == 12 + len(content) + 16

    def test_decrypt_file_content_too_short(self):
        """Test that decrypting too-short content fails when no IV provided."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        with pytest.raises(VaultCryptoError):
            decrypt_file_content(vault_key, b"short")

    def test_encrypt_large_file_content(self):
        """Test encryption of larger file content."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        # 1 MB of data
        content = b"x" * (1024 * 1024)
        encrypted = encrypt_file_content(vault_key, content)
        decrypted = decrypt_file_content(vault_key, encrypted)

        assert decrypted == content

    def test_decrypt_file_content_with_separate_iv(self):
        """Test decrypting file content with IV provided separately (API format)."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        # Simulate API format: ciphertext only, IV in metadata
        content = b"File content from vault API"
        ciphertext, iv = vault_key.encrypt(content)
        iv_b64 = base64.b64encode(iv).decode("ascii")

        # Decrypt using the separate IV
        decrypted = decrypt_file_content(vault_key, ciphertext, iv_b64=iv_b64)

        assert decrypted == content

    def test_decrypt_file_content_with_iv_from_entry(self):
        """Test decrypting with IV format matching vault file entries."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        content = b"Secret vault file content"
        ciphertext, iv = vault_key.encrypt(content)

        # IV as stored in vault file entry (base64, possibly without padding)
        iv_b64 = base64.b64encode(iv).decode("ascii").rstrip("=")
        # Add padding back for proper decoding
        iv_b64_padded = (
            iv_b64 + "=" * (4 - len(iv_b64) % 4) if len(iv_b64) % 4 else iv_b64
        )

        decrypted = decrypt_file_content(vault_key, ciphertext, iv_b64=iv_b64_padded)

        assert decrypted == content

    def test_decrypt_file_content_empty_with_iv(self):
        """Test decrypting empty content with separate IV."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        content = b""
        ciphertext, iv = vault_key.encrypt(content)
        iv_b64 = base64.b64encode(iv).decode("ascii")

        decrypted = decrypt_file_content(vault_key, ciphertext, iv_b64=iv_b64)

        assert decrypted == content

    def test_decrypt_file_content_binary_data_with_iv(self):
        """Test decrypting binary content with separate IV."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        # Binary content with all byte values
        content = bytes(range(256)) * 100
        ciphertext, iv = vault_key.encrypt(content)
        iv_b64 = base64.b64encode(iv).decode("ascii")

        decrypted = decrypt_file_content(vault_key, ciphertext, iv_b64=iv_b64)

        assert decrypted == content


class TestVaultKeyFileOperations:
    """Tests for VaultKey file operations."""

    def test_encrypt_decrypt_file(self, tmp_path):
        """Test file encryption and decryption."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        # Create a test file
        input_file = tmp_path / "test.txt"
        input_file.write_text("Secret content")

        # Encrypt
        encrypted_file = vault_key.encrypt_file(input_file)
        assert encrypted_file.exists()
        assert encrypted_file.suffix == ".enc"

        # Decrypt
        decrypted_file = vault_key.decrypt_file(encrypted_file)
        assert decrypted_file.exists()
        assert decrypted_file.read_text() == "Secret content"

    def test_encrypt_file_custom_output(self, tmp_path):
        """Test file encryption with custom output path."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        input_file = tmp_path / "input.txt"
        input_file.write_text("Content")
        output_file = tmp_path / "custom_output.encrypted"

        encrypted_file = vault_key.encrypt_file(input_file, output_file)

        assert encrypted_file == output_file
        assert encrypted_file.exists()

    def test_decrypt_file_custom_output(self, tmp_path):
        """Test file decryption with custom output path."""
        setup_result = setup_vault("password")
        vault_key = setup_result.key

        # Create and encrypt
        input_file = tmp_path / "test.txt"
        input_file.write_text("Secret")
        encrypted_file = vault_key.encrypt_file(input_file)

        # Decrypt to custom location
        output_file = tmp_path / "decrypted_output.txt"
        decrypted_file = vault_key.decrypt_file(encrypted_file, output_file)

        assert decrypted_file == output_file
        assert decrypted_file.read_text() == "Secret"
