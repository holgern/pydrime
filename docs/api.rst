Python API Reference
====================

This page documents the PyDrime Python API for programmatic access to Drime Cloud.

DrimeClient
-----------

.. autoclass:: pydrime.api.DrimeClient
   :members:
   :undoc-members:
   :show-inheritance:

Quick Start
-----------

.. code-block:: python

   from pydrime import DrimeClient
   from pathlib import Path

   # Initialize the client
   client = DrimeClient(api_key="your_api_key_here")

   # Or use environment variable DRIME_API_KEY
   client = DrimeClient()

Upload Operations
-----------------

Upload a File
~~~~~~~~~~~~~

.. code-block:: python

   from pathlib import Path

   # Simple upload
   result = client.upload_file(Path("myfile.txt"))
   print(f"File uploaded: {result['fileEntry']['id']}")

   # Upload with custom path
   result = client.upload_file(
       Path("myfile.txt"),
       relative_path="folder/subfolder/myfile.txt"
   )

   # Upload to workspace
   result = client.upload_file(
       Path("myfile.txt"),
       workspace_id=123
   )

Upload with Progress Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def progress_callback(bytes_uploaded, total_bytes):
       percent = (bytes_uploaded / total_bytes) * 100
       print(f"Progress: {percent:.1f}%")

   result = client.upload_file(
       Path("largefile.zip"),
       progress_callback=progress_callback
   )

Multipart Upload
~~~~~~~~~~~~~~~~

For files larger than 30MB, multipart upload is used automatically:

.. code-block:: python

   # Automatically uses multipart for large files
   result = client.upload_file(Path("largefile.zip"))

   # Force multipart upload threshold
   result = client.upload_file(
       Path("myfile.txt"),
       use_multipart_threshold=10 * 1024 * 1024  # 10MB
   )

File Entry Operations
---------------------

List Files
~~~~~~~~~~

.. code-block:: python

   # List all files
   result = client.get_file_entries()
   for entry in result['data']:
       print(f"{entry['name']} - {entry['type']}")

   # List files in specific folder
   result = client.get_file_entries(parent_ids=[12345])

   # Search files
   result = client.get_file_entries(query="report")

   # Filter by type
   result = client.get_file_entries(entry_type="image")

   # Convenience method
   result = client.list_files(parent_id=12345, query="test")

Update File Entry
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Rename file
   result = client.update_file_entry(
       entry_id=12345,
       name="New Name.txt"
   )

   # Update description
   result = client.update_file_entry(
       entry_id=12345,
       description="Updated description"
   )

Delete Files
~~~~~~~~~~~~

.. code-block:: python

   # Move to trash
   result = client.delete_file_entries([12345, 67890])

   # Delete permanently
   result = client.delete_file_entries(
       [12345],
       delete_forever=True
   )

Move and Duplicate
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Move files to folder
   result = client.move_file_entries(
       entry_ids=[12345, 67890],
       destination_id=99999
   )

   # Move to root
   result = client.move_file_entries(
       entry_ids=[12345],
       destination_id=None
   )

   # Duplicate files
   result = client.duplicate_file_entries(
       entry_ids=[12345],
       destination_id=99999
   )

Restore from Trash
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   result = client.restore_file_entries([12345, 67890])

Folder Operations
-----------------

Create Folder
~~~~~~~~~~~~~

.. code-block:: python

   # Create in root
   result = client.create_folder("My Folder")
   folder_id = result['folder']['id']

   # Create in parent folder
   result = client.create_folder(
       name="Subfolder",
       parent_id=12345
   )

   # Convenience method
   result = client.create_directory("My Directory")

Download Operations
-------------------

Download File
~~~~~~~~~~~~~

.. code-block:: python

   from pathlib import Path

   # Download by hash
   saved_path = client.download_file("abc123hash")
   print(f"Downloaded to: {saved_path}")

   # Download to specific path
   saved_path = client.download_file(
       "abc123hash",
       output_path=Path("/path/to/save/file.txt")
   )

Sharing Operations
------------------

Share with Users
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Share file with users
   result = client.share_entry(
       entry_id=12345,
       emails=["user1@example.com", "user2@example.com"],
       permissions=["view", "download"]
   )

   # Change permissions
   result = client.change_permissions(
       entry_id=12345,
       user_id=67890,
       permissions=["view", "edit", "download"]
   )

   # Unshare
   result = client.unshare_entry(
       entry_id=12345,
       user_id=67890
   )

Shareable Links
~~~~~~~~~~~~~~~

.. code-block:: python

   # Create shareable link
   result = client.create_shareable_link(entry_id=12345)
   link_hash = result['link']['hash']
   print(f"Share link: https://dri.me/{link_hash}")

   # With password
   result = client.create_shareable_link(
       entry_id=12345,
       password="secret123"
   )

   # With expiration
   result = client.create_shareable_link(
       entry_id=12345,
       expires_at="2025-12-31T23:59:59.000000Z"
   )

   # With permissions
   result = client.create_shareable_link(
       entry_id=12345,
       allow_edit=True,
       allow_download=True
   )

   # Get existing link
   result = client.get_shareable_link(entry_id=12345)

   # Update link
   result = client.update_shareable_link(
       entry_id=12345,
       password="newpassword"
   )

   # Delete link
   result = client.delete_shareable_link(entry_id=12345)

Starring Operations
-------------------

.. code-block:: python

   # Star files
   result = client.star_entries([12345, 67890])

   # Unstar files
   result = client.unstar_entries([12345, 67890])

   # List starred files
   result = client.get_file_entries(starred_only=True)

Workspace Operations
--------------------

.. code-block:: python

   # Get user workspaces
   result = client.get_workspaces()
   for workspace in result['workspaces']:
       print(f"{workspace['name']} (ID: {workspace['id']})")

   # Get logged user info
   user_info = client.get_logged_user()
   print(f"Logged in as: {user_info['user']['email']}")

Authentication
--------------

Login
~~~~~

.. code-block:: python

   # Login to get access token
   result = client.login(
       email="user@example.com",
       password="password123",
       device_name="my-app"
   )
   access_token = result['user']['access_token']

   # Use token for subsequent requests
   client = DrimeClient(api_key=access_token)

Register
~~~~~~~~

.. code-block:: python

   # Register new account
   result = client.register(
       email="newuser@example.com",
       password="password123",
       device_name="my-app"
   )
   access_token = result['user']['access_token']

Advanced Usage
--------------

Custom API URL
~~~~~~~~~~~~~~

.. code-block:: python

   client = DrimeClient(
       api_key="your_key",
       api_url="https://custom.api.url/v1"
   )

Session Management
~~~~~~~~~~~~~~~~~~

The client uses a ``requests.Session`` object internally, which reuses connections for better performance:

.. code-block:: python

   # The session is automatically managed
   client = DrimeClient(api_key="your_key")

   # Multiple requests reuse the same connection
   client.upload_file(Path("file1.txt"))
   client.upload_file(Path("file2.txt"))
   client.upload_file(Path("file3.txt"))

Session Management
------------------

The client manages HTTP connections automatically. For batch operations, you can
explicitly close the client to ensure all requests are completed:

.. code-block:: python

   from pydrime import DrimeClient
   from pathlib import Path

   client = DrimeClient(api_key="your_key")

   # Upload multiple files
   for file in files:
       client.upload_file(file)

   # Close the client to ensure all uploads are complete
   client.close()

   # The client can be reused after closing - it will reconnect automatically
   client.list_files()

Validate Uploads
----------------

Before uploading, check for duplicates:

.. code-block:: python

   # Validate files before upload
   files = [
       {"name": "example.txt", "size": 1024, "relativePath": "docs/"},
       {"name": "image.jpg", "size": 51200, "relativePath": ""},
   ]
   result = client.validate_uploads(files, workspace_id=0)
   duplicates = result.get("duplicates", [])

   if duplicates:
       print(f"Duplicate files found: {duplicates}")

Get an available name when there's a conflict:

.. code-block:: python

   # Get available name for duplicate
   new_name = client.get_available_name("document.pdf", workspace_id=0)
   print(new_name)  # "document (1).pdf"

Folder Operations
-----------------

Get Folder Count
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get number of items in a folder
   count = client.get_folder_count(folder_id=481967773)
   print(f"Folder contains {count} items")

Get Folder Path
~~~~~~~~~~~~~~~

.. code-block:: python

   # Get folder path hierarchy
   result = client.get_folder_path("NDgxMDAzNjAzfA")
   for folder in result["path"]:
       print(folder["name"])

   # For vault folders
   result = client.get_folder_path("MzQ0MzB8cGFkZA", vault_id=784)

Get Folder by Name
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Find folder by name
   folder = client.get_folder_by_name("Documents")
   print(f"Folder ID: {folder['id']}")

   # Search in specific parent folder
   folder = client.get_folder_by_name("Subfolder", parent_id=12345)

   # Case-insensitive search
   folder = client.get_folder_by_name("docs", case_sensitive=False)

Resolve Identifiers
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Resolve folder by ID or name
   folder_id = client.resolve_folder_identifier("480432024")  # Returns 480432024
   folder_id = client.resolve_folder_identifier("Documents")  # Looks up by name

   # Resolve any entry (file or folder) by ID or name
   entry_id = client.resolve_entry_identifier("test1.txt")
   entry_id = client.resolve_entry_identifier("480432024")

Notifications
-------------

.. code-block:: python

   # Get user notifications
   result = client.get_notifications(per_page=10, page=1, workspace_id=0)
   notifications = result["pagination"]["data"]

   for notif in notifications:
       print(notif["data"]["lines"][0]["content"])

Notes
-----

.. code-block:: python

   # Get all notes
   notes = client.get_notes()
   for note in notes:
       print(f"{note['title']}: {note['body']}")

   # Get a single note by ID
   result = client.get_note(note_id=1958, workspace_id=1593)
   note = result["data"]
   print(f"{note['title']}: {note['body']}")

   # Update a note
   result = client.update_note(
       note_id=1958,
       title="Updated Title",
       body="<p>Updated content</p>"
   )
   print(result["success"])  # True

   # Update only the title
   result = client.update_note_title(
       note_id=1958,
       title="New Title",
       token="your_token"
   )
   print(result["success"])  # True

Space Usage
-----------

.. code-block:: python

   # Get storage usage information
   usage = client.get_space_usage()
   print(f"Used: {usage}")

Vault Operations
----------------

The vault provides encrypted file storage. Files are encrypted client-side before
upload and decrypted after download.

Get Vault Information
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get vault metadata
   result = client.get_vault()
   vault = result["vault"]
   print(f"Vault ID: {vault['id']}")

List Vault Files
~~~~~~~~~~~~~~~~

.. code-block:: python

   # List vault root
   result = client.get_vault_file_entries()
   for entry in result.get("pagination", {}).get("data", []):
       print(entry["name"])

   # List specific folder by hash
   result = client.get_vault_file_entries(folder_hash="MzQ0MzB8cGFkZA")

   # With pagination and sorting
   result = client.get_vault_file_entries(
       page=1,
       per_page=50,
       order_by="name",
       order_dir="asc"
   )

Download Vault Files
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Download encrypted file from vault
   path = client.download_vault_file("MzQ0MzF8cGFkZA")
   print(f"Downloaded to: {path}")

   # Download to specific path
   path = client.download_vault_file(
       "MzQ0MzF8cGFkZA",
       output_path=Path("/path/to/save/file.txt")
   )

Upload Vault Files
~~~~~~~~~~~~~~~~~~

Vault uploads require client-side encryption:

.. code-block:: python

   from pydrime.vault_crypto import unlock_vault, encrypt_file, encrypt_filename

   # Get vault info and unlock
   vault = client.get_vault()["vault"]
   vault_key = unlock_vault(
       password="your_password",
       salt=vault["salt"],
       check=vault["check"],
       iv=vault["iv"]
   )

   # Encrypt file content and name
   encrypted_content, content_iv = encrypt_file(vault_key, file_path)
   encrypted_name, name_iv = encrypt_filename(vault_key, "secret.txt")

   # Upload to vault
   result = client.upload_vault_file(
       file_path=Path("secret.txt"),
       encrypted_content=encrypted_content,
       encrypted_name=encrypted_name,
       name_iv=name_iv,
       content_iv=content_iv,
       vault_id=vault["id"],
       parent_id=None  # Upload to root
   )

Delete Vault Files
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Move vault file to trash
   client.delete_vault_file_entries([123])

   # Delete vault file permanently
   client.delete_vault_file_entries([123], delete_forever=True)

Create Vault Folder
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create folder in vault
   result = client.create_vault_folder("MyFolder", vault_id=784)
   folder_id = result["folder"]["id"]

   # Create subfolder
   result = client.create_vault_folder(
       "Subfolder",
       vault_id=784,
       parent_id=folder_id
   )

Type Hints
----------

The API includes comprehensive type hints:

.. code-block:: python

   from typing import Optional
   from pathlib import Path
   from pydrime import DrimeClient

   def upload_files(client: DrimeClient, files: list[Path]) -> list[dict]:
       results = []
       for file_path in files:
           result = client.upload_file(file_path)
           results.append(result)
       return results
