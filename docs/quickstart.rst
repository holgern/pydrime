Quick Start
===========

This guide will help you get started with PyDrime quickly.

Initial Setup
-------------

Configure your API key
~~~~~~~~~~~~~~~~~~~~~~

The easiest way to set up PyDrime is using the ``init`` command:

.. code-block:: bash

   pydrime init

This will:

1. Prompt you for your Drime Cloud API key
2. Validate the key with Drime Cloud
3. Store it securely in ``~/.config/pydrime/config``
4. Set appropriate file permissions (owner read/write only)

Alternative Configuration Methods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Environment Variable:**

.. code-block:: bash

   export DRIME_API_KEY="your_api_key_here"

**Configuration File:**

Create or edit ``~/.config/pydrime/config``:

.. code-block:: text

   DRIME_API_KEY=your_api_key_here

**Command-line Argument:**

.. code-block:: bash

   pydrime --api-key "your_api_key_here" upload myfile.txt

Configuration Priority
~~~~~~~~~~~~~~~~~~~~~~

The tool checks for API keys in the following order (highest to lowest priority):

1. Command-line ``--api-key`` argument
2. ``DRIME_API_KEY`` environment variable
3. ``~/.config/pydrime/config`` file
4. Local ``.env`` file

Basic Usage
-----------

Check Status
~~~~~~~~~~~~

Verify your API key and connection:

.. code-block:: bash

   pydrime status

Upload a File
~~~~~~~~~~~~~

.. code-block:: bash

   pydrime upload /path/to/file.txt

Upload to a specific workspace:

.. code-block:: bash

   pydrime upload /path/to/file.txt --workspace 123

Upload a Directory
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pydrime upload /path/to/directory

Specify a remote path:

.. code-block:: bash

   pydrime upload /path/to/file.txt --remote-path "folder/file.txt"

Advanced Upload Options
~~~~~~~~~~~~~~~~~~~~~~~

**Progress Tracking**

By default, uploads show an interactive progress bar with file-level details. For CI/CD environments or log files, use simple text progress:

.. code-block:: bash

   # CI/CD friendly progress (line-by-line updates)
   pydrime upload /path/to/directory --simple-progress

   # Silent mode (no progress output)
   pydrime upload /path/to/directory --no-progress

**Parallel Uploads**

Speed up directory uploads by uploading multiple files in parallel:

.. code-block:: bash

   # Upload with 10 parallel workers (default is 5, max is 20)
   pydrime upload /path/to/directory -j 10

   # Maximum parallelism
   pydrime upload /path/to/directory -j 20

**Duplicate Handling**

Control what happens when a file with the same name already exists:

.. code-block:: bash

   # Skip existing files (fastest for incremental uploads)
   pydrime upload /path/to/directory --on-duplicate skip

   # Rename new files as "file (1).txt", "file (2).txt", etc.
   pydrime upload /path/to/file.txt --on-duplicate rename

   # Replace existing files (old versions moved to trash)
   pydrime upload /path/to/file.txt --on-duplicate replace

   # Ask interactively for each duplicate
   pydrime upload /path/to/directory --on-duplicate ask

**Combining Options**

.. code-block:: bash

   # Fast incremental backup: parallel + skip duplicates + simple progress
   pydrime upload ./backup -j 10 --on-duplicate skip --simple-progress

   # CI/CD deployment: replace old files with silent mode
   pydrime upload ./dist --on-duplicate replace --no-progress

List Files
~~~~~~~~~~

List files in root:

.. code-block:: bash

   pydrime ls

List files in a specific folder by ID:

.. code-block:: bash

   pydrime ls 12345

List files in a specific folder by name:

.. code-block:: bash

   pydrime ls Documents

Search for files:

.. code-block:: bash

   pydrime ls --query "report"

Download Files
~~~~~~~~~~~~~~

Download a file by name:

.. code-block:: bash

   pydrime download test.txt

Download a file by hash:

.. code-block:: bash

   pydrime download abc123hash

Download a file by ID:

.. code-block:: bash

   pydrime download 480424796

Download a folder (automatically includes all contents):

.. code-block:: bash

   pydrime download my_folder

Download to a specific location:

.. code-block:: bash

   pydrime download my_folder --output /path/to/save

Download multiple files:

.. code-block:: bash

   pydrime download file1.txt file2.txt folder1

Skip existing files (useful for incremental downloads):

.. code-block:: bash

   pydrime download my_folder --on-duplicate skip

Rename duplicates to keep both versions:

.. code-block:: bash

   pydrime download test.txt --on-duplicate rename

Overwrite existing files (default):

.. code-block:: bash

   pydrime download test.txt --on-duplicate overwrite

Create Directory
~~~~~~~~~~~~~~~~

.. code-block:: bash

   pydrime mkdir "My Folder"

Create in a specific parent folder:

.. code-block:: bash

   pydrime mkdir "Subfolder" --parent-id 12345

Navigate Directories
~~~~~~~~~~~~~~~~~~~~

Change to a directory:

.. code-block:: bash

   # By ID
   pydrime cd 12345

   # By name
   pydrime cd "My Folder"

   # Go to parent
   pydrime cd ..

   # Go to root
   pydrime cd

Show current directory:

.. code-block:: bash

   pydrime pwd

Rename Files
~~~~~~~~~~~~

.. code-block:: bash

   # Rename by ID
   pydrime rename 12345 "New Name.txt"

   # Rename by name
   pydrime rename "oldname.txt" "newname.txt"

Delete Files
~~~~~~~~~~~~

.. code-block:: bash

   # Delete by ID
   pydrime rm 12345

   # Delete by name
   pydrime rm test.txt

   # Delete folder by name
   pydrime rm my_folder

Share Files
~~~~~~~~~~~

Create a shareable link:

.. code-block:: bash

   # Share by ID
   pydrime share 12345

   # Share by name
   pydrime share test.txt

With password protection:

.. code-block:: bash

   pydrime share 12345 --password "mypassword"

With expiration:

.. code-block:: bash

   pydrime share 12345 --expires "2025-12-31T23:59:59.000000Z"

Validate Uploads
~~~~~~~~~~~~~~~~

After uploading files, verify they exist in Drime Cloud with correct sizes:

.. code-block:: bash

   # Validate a single file
   pydrime validate test.txt

   # Validate a folder
   pydrime validate my_folder

   # Validate multiple paths
   pydrime validate file1.txt folder1 file2.txt

   # Use in scripts (check exit code)
   pydrime validate uploaded_files && echo "All files validated successfully"

The validate command is particularly useful for:

* Verifying uploads completed successfully
* CI/CD pipelines to ensure data integrity
* Checking backups match local files

Sync Files
~~~~~~~~~~

Synchronize local directory with Drime Cloud:

.. code-block:: bash

   # Default two-way sync
   pydrime sync ./my_folder

   # Sync with specific remote path
   pydrime sync ./docs -r remote_docs

   # Using sync modes explicitly
   pydrime sync /home/user/docs:twoWay:/Documents
   pydrime sync ./backup:localBackup:/Backup

Available sync modes:

* ``twoWay`` (``tw``) - Mirror changes in both directions
* ``localToCloud`` (``ltc``) - Upload local changes only
* ``localBackup`` (``lb``) - Upload to cloud, never delete
* ``cloudToLocal`` (``ctl``) - Download cloud changes only
* ``cloudBackup`` (``cb``) - Download from cloud, never delete

Preview sync changes without syncing:

.. code-block:: bash

   pydrime sync ./data --dry-run

Find Duplicates
~~~~~~~~~~~~~~~

Find and optionally delete duplicate files:

.. code-block:: bash

   # Show duplicates (dry run)
   pydrime find-duplicates

   # Find in specific folder
   pydrime find-duplicates --folder "My Documents" --recursive

   # Actually delete duplicates
   pydrime find-duplicates --delete

Storage Usage
~~~~~~~~~~~~~

Check your storage usage:

.. code-block:: bash

   pydrime usage

Server Features
~~~~~~~~~~~~~~~

WebDAV and REST server functionality has been moved to separate packages:

* **pywebdavserver** - Mount Drime Cloud as a network drive
* **pyrestserver** - Use Drime Cloud as a restic backup destination

Install them separately if needed:

.. code-block:: bash

   pip install pywebdavserver
   pip install pyrestserver

See their respective documentation for usage instructions.

Python API Usage
----------------

Basic Example
~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient
   from pathlib import Path

   # Initialize client
   client = DrimeClient(api_key="your_api_key_here")

   # Upload a file
   result = client.upload_file(Path("myfile.txt"))
   print(f"Uploaded: {result}")

   # List files
   files = client.list_files()
   for file in files.get("data", []):
       print(f"{file['name']} - {file['type']}")

   # Download a file
   saved_path = client.download_file("abc123hash")
   print(f"Downloaded to: {saved_path}")

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeAPIError, DrimeAuthenticationError

   try:
       client = DrimeClient(api_key="your_api_key_here")
       result = client.upload_file(Path("myfile.txt"))
   except DrimeAuthenticationError:
       print("Invalid API key!")
   except DrimeAPIError as e:
       print(f"API error: {e}")

Next Steps
----------

* See :doc:`cli` for complete CLI reference
* See :doc:`api` for Python API documentation
* See :doc:`exceptions` for error handling
