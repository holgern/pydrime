Command Line Interface
======================

This page documents all available PyDrime CLI commands.

Global Options
--------------

These options can be used with any command:

.. code-block:: bash

   pydrime [OPTIONS] COMMAND [ARGS]...

Options:

* ``-k, --api-key TEXT`` - Drime Cloud API key
* ``-q, --quiet`` - Suppress non-essential output
* ``--json`` - Output in JSON format
* ``-v, --verbose`` - Enable verbose/debug logging output
* ``--validate-schema`` - Enable API schema validation warnings (for debugging)
* ``--version`` - Show version and exit
* ``--help`` - Show help message

Commands
--------

init
~~~~

Initialize Drime Cloud configuration.

.. code-block:: bash

   pydrime init [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key (will prompt if not provided)

**Description:**

Stores your API key securely in ``~/.config/pydrime/config`` for future use. The command validates the API key before saving.

**Example:**

.. code-block:: bash

   pydrime init
   # or provide key directly
   pydrime init --api-key "your_key_here"

status
~~~~~~

Check API key validity and connection status.

.. code-block:: bash

   pydrime status [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Verifies that your API key is valid and displays information about the logged-in user.

**Example:**

.. code-block:: bash

   pydrime status

upload
~~~~~~

Upload a file or directory to Drime Cloud.

.. code-block:: bash

   pydrime upload [OPTIONS] PATH

**Arguments:**

* ``PATH`` - Local file or directory to upload

**Options:**

* ``-r, --remote-path TEXT`` - Remote destination path with folder structure
* ``-w, --workspace INTEGER`` - Workspace ID (default: 0 for personal space)
* ``-j, --workers INTEGER`` - Number of parallel upload workers (default: 5, range: 1-20)
* ``--chunk-size INTEGER`` - Chunk size in MB for multipart uploads (default: 25MB, range: 5-100MB)
* ``--multipart-threshold INTEGER`` - File size threshold in MB for using multipart upload (default: 30MB, minimum: 1MB)
* ``--on-duplicate [skip|rename|replace|ask]`` - How to handle duplicate files (default: skip)
* ``--simple-progress`` - Use simple text progress display (CI/CD friendly)
* ``--no-progress`` - Disable progress display
* ``--dry-run`` - Show what would be uploaded without actually uploading
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Uploads files to Drime Cloud with real-time progress tracking, automatic selection between simple and multipart upload based on file size, and flexible duplicate handling. Supports parallel uploads for faster transfers.

**Upload Destination Information:**

Before uploading, the command displays:

* **Workspace** - Shows the workspace name and ID where files will be uploaded

  * ``Workspace: Personal (0)`` - Personal workspace
  * ``Workspace: Team Workspace (5)`` - Custom workspace

* **Parent folder** - Shows the current folder context where files will be uploaded

  * ``Parent folder: / (Root, ID: 0)`` - Root directory
  * ``Parent folder: /Documents (ID: 123)`` - Specific folder

* **Remote path structure** - Shows the relative path structure if ``--remote-path`` is specified

This information is displayed in both regular uploads and ``--dry-run`` mode to help you verify where files will be stored.

**Upload Methods:**

* **Simple Upload** - For files smaller than ``--multipart-threshold`` (default: 30MB)

  - Single HTTP request upload
  - Fast for small files
  - Progress shows completion only (0% → 100%)

* **Multipart Upload** - For files larger than ``--multipart-threshold``

  - Files split into chunks (configurable with ``--chunk-size``)
  - More reliable for large files
  - Resumable on network failures
  - Real-time progress tracking per chunk
  - Default chunk size: 25MB

**Progress Bars:**

When uploading multiple files, you'll see:

* **Overall Progress** - Total bytes uploaded across all files
* **Individual File Progress** - Per-file upload progress with transfer speed and time estimates
* **Transfer Speed** - Real-time upload speed (MB/s)
* **Time Remaining** - Estimated time to completion
* **Time Elapsed** - Total time spent uploading

Failed uploads are marked with a red ✗ and their progress is rolled back from the overall total.

**Parallel Uploads:**

Use the ``-j/--workers`` option to upload multiple files simultaneously:

* ``-j 1`` - Sequential upload (default, safest)
* ``-j 4`` - Upload 4 files in parallel (recommended for many files)
* ``-j 8`` - Upload 8 files in parallel (for fast connections)

**Note:** Parallel workers only affect multiple file uploads. Single file uploads always use one connection but benefit from chunked multipart for large files.

**Performance Tuning:**

Adjust chunk size and multipart threshold based on your connection:

**Fast, Stable Connection:**

.. code-block:: bash

   pydrime upload bigfile.bin --chunk-size 50 --multipart-threshold 100

* Larger chunks (50MB)
* Higher threshold (100MB)
* Fewer API calls
* Faster overall

**Slow, Unstable Connection:**

.. code-block:: bash

   pydrime upload bigfile.bin --chunk-size 10 --multipart-threshold 20

* Smaller chunks (10MB)
* Lower threshold (20MB)
* More frequent progress updates
* Easier to recover from failures

**Batch Upload with Parallel Workers:**

.. code-block:: bash

   pydrime upload folder/ -j 4 --chunk-size 20

* 4 parallel uploads
* 20MB chunks for multipart files
* Optimal for uploading many files

**Duplicate Handling:**

When files with the same name already exist, the ``--on-duplicate`` option controls the behavior:

* ``ask`` - Prompt for each duplicate (default)
* ``replace`` - Delete existing file and upload new one
* ``rename`` - Automatically rename to avoid conflicts (e.g., ``file (1).txt``)
* ``skip`` - Skip uploading the duplicate file

**Examples:**

.. code-block:: bash

   # Upload a single file
   pydrime upload myfile.txt

   # Upload with custom chunk size (faster for stable connections)
   pydrime upload bigfile.zip --chunk-size 50

   # Upload with smaller chunks (more reliable for unstable connections)
   pydrime upload bigfile.zip --chunk-size 10 --multipart-threshold 15

   # Upload to specific workspace
   pydrime upload myfile.txt --workspace 123

   # Upload with remote path
   pydrime upload myfile.txt --remote-path "folder/file.txt"

   # Upload directory with parallel workers
   pydrime upload /path/to/directory -j 4

   # Upload with custom performance settings
   pydrime upload data/ -j 8 --chunk-size 30 --multipart-threshold 50

   # Dry run (preview only)
   pydrime upload /path/to/directory --dry-run

   # Upload without progress bars
   pydrime upload large_file.bin --no-progress

   # Auto-replace duplicates without asking
   pydrime upload folder/ --on-duplicate replace

   # Auto-rename duplicates
   pydrime upload folder/ --on-duplicate rename

   # Skip duplicate files (incremental upload)
   pydrime upload folder/ --on-duplicate skip


ls
~~

List files and folders in a Drime Cloud directory.

.. code-block:: bash

   pydrime ls [OPTIONS] [PARENT_IDENTIFIER]

**Arguments:**

* ``PARENT_IDENTIFIER`` - ID or name of parent folder (omit to list current directory or root)

**Options:**

* ``-d, --deleted`` - Show deleted files
* ``-s, --starred`` - Show starred files
* ``-r, --recent`` - Show recent files
* ``-S, --shared`` - Show shared files
* ``-p, --page TEXT`` - Display files in specified folder hash/page
* ``-w, --workspace INTEGER`` - Workspace ID
* ``-q, --query TEXT`` - Search by name
* ``-t, --type [folder|image|text|audio|video|pdf]`` - Filter by file type
* ``--recursive`` - List files recursively
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Similar to Unix ``ls`` command, displays file and folder names in a columnar format.
For detailed disk usage information including file sizes and metadata, use the ``du`` command.
Supports both numeric folder IDs and folder names (resolved in current working directory).

**Examples:**

.. code-block:: bash

   # List files in current directory
   pydrime ls

   # List files in folder by ID
   pydrime ls 12345

   # List files in folder by name
   pydrime ls Documents

   # Search for files
   pydrime ls --query "report"

   # List deleted files
   pydrime ls --deleted

   # List recursively
   pydrime ls --recursive

   # Output as JSON
   pydrime --json ls

du
~~

Show disk usage information for files and folders.

.. code-block:: bash

   pydrime du [OPTIONS] [PARENT_IDENTIFIER]

**Arguments:**

* ``PARENT_IDENTIFIER`` - ID or name of parent folder (omit to show current directory or root)

**Options:**

* ``-d, --deleted`` - Show deleted files
* ``-s, --starred`` - Show starred files
* ``-r, --recent`` - Show recent files
* ``-S, --shared`` - Show shared files
* ``-p, --page TEXT`` - Display files in specified folder hash/page
* ``-w, --workspace INTEGER`` - Workspace ID
* ``-q, --query TEXT`` - Search by name
* ``-t, --type [folder|image|text|audio|video|pdf]`` - Filter by file type
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Similar to Unix ``du`` command, displays detailed information about files and folders
including size, type, and metadata. Shows a summary with total size and file/folder counts.
The API automatically calculates folder sizes to include all files inside.
For a simple file listing, use the ``ls`` command.
Supports both numeric folder IDs and folder names (resolved in current working directory).

**Examples:**

.. code-block:: bash

   # Show disk usage for current directory
   pydrime du

   # Show disk usage for folder by ID
   pydrime du 12345

   # Show disk usage for folder by name
   pydrime du Documents

   # Output as JSON
   pydrime --json du

recent
~~~~~~

List recently accessed files.

.. code-block:: bash

   pydrime recent [OPTIONS]

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID
* ``-p, --page INTEGER`` - Page number (1-based, default: 1)
* ``--page-size INTEGER`` - Number of items per page (default: 50)
* ``--order-by [created_at|updated_at|name|file_size]`` - Field to order by (default: created_at)
* ``--order-dir [asc|desc]`` - Order direction (default: desc)

**Description:**

Shows files that have been recently created or modified, ordered by date.
Similar to the ``ls --recent`` flag but as a dedicated command with more options.

**Examples:**

.. code-block:: bash

   # List recent files
   pydrime recent

   # List page 2 of recent files
   pydrime recent --page 2

   # Order by last update time
   pydrime recent --order-by updated_at

   # Order ascending (oldest first)
   pydrime recent --order-dir asc

   # List recent files in specific workspace
   pydrime recent -w 1593

   # Output as JSON
   pydrime --json recent

trash
~~~~~

List deleted files and folders in trash.

.. code-block:: bash

   pydrime trash [OPTIONS]

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID
* ``-p, --page INTEGER`` - Page number (1-based, default: 1)
* ``--page-size INTEGER`` - Number of items per page (default: 50)
* ``--order-by [created_at|updated_at|name|file_size]`` - Field to order by (default: updated_at)
* ``--order-dir [asc|desc]`` - Order direction (default: desc)

**Description:**

Shows files and folders that have been deleted and are in the trash.
Similar to the ``ls --deleted`` flag but as a dedicated command with more options.

**Examples:**

.. code-block:: bash

   # List trashed files
   pydrime trash

   # List page 2 of trashed files
   pydrime trash --page 2

   # Order by name
   pydrime trash --order-by name

   # Order ascending
   pydrime trash --order-dir asc

   # List trash in specific workspace
   pydrime trash -w 1593

   # Output as JSON
   pydrime --json trash

starred
~~~~~~~

List starred files and folders.

.. code-block:: bash

   pydrime starred [OPTIONS]

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID
* ``-p, --page INTEGER`` - Page number (1-based, default: 1)
* ``--page-size INTEGER`` - Number of items per page (default: 50)
* ``--order-by [created_at|updated_at|name|file_size]`` - Field to order by (default: updated_at)
* ``--order-dir [asc|desc]`` - Order direction (default: desc)

**Description:**

Shows files and folders that have been marked as starred/favorites.
Similar to the ``ls --starred`` flag but as a dedicated command with more options.

**Examples:**

.. code-block:: bash

   # List starred files
   pydrime starred

   # List page 2 of starred files
   pydrime starred --page 2

   # Order by name
   pydrime starred --order-by name

   # Order ascending
   pydrime starred --order-dir asc

   # List starred files in specific workspace
   pydrime starred -w 1593

   # Output as JSON
   pydrime --json starred

mkdir
~~~~~

Create a directory in Drime Cloud.

.. code-block:: bash

   pydrime mkdir [OPTIONS] NAME

**Arguments:**

* ``NAME`` - Name of the directory to create

**Options:**

* ``-p, --parent-id INTEGER`` - Parent folder ID (omit to create in root)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Examples:**

.. code-block:: bash

   # Create folder in root
   pydrime mkdir "My Folder"

   # Create subfolder
   pydrime mkdir "Subfolder" --parent-id 12345

download
~~~~~~~~

Download file(s) or folder(s) from Drime Cloud.

.. code-block:: bash

   pydrime download [OPTIONS] ENTRY_IDENTIFIERS...

**Arguments:**

* ``ENTRY_IDENTIFIERS`` - One or more file/folder paths, names, hashes, or numeric IDs

**Options:**

* ``-o, --output TEXT`` - Output directory path (for folders or multiple files)
* ``-d, --on-duplicate [skip|overwrite|rename]`` - Action when file exists locally (default: overwrite)
* ``-j, --workers INTEGER`` - Number of parallel workers (default: 1, use 4-8 for parallel downloads)
* ``--no-progress`` - Disable progress bars
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Downloads files and folders from Drime Cloud. Supports multiple identifier types:

* **Names** - File or folder names (resolved in current working directory)
* **IDs** - Numeric entry IDs
* **Hashes** - Entry hash values

Folders are automatically downloaded recursively with all their contents and subfolder structure.

**Duplicate Handling:**

When a file already exists locally, the ``--on-duplicate`` option controls the behavior:

* ``skip`` - Skip existing files without downloading (efficient, no API calls)
* ``overwrite`` - Replace existing files (default behavior)
* ``rename`` - Create new file with unique name (e.g., ``file (1).txt``, ``file (2).txt``)

**Examples:**

.. code-block:: bash

   # Download file by name
   pydrime download test.txt

   # Download file by ID
   pydrime download 480424796

   # Download file by hash
   pydrime download NDgwNDI0Nzk2fA

   # Download folder (automatically recursive)
   pydrime download my_folder

   # Download folder to specific directory
   pydrime download my_folder --output /path/to/destination

   # Download multiple files
   pydrime download file1.txt file2.txt file3.txt

   # Mix names, IDs, and hashes
   pydrime download 480424796 NDgwNDI0ODAyfA test.txt

   # Skip existing files (incremental download)
   pydrime download my_folder --on-duplicate skip

   # Rename duplicates to keep both versions
   pydrime download test.txt --on-duplicate rename

   # Overwrite existing files (default)
   pydrime download test.txt --on-duplicate overwrite

rename
~~~~~~

Rename a file or folder entry.

.. code-block:: bash

   pydrime rename [OPTIONS] ENTRY_IDENTIFIER NEW_NAME

**Arguments:**

* ``ENTRY_IDENTIFIER`` - ID or name of the entry to rename (names are resolved in the current working directory)
* ``NEW_NAME`` - New name for the entry

**Options:**

* ``-d, --description TEXT`` - New description for the entry
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Supports both numeric IDs and file/folder names. Names are resolved in the current working directory.

**Examples:**

.. code-block:: bash

   # Rename by ID
   pydrime rename 12345 "New File Name.txt"

   # Rename by name
   pydrime rename test.txt "New File Name.txt"

   # Rename folder by name
   pydrime rename drime_test my_folder

   # Rename with description
   pydrime rename 12345 "New Name" --description "Updated file"

rm
~~

Delete one or more file or folder entries.

.. code-block:: bash

   pydrime rm [OPTIONS] ENTRY_IDENTIFIERS...

**Arguments:**

* ``ENTRY_IDENTIFIERS`` - One or more entry IDs or names to delete (names are resolved in the current working directory)

**Options:**

* ``--permanent`` - Delete permanently (cannot be undone)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Moves entries to trash or deletes them permanently. Requires confirmation before deletion.
Supports both numeric IDs and file/folder names resolved in the current working directory.

**Examples:**

.. code-block:: bash

   # Move to trash by ID
   pydrime rm 12345

   # Delete by name
   pydrime rm test.txt

   # Delete folder by name
   pydrime rm drime_test

   # Delete multiple files (mix IDs and names)
   pydrime rm 12345 test.txt folder_name

   # Delete permanently
   pydrime rm 12345 --permanent

share
~~~~~

Create a shareable link for a file or folder.

.. code-block:: bash

   pydrime share [OPTIONS] ENTRY_IDENTIFIER

**Arguments:**

* ``ENTRY_IDENTIFIER`` - ID or name of the entry to share (names are resolved in the current working directory)

**Options:**

* ``-p, --password TEXT`` - Optional password for the link
* ``-e, --expires TEXT`` - Expiration date (format: 2025-12-31T23:59:59.000000Z)
* ``--allow-edit`` - Allow editing through the link
* ``--allow-download`` - Allow downloading through the link (default: True)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Creates a public shareable link for a file or folder with optional password protection and expiration.
Supports both numeric IDs and file/folder names resolved in the current working directory.

**Examples:**

.. code-block:: bash

   # Simple share by ID
   pydrime share 12345

   # Share by name
   pydrime share test.txt

   # Share folder by name
   pydrime share drime_test

   # Password protected
   pydrime share 12345 --password "mypassword"

   # With expiration
   pydrime share 12345 --expires "2025-12-31T23:59:59.000000Z"

   # Allow editing
   pydrime share 12345 --allow-edit --allow-download

cd
~~

Change the current working directory.

.. code-block:: bash

   pydrime cd [OPTIONS] [FOLDER_IDENTIFIER]

**Arguments:**

* ``FOLDER_IDENTIFIER`` - Folder ID, name, or special value (omit for root, ``..`` for parent, ``/`` for root)

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Changes the current working directory context for subsequent commands. Similar to Unix ``cd`` command.
The current directory is saved in the configuration file and persists across sessions.
Supports folder IDs, folder names (resolved in current directory), and special values.

**Examples:**

.. code-block:: bash

   # Change to root directory
   pydrime cd

   # Change to folder by ID
   pydrime cd 12345

   # Change to folder by name
   pydrime cd my_folder

   # Go to parent directory
   pydrime cd ..

   # Go to root explicitly
   pydrime cd /
   pydrime cd 0

pwd
~~~

Print current working directory.

.. code-block:: bash

   pydrime pwd [OPTIONS]

**Options:**

* ``--id-only`` - Output only the folder ID
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Shows the current folder path with ID and default workspace. Similar to Unix ``pwd`` command.
The output includes the folder ID in the format ``/{folder_name} (ID: {id})`` or ``/ (ID: 0)`` for root.

**Examples:**

.. code-block:: bash

   # Show current directory
   pydrime pwd
   # Output: /Documents (ID: 12345)
   #         Workspace: 0

   # Show only the ID
   pydrime pwd --id-only
   # Output: 12345

   # Show in JSON format
   pydrime --json pwd
   # Output: {"id": 12345, "name": "Documents", "workspace": 0}

workspaces
~~~~~~~~~~

List all workspaces you have access to.

.. code-block:: bash

   pydrime workspaces [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Shows workspace name, ID, your role, and owner information for all workspaces you have access to.

**Example:**

.. code-block:: bash

   pydrime workspaces

workspace
~~~~~~~~~

Set or show the default workspace.

.. code-block:: bash

   pydrime workspace [OPTIONS] [WORKSPACE_IDENTIFIER]

**Arguments:**

* ``WORKSPACE_IDENTIFIER`` - ID or name of the workspace to set as default (omit to show current default)

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Sets or displays the default workspace used for operations. The workspace setting persists across sessions.
Supports both numeric workspace IDs and workspace names with case-insensitive matching.

When displaying the current workspace, shows both the workspace name and ID for clarity.

**Examples:**

.. code-block:: bash

   # Show current default workspace
   pydrime workspace
   # Output: Default workspace: Personal (0)
   #     or: Default workspace: Team Workspace (5)

   # Set workspace by ID
   pydrime workspace 5
   # Output: Set default workspace to: Team Workspace (5)

   # Set personal workspace
   pydrime workspace 0
   # Output: Set default workspace to: Personal (0)

   # Set workspace by name (case-insensitive)
   pydrime workspace test
   pydrime workspace "My Team"
   pydrime workspace TEAM  # Matches "team" workspace

validate
~~~~~~~~

Validate that local files/folders are correctly uploaded to Drime Cloud.

.. code-block:: bash

   pydrime validate [OPTIONS] PATHS...

**Arguments:**

* ``PATHS`` - One or more local file or directory paths to validate

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID (default: 0 for personal space)
* ``-r, --remote-path TEXT`` - Remote destination path
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Validates that local files and folders exist on Drime Cloud with matching sizes.
For each local path, the command:

1. Checks if a corresponding entry exists in Drime Cloud (by name)
2. Verifies that files have matching sizes
3. Reports any missing or mismatched entries

Exit codes:

* ``0`` - All files validated successfully
* ``1`` - Validation issues found (missing files or size mismatches)

**Examples:**

.. code-block:: bash

   # Validate a single file
   pydrime validate test.txt

   # Validate a directory
   pydrime validate my_folder

   # Validate multiple paths
   pydrime validate file1.txt folder1 file2.txt

   # Validate in specific workspace
   pydrime validate my_folder --workspace 123

   # Validate with remote path
   pydrime validate /path/to/local -r remote_folder

   # Output as JSON for scripting
   pydrime --json validate my_folder

   # Use in CI/CD (check exit code)
   pydrime validate uploaded_files && echo "Validation passed"

sync
~~~~

Sync files between local directory and Drime Cloud.

.. code-block:: bash

   pydrime sync [OPTIONS] PATH

**Arguments:**

* ``PATH`` - Local directory to sync OR literal sync pair in format ``/local/path:syncMode:/remote/path``

**Options:**

* ``-r, --remote-path TEXT`` - Remote destination path
* ``-w, --workspace INTEGER`` - Workspace ID (uses default workspace if not specified)
* ``-C, --config PATH`` - JSON config file with list of sync pairs
* ``--dry-run`` - Show what would be synced without syncing
* ``--no-progress`` - Disable progress bars
* ``-c, --chunk-size INTEGER`` - Chunk size in MB for multipart uploads (default: 25MB)
* ``-m, --multipart-threshold INTEGER`` - File size threshold in MB for using multipart upload (default: 30MB)
* ``-b, --batch-size INTEGER`` - Number of remote files to process per batch in streaming mode (default: 50)
* ``--no-streaming`` - Disable streaming mode (scan all files upfront instead of batch processing)
* ``--workers INTEGER`` - Number of parallel workers for uploads/downloads (default: 1)
* ``--start-delay FLOAT`` - Delay in seconds between starting each parallel operation (default: 0.0)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Sync Modes:**

* ``twoWay`` (``tw``) - Mirror every action in both directions
* ``localToCloud`` (``ltc``) - Mirror local actions to cloud only
* ``localBackup`` (``lb``) - Upload to cloud, never delete
* ``cloudToLocal`` (``ctl``) - Mirror cloud actions to local only
* ``cloudBackup`` (``cb``) - Download from cloud, never delete

**Description:**

Synchronizes files between a local directory and Drime Cloud. Supports multiple
sync modes for different use cases. Can be specified as a simple directory path
(uses two-way sync by default) or as a literal sync pair with explicit mode.

**Ignore Files (.pydrignore):**

You can exclude files from sync by placing a ``.pydrignore`` file in any directory.
This works similarly to Kopia's ``.kopiaignore`` or Git's ``.gitignore``.

The ``.pydrignore`` file uses gitignore-style pattern matching:

.. code-block:: text

   # Comment lines start with #
   *.log           # Ignore all .log files anywhere
   /logs           # Ignore 'logs' only at root directory
   temp/           # Ignore directories named 'temp'
   !important.log  # Un-ignore important.log (negation)
   *.db*           # Ignore files with .db in extension
   **/cache/**     # Ignore any 'cache' directory and contents
   [a-z]*.tmp      # Character ranges and wildcards
   ?tmp.db         # ? matches exactly one character

**Pattern Syntax:**

+---------------+-----------------------------------------------------------+
| Pattern       | Description                                               |
+===============+===========================================================+
| ``#``         | Comment line (ignored)                                    |
+---------------+-----------------------------------------------------------+
| ``!``         | Negates a rule (un-ignores previously ignored path)       |
+---------------+-----------------------------------------------------------+
| ``*``         | Wildcard matching any characters (except ``/``)           |
+---------------+-----------------------------------------------------------+
| ``**``        | Double wildcard matching any path components              |
+---------------+-----------------------------------------------------------+
| ``?``         | Matches exactly one character                             |
+---------------+-----------------------------------------------------------+
| ``[abc]``     | Matches one of ``a``, ``b``, or ``c``                     |
+---------------+-----------------------------------------------------------+
| ``[a-z]``     | Matches characters in range ``a`` to ``z``                |
+---------------+-----------------------------------------------------------+
| ``/`` (start) | Anchored to root directory only                           |
+---------------+-----------------------------------------------------------+
| ``/`` (end)   | Matches directories only                                  |
+---------------+-----------------------------------------------------------+

**Hierarchical Ignore Files:**

``.pydrignore`` files in subdirectories only apply to that subtree and can
override parent rules using negation (``!``). For example:

.. code-block:: text

   # Root .pydrignore
   *.log

   # subdir/.pydrignore
   !debug.log    # Un-ignore debug.log in this subdirectory

**Example .pydrignore file:**

.. code-block:: text

   # Ignore temporary files
   *.tmp
   *.log
   *.bak

   # Ignore cache and build directories
   **/cache/**
   **/node_modules/**
   build/
   dist/

   # Ignore database files
   *.db
   *.sqlite

   # But keep important logs
   !important.log

   # Ignore only at root level
   /logs/*
   /.git/

**JSON Config File (--config):**

You can define multiple sync pairs in a JSON configuration file. This is useful for
setting up complex sync configurations or running multiple sync operations at once.

.. code-block:: json

   [
     {
       "workspace": 0,
       "local": "/path/to/local",
       "remote": "remote/path",
       "syncMode": "twoWay",
       "disableLocalTrash": false,
       "ignore": ["*.tmp"],
       "excludeDotFiles": false
     },
     {
       "workspace": 5,
       "local": "/home/user/docs",
       "remote": "Documents",
       "syncMode": "localBackup"
     }
   ]

**Config File Fields:**

+----------------------+-----------------------------------------------------------+
| Field                | Description                                               |
+======================+===========================================================+
| ``workspace``        | Workspace ID (optional, default: 0)                       |
+----------------------+-----------------------------------------------------------+
| ``local``            | Local directory path (required)                           |
+----------------------+-----------------------------------------------------------+
| ``remote``           | Remote destination path (required)                        |
+----------------------+-----------------------------------------------------------+
| ``syncMode``         | Sync mode: twoWay, localToCloud, localBackup, etc.        |
+----------------------+-----------------------------------------------------------+
| ``disableLocalTrash``| If true, permanently delete local files (default: false)  |
+----------------------+-----------------------------------------------------------+
| ``ignore``           | List of additional ignore patterns (optional)             |
+----------------------+-----------------------------------------------------------+
| ``excludeDotFiles``  | If true, exclude all dotfiles (default: false)            |
+----------------------+-----------------------------------------------------------+

**Examples:**

.. code-block:: bash

   # Directory path with default two-way sync
   pydrime sync ./my_folder
   pydrime sync ./docs -r remote_docs

   # Literal sync pairs with explicit modes
   pydrime sync /home/user/docs:twoWay:/Documents
   pydrime sync /home/user/pics:localToCloud:/Pictures
   pydrime sync ./local:localBackup:/Backup
   pydrime sync ./data:cloudToLocal:/CloudData
   pydrime sync ./archive:cloudBackup:/Archive

   # With abbreviations
   pydrime sync /home/user/pics:tw:/Pictures
   pydrime sync ./backup:ltc:/CloudBackup
   pydrime sync ./local:lb:/Backup

   # Other options
   pydrime sync . -w 5                          # Sync in workspace 5
   pydrime sync ./data --dry-run                # Preview sync changes
   pydrime sync ./data -b 100                   # Process 100 files per batch
   pydrime sync ./data --no-streaming           # Scan all files upfront

   # JSON config file with multiple sync pairs
   pydrime sync --config sync_pairs.json
   pydrime sync -C sync_pairs.json --dry-run

stat
~~~~

Show detailed statistics for a file or folder.

.. code-block:: bash

   pydrime stat [OPTIONS] IDENTIFIER

**Arguments:**

* ``IDENTIFIER`` - File/folder path, name, hash, or numeric ID

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Displays detailed metadata for a file or folder including size, type, timestamps,
owner, and other properties. Supports paths, names (resolved in current directory),
numeric IDs, and hashes.

**Examples:**

.. code-block:: bash

   # By name in current folder
   pydrime stat my-file.txt

   # By path
   pydrime stat myfolder/my-file.txt

   # By numeric ID
   pydrime stat 480424796

   # By hash
   pydrime stat NDgwNDI0Nzk2fA

   # Folder by name
   pydrime stat "My Documents"

cat
~~~

Print file contents to standard output.

.. code-block:: bash

   pydrime cat [OPTIONS] IDENTIFIER

**Arguments:**

* ``IDENTIFIER`` - File path, name, hash, or numeric ID

**Options:**

* ``-n, --number`` - Number all output lines
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Displays the entire contents of a cloud file. Similar to Unix ``cat`` command.
For binary files, use the download command instead.

**Examples:**

.. code-block:: bash

   # By name
   pydrime cat readme.txt

   # By path
   pydrime cat folder/config.json

   # By numeric ID
   pydrime cat 480424796

   # By hash
   pydrime cat NDgwNDI0Nzk2fA

   # With line numbers
   pydrime cat readme.txt -n

   # Output as JSON
   pydrime --json cat readme.txt

head
~~~~

Print first lines of a file.

.. code-block:: bash

   pydrime head [OPTIONS] IDENTIFIER

**Arguments:**

* ``IDENTIFIER`` - File path, name, hash, or numeric ID

**Options:**

* ``-n, --lines INTEGER`` - Number of lines to show (default: 10)
* ``-c, --bytes INTEGER`` - Number of bytes to show (overrides -n)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Displays the first N lines (default: 10) or bytes of a cloud file.
Similar to Unix ``head`` command.

**Examples:**

.. code-block:: bash

   # First 10 lines (default)
   pydrime head readme.txt

   # First 20 lines
   pydrime head readme.txt -n 20

   # First 100 bytes
   pydrime head config.json -c 100

   # By path
   pydrime head folder/file.txt

tail
~~~~

Print last lines of a file.

.. code-block:: bash

   pydrime tail [OPTIONS] IDENTIFIER

**Arguments:**

* ``IDENTIFIER`` - File path, name, hash, or numeric ID

**Options:**

* ``-n, --lines INTEGER`` - Number of lines to show (default: 10)
* ``-c, --bytes INTEGER`` - Number of bytes to show (overrides -n)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Displays the last N lines (default: 10) or bytes of a cloud file.
Similar to Unix ``tail`` command.

**Examples:**

.. code-block:: bash

   # Last 10 lines (default)
   pydrime tail logfile.log

   # Last 20 lines
   pydrime tail readme.txt -n 20

   # Last 500 bytes
   pydrime tail logfile.log -c 500

   # By path
   pydrime tail folder/file.txt

folders
~~~~~~~

List all folders in a workspace.

.. code-block:: bash

   pydrime folders [OPTIONS]

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID (default: 0 for personal workspace)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Shows folder ID, name, parent ID, and path for all folders accessible to the
current user in the specified workspace.

**Example:**

.. code-block:: bash

   pydrime folders
   pydrime folders --workspace 5

usage
~~~~~

Display storage space usage information.

.. code-block:: bash

   pydrime usage [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Shows how much storage you've used and how much is available, including
a percentage usage indicator.

**Example:**

.. code-block:: bash

   pydrime usage

find-duplicates
~~~~~~~~~~~~~~~

Find and optionally delete duplicate files.

.. code-block:: bash

   pydrime find-duplicates [OPTIONS]

**Options:**

* ``-w, --workspace INTEGER`` - Workspace ID (0 for personal workspace)
* ``-f, --folder TEXT`` - Folder ID or name to scan (omit for root folder)
* ``-r, --recursive`` - Scan recursively into subfolders
* ``--dry-run`` - Show duplicates without deleting (default)
* ``--delete`` - Actually delete duplicate files (moves to trash)
* ``--keep-newest`` - Keep newest file instead of oldest (default: keep oldest)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Duplicates are identified by having identical filename, size, and parent folder.
By default, the oldest file (lowest ID) is kept and newer duplicates are deleted.

**Examples:**

.. code-block:: bash

   # Dry run (show duplicates without deleting)
   pydrime find-duplicates

   # Find duplicates in a specific folder by ID
   pydrime find-duplicates --folder 12345

   # Find duplicates in a specific folder by name
   pydrime find-duplicates --folder "My Documents"

   # Find duplicates recursively
   pydrime find-duplicates --recursive

   # Actually delete duplicates (moves to trash)
   pydrime find-duplicates --delete

   # Keep newest file instead of oldest
   pydrime find-duplicates --delete --keep-newest

Vault Commands
--------------

The vault provides encrypted file storage. Access vault commands via ``pydrime vault``.

vault show
~~~~~~~~~~

Show vault information.

.. code-block:: bash

   pydrime vault show [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Displays metadata about your encrypted vault including ID and timestamps.

**Example:**

.. code-block:: bash

   pydrime vault show

vault unlock
~~~~~~~~~~~~

Unlock the vault for the current shell session.

.. code-block:: bash

   pydrime vault unlock [OPTIONS]

**Options:**

* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Prompts for your vault password and outputs shell commands to set an environment
variable. The password is stored in memory only and never written to disk.

**Usage:**

.. code-block:: bash

   # bash/zsh
   eval $(pydrime vault unlock)

   # fish
   pydrime vault unlock | source

After unlocking, vault commands won't prompt for password.
Use ``pydrime vault lock`` to clear the password from your session.

vault lock
~~~~~~~~~~

Lock the vault and clear password from shell session.

.. code-block:: bash

   pydrime vault lock

**Description:**

Outputs shell commands to unset the vault password environment variable.

**Usage:**

.. code-block:: bash

   # bash/zsh
   eval $(pydrime vault lock)

   # fish
   pydrime vault lock | source

vault ls
~~~~~~~~

List files and folders in the vault.

.. code-block:: bash

   pydrime vault ls [OPTIONS] [FOLDER_IDENTIFIER]

**Arguments:**

* ``FOLDER_IDENTIFIER`` - Folder name, ID, or hash to list (default: root)

**Options:**

* ``-p, --page INTEGER`` - Page number (default: 1)
* ``--page-size INTEGER`` - Number of items per page (default: 50)
* ``--order-by [updated_at|created_at|name|file_size]`` - Field to order by (default: updated_at)
* ``--order [asc|desc]`` - Order direction (default: desc)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Examples:**

.. code-block:: bash

   # List root vault folder
   pydrime vault ls

   # List folder by name
   pydrime vault ls Test1

   # List folder by ID
   pydrime vault ls 34430

   # List folder by hash
   pydrime vault ls MzQ0MzB8cGFkZA

   # Show page 2 of results
   pydrime vault ls --page 2

   # Sort by name
   pydrime vault ls --order-by name

vault download
~~~~~~~~~~~~~~

Download a file from the vault.

.. code-block:: bash

   pydrime vault download [OPTIONS] FILE_IDENTIFIER

**Arguments:**

* ``FILE_IDENTIFIER`` - File path, name, ID, or hash to download

**Options:**

* ``-o, --output PATH`` - Output file path (default: current directory with original filename)
* ``-p, --password TEXT`` - Vault password (will prompt if not provided)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Downloads an encrypted file from your vault and decrypts it locally.
You will be prompted for your vault password if not provided.

**Examples:**

.. code-block:: bash

   # Download from root
   pydrime vault download document.pdf

   # Download from subfolder
   pydrime vault download Test1/document.pdf

   # Download by ID
   pydrime vault download 34431

   # Download by hash
   pydrime vault download MzQ0MzF8cGFkZA

   # Download to specific path
   pydrime vault download doc.pdf -o out.pdf

vault upload
~~~~~~~~~~~~

Upload a file to the vault with encryption.

.. code-block:: bash

   pydrime vault upload [OPTIONS] FILE_PATH

**Arguments:**

* ``FILE_PATH`` - Path to the local file to upload

**Options:**

* ``-f, --folder TEXT`` - Target folder name, ID, or hash in vault (default: root)
* ``-p, --password TEXT`` - Vault password (will prompt if not provided)
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

Encrypts a local file and uploads it to your encrypted vault.
You will be prompted for your vault password if not provided.

**Examples:**

.. code-block:: bash

   # Upload to vault root
   pydrime vault upload secret.txt

   # Upload to folder
   pydrime vault upload document.pdf -f MyFolder

   # With password option
   pydrime vault upload photo.jpg -p mypassword

vault rm
~~~~~~~~

Delete a file or folder from the vault.

.. code-block:: bash

   pydrime vault rm [OPTIONS] FILE_IDENTIFIER

**Arguments:**

* ``FILE_IDENTIFIER`` - File or folder name, ID, or hash to delete

**Options:**

* ``--no-trash`` - Delete permanently instead of moving to trash
* ``-y, --yes`` - Skip confirmation prompt
* ``-k, --api-key TEXT`` - Drime Cloud API key

**Description:**

By default, files are moved to trash. Use ``--no-trash`` to delete permanently.

**Examples:**

.. code-block:: bash

   # Move to trash
   pydrime vault rm secret.txt

   # Delete permanently
   pydrime vault rm secret.txt --no-trash

   # Delete by ID
   pydrime vault rm 34431

   # Delete by hash
   pydrime vault rm MzQ0MzF8cGFkZA

   # Skip confirmation
   pydrime vault rm MyFolder -y

.. note::

   **Server Features Moved to Separate Packages**

   WebDAV and REST server functionality has been moved to dedicated packages:

   * **pywebdavserver** - WebDAV server for mounting Drime Cloud as a network drive
   * **pyrestserver** - REST server for restic backup integration

   Install these packages separately if you need server functionality:

   .. code-block:: bash

      pip install pywebdavserver
      pip install pyrestserver

   See their respective documentation for usage instructions.
