[![PyPI - Version](https://img.shields.io/pypi/v/pydrime)](https://pypi.org/project/pydrime/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydrime)
![PyPI - Downloads](https://img.shields.io/pypi/dm/pydrime)
[![codecov](https://codecov.io/gh/holgern/pydrime/graph/badge.svg?token=iCHXwbjAXG)](https://codecov.io/gh/holgern/pydrime)

# PyDrime

A command-line tool for uploading files and directories to Drime Cloud. This library was
influenced by the great
[filen.io cli tool](https://github.com/FilenCloudDienste/filen-cli) and it's
[sync lib](https://github.com/FilenCloudDienste/filen-sync).

## ⚠️ Disclaimer

**PyDrime is an unofficial, community-developed library and is not affiliated with,
endorsed by, or supported by Drime or the Drime Cloud service.**

This software is provided "as is", without warranty of any kind, express or implied,
including but not limited to the warranties of merchantability, fitness for a particular
purpose and noninfringement. In no event shall the authors or copyright holders be
liable for any claim, damages or other liability, whether in an action of contract, tort
or otherwise, arising from, out of or in connection with the software or the use or
other dealings in the software.

**Use at your own risk.** The authors are not responsible for any data loss, corruption,
or other issues that may arise from using this tool. Always maintain backups of your
important data.

## Features

- Upload individual files or entire directories
- Download files and folders by name, ID, or hash
- **Sync** local directories with Drime Cloud (bidirectional and one-way modes)
- **Encrypted Vault** for secure file storage with client-side encryption
- Recursive directory scanning
- Progress tracking with visual feedback
- Dry-run mode to preview uploads
- Parallel uploads/downloads with configurable workers
- Duplicate file detection and removal
- Support for environment-based configuration
- Rich terminal output with colors
- JSON output for scripting and automation

## Installation

### From source

```bash
# Clone the repository
git clone https://github.com/holgern/pydrime.git
cd pydrime

# Install in development mode
pip install -e .

# Or install with dev dependencies
pip install -e ".[dev]"

# Or install with dev dependencies
pip install -e ".[dev]"

```

### Termux specific install instruction

```bash
pkg install -y rust binutils
```

### From pypi

```bash
pip install pydrime
```

## Configuration

Before using the tool, you need to configure your Drime Cloud API credentials.

### Recommended: Use `pydrime init` Command

The easiest way to configure your API key:

```bash
pydrime init
```

This will:

1. Prompt you for your API key
2. Validate the key with Drime Cloud
3. Store it securely in `~/.config/pydrime/config`
4. Set appropriate file permissions (owner read/write only)

### Alternative Configuration Methods

#### Option 1: Environment Variable

```bash
export DRIME_API_KEY="your_api_key_here"
```

#### Option 2: .env File

Create a `.env` file in your project directory:

```bash
cp .env.example .env
# Edit .env and add your API key
```

#### Option 3: Command-line Argument

Pass the API key directly when running commands:

```bash
pydrime upload /path/to/file --api-key "your_api_key_here"
```

### Configuration Priority

The tool checks for API keys in the following order (highest to lowest priority):

1. Command-line `--api-key` argument
2. `DRIME_API_KEY` environment variable
3. `~/.config/pydrime/config` file
4. Local `.env` file

## Usage

### Initial Setup

Configure your API key (first time only):

```bash
pydrime init
```

### Check Connection Status

Verify your API key and connection:

```bash
pydrime status
```

### Upload a File

```bash
pydrime upload /path/to/file.txt
```

### Upload to Specific Workspace

```bash
pydrime upload /path/to/file.txt --workspace 123
```

### Upload a Directory

```bash
pydrime upload /path/to/directory
```

### Specify Remote Path

```bash
pydrime upload /path/to/file.txt --remote-path "folder/file.txt"
```

### Dry Run (Preview)

```bash
pydrime upload /path/to/directory --dry-run
```

### List Remote Files

List files in root:

```bash
pydrime ls
```

List files in a specific folder (by ID):

```bash
pydrime ls 12345
```

Search for files:

```bash
pydrime ls --query "report"
```

### Create Remote Directory

Create folder in root:

```bash
pydrime mkdir "My Folder"
```

Create folder in a specific parent (by ID):

```bash
pydrime mkdir "Subfolder" --parent-id 12345
```

### Download Files

Download a single file:

```bash
pydrime download abc123hash
```

Download multiple files:

```bash
pydrime download hash1 hash2 hash3
```

Download to specific location:

```bash
pydrime download abc123hash --output /path/to/save/file.txt
```

### Rename Files

Rename by ID:

```bash
pydrime rename 12345 "New File Name.txt"
```

Rename by name (in current directory):

```bash
pydrime rename "oldname.txt" "newname.txt"
```

Rename with description:

```bash
pydrime rename 12345 "New Name" --description "Updated file"
```

### Delete Files

Move to trash by ID:

```bash
pydrime rm 12345
```

Delete by name (in current directory):

```bash
pydrime rm test.txt
```

Delete folder by name:

```bash
pydrime rm drime_test
```

Delete multiple files (mix IDs and names):

```bash
pydrime rm 12345 test.txt folder_name
```

Delete permanently (cannot be undone):

```bash
pydrime rm 12345 --permanent
```

### Share Files

Create a shareable link by ID:

```bash
pydrime share 12345
```

Share by name (in current directory):

```bash
pydrime share test.txt
```

Create password-protected link:

```bash
pydrime share 12345 --password "mypassword"
```

Create link with expiration:

```bash
pydrime share 12345 --expires "2025-12-31T23:59:59.000000Z"
```

Create link allowing edits:

```bash
pydrime share 12345 --allow-edit --allow-download
```

### List Workspaces

```bash
pydrime workspaces
```

### Sync Files

Synchronize local directory with Drime Cloud:

```bash
# Default two-way sync
pydrime sync ./my_folder

# Sync with specific remote path
pydrime sync ./docs -r remote_docs

# Preview sync changes (dry run)
pydrime sync ./data --dry-run

# Using sync modes explicitly (format: local:mode:remote)
pydrime sync /home/user/docs:twoWay:/Documents
pydrime sync ./backup:localBackup:/Backup
```

**Sync Modes:**

- `twoWay` (`tw`) - Mirror changes in both directions
- `localToCloud` (`ltc`) - Upload local changes only
- `localBackup` (`lb`) - Upload to cloud, never delete
- `cloudToLocal` (`ctl`) - Download cloud changes only
- `cloudBackup` (`cb`) - Download from cloud, never delete

**Ignore Files (.pydrignore):**

You can exclude files from sync and upload operations by creating `.pydrignore` files.
These work like `.gitignore` files with support for gitignore-style patterns:

```bash
# Example .pydrignore file
*.log           # Ignore all .log files
node_modules/   # Ignore node_modules directory
!important.log  # But keep important.log
/build          # Ignore build only at root
**/cache/**     # Ignore cache directories anywhere
```

`.pydrignore` files are hierarchical - rules in subdirectories can override parent
rules.

### Find Duplicates

Find and optionally delete duplicate files:

```bash
# Show duplicates (dry run)
pydrime find-duplicates

# Find in specific folder recursively
pydrime find-duplicates --folder "My Documents" --recursive

# Actually delete duplicates (moves to trash)
pydrime find-duplicates --delete
```

### Storage Usage

Check your storage usage:

```bash
pydrime usage
```

### Vault Commands

The vault provides encrypted file storage with client-side encryption:

```bash
# Show vault information
pydrime vault show

# Unlock vault for current session
eval $(pydrime vault unlock)

# List vault files
pydrime vault ls
pydrime vault ls MyFolder

# Upload file to vault (will prompt for password)
pydrime vault upload secret.txt
pydrime vault upload document.pdf -f MyFolder

# Download file from vault
pydrime vault download secret.txt
pydrime vault download secret.txt -o decrypted.txt

# Delete vault file
pydrime vault rm secret.txt

# Lock vault (clear password from session)
eval $(pydrime vault lock)
```

## Server Features

WebDAV and REST server functionality has been moved to separate packages:

### pywebdavserver

Mount Drime Cloud as a network drive via WebDAV protocol.

```bash
pip install pywebdavserver
pywebdavserver serve
```

See the
[pywebdavserver documentation](https://github.com/holgern/pydrime/tree/main/pywebdavserver)
for more information.

### pyrestserver

Use Drime Cloud as a backup destination for [restic](https://restic.net/).

```bash
pip install pyrestserver
pyrestserver serve
```

See the
[pyrestserver documentation](https://github.com/holgern/pydrime/tree/main/pyrestserver)
for more information.

## Command Reference

### init

Initialize Drime Cloud configuration.

```bash
pydrime init [OPTIONS]
```

**Options:**

- `-k, --api-key TEXT`: Drime Cloud API key (will prompt if not provided)

**Description:** Stores your API key securely in `~/.config/pydrime/config` for future
use. The command validates the API key before saving.

### status

Check API key validity and connection status.

```bash
pydrime status [OPTIONS]
```

**Options:**

- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Verifies that your API key is valid and displays information about the
logged-in user.

### upload

Upload a file or directory to Drime Cloud.

```bash
pydrime upload [OPTIONS] PATH
```

**Arguments:**

- `PATH`: Local file or directory to upload

**Options:**

- `-r, --remote-path TEXT`: Remote destination path with folder structure (e.g.,
  "/folder1/folder2/file.txt")
- `-w, --workspace INTEGER`: Workspace ID (default: 0 for personal space)
- `-k, --api-key TEXT`: Drime Cloud API key
- `--dry-run`: Show what would be uploaded without actually uploading

**Description:** Uploads files to Drime Cloud. For files larger than 30MB, automatically
uses multipart upload for better reliability.

### ls

List files in a Drime Cloud directory.

```bash
pydrime ls [OPTIONS] [PARENT_ID]
```

**Arguments:**

- `PARENT_ID`: ID of parent folder (omit to list root files)

**Options:**

- `-q, --query TEXT`: Search query to filter files by name
- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Lists files and displays them in a formatted table showing ID, name,
type, and size.

### mkdir

Create a directory in Drime Cloud.

```bash
pydrime mkdir [OPTIONS] NAME
```

**Arguments:**

- `NAME`: Name of the directory to create

**Options:**

- `-p, --parent-id INTEGER`: Parent folder ID (omit to create in root)
- `-k, --api-key TEXT`: Drime Cloud API key

### download

Download file(s) from Drime Cloud by hash.

```bash
pydrime download [OPTIONS] HASH_VALUES...
```

**Arguments:**

- `HASH_VALUES`: One or more file hashes to download

**Options:**

- `-o, --output TEXT`: Output file path (for single file only)
- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Downloads files from Drime Cloud using their hash values. Can download
multiple files at once.

### rename

Rename a file or folder entry.

```bash
pydrime rename [OPTIONS] ENTRY_IDENTIFIER NEW_NAME
```

**Arguments:**

- `ENTRY_IDENTIFIER`: ID or name of the entry to rename (names are resolved in the
  current working directory)
- `NEW_NAME`: New name for the entry

**Options:**

- `-d, --description TEXT`: New description for the entry
- `-k, --api-key TEXT`: Drime Cloud API key

**Examples:**

```bash
pydrime rename 12345 "newfile.txt"         # Rename by ID
pydrime rename test.txt "newfile.txt"      # Rename by name
pydrime rename drime_test my_folder        # Rename folder by name
pydrime rename test.txt file.txt -d "Desc" # Rename with description
```

### rm

Delete one or more file or folder entries.

```bash
pydrime rm [OPTIONS] ENTRY_IDENTIFIERS...
```

**Arguments:**

- `ENTRY_IDENTIFIERS`: One or more entry IDs or names to delete (names are resolved in
  the current working directory)

**Options:**

- `--permanent`: Delete permanently (cannot be undone)
- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Moves entries to trash or deletes them permanently. Requires
confirmation before deletion. Supports both numeric IDs and file/folder names.

**Examples:**

```bash
pydrime rm 480424796                    # Delete by ID
pydrime rm test1.txt                    # Delete by name
pydrime rm drime_test                   # Delete folder by name
pydrime rm test1.txt test2.txt          # Delete multiple files
pydrime rm 480424796 drime_test         # Mix IDs and names
pydrime rm --permanent test1.txt        # Permanent deletion
```

### share

Create a shareable link for a file or folder.

```bash
pydrime share [OPTIONS] ENTRY_IDENTIFIER
```

**Arguments:**

- `ENTRY_IDENTIFIER`: ID or name of the entry to share (names are resolved in the
  current working directory)

**Options:**

- `-p, --password TEXT`: Optional password for the link
- `-e, --expires TEXT`: Expiration date (format: 2025-12-31T23:59:59.000000Z)
- `--allow-edit`: Allow editing through the link
- `--allow-download`: Allow downloading through the link (default: True)
- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Creates a public shareable link for a file or folder with optional
password protection and expiration.

**Examples:**

```bash
pydrime share 480424796                   # Share by ID
pydrime share test1.txt                   # Share by name
pydrime share drime_test                  # Share folder by name
pydrime share test.txt -p mypass123       # Share with password
pydrime share test.txt -e 2025-12-31      # Share with expiration
pydrime share test.txt --allow-edit       # Allow editing
```

### workspaces

List all workspaces you have access to.

```bash
pydrime workspaces [OPTIONS]
```

**Options:**

- `-k, --api-key TEXT`: Drime Cloud API key

**Description:** Shows workspace name, ID, your role, and owner information for all
workspaces you have access to.

## Development

### Setup Development Environment

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/

# Lint code
ruff check src/
```

## API Integration Notes

This tool is designed to work with the Drime Cloud API. The current implementation
includes placeholder endpoints that need to be updated based on the actual API
documentation.

### TODO: Update API Endpoints

The following files contain placeholder API endpoints that should be updated once the
API documentation is available:

- `pydrime/api.py`: Update `upload_file()`, `create_directory()`, and `list_files()`
  endpoints
- Authentication method may need adjustment based on actual API requirements

## Troubleshooting

### "API key not configured" Error

Make sure you've set the `DRIME_API_KEY` environment variable or created a `.env` file
with your API key.

### Permission Denied Errors

If you encounter permission errors when scanning directories, the tool will skip those
files and continue with accessible ones.

### API Connection Issues

Verify that:

1. Your API key is valid
2. You have internet connectivity
3. The Drime Cloud API is accessible (not behind a firewall)

## License

MIT License - see LICENSE file for details
