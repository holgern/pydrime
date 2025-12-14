PyDrime Documentation
=====================

PyDrime is a Python command-line tool and API client for uploading, downloading, and managing files on Drime Cloud.

.. warning::
   **Disclaimer**

   PyDrime is an **unofficial, community-developed library** and is **not affiliated with, endorsed by, or supported by Drime or the Drime Cloud service**.

   This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages or other liability, whether in an action of contract, tort or otherwise, arising from, out of or in connection with the software or the use or other dealings in the software.

   **Use at your own risk.** The authors are not responsible for any data loss, corruption, or other issues that may arise from using this tool. Always maintain backups of your important data.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   cli
   api
   exceptions

Features
--------

* Upload individual files or entire directories
* Download files by hash or ID
* Create and manage folders with navigation (cd/pwd)
* Rename, delete, and share files by name or ID
* List and search files with flexible filtering
* Multipart upload for large files (>30MB)
* Rich terminal output with progress tracking
* JSON output for programmatic processing
* Context-aware file operations (current directory support)
* **Sync** - Bidirectional and one-way sync between local and cloud
* **Encrypted Vault** - Client-side encryption for sensitive files
* **Duplicate Finder** - Find and remove duplicate files
* **Storage Usage** - Monitor your cloud storage usage

Server Features
~~~~~~~~~~~~~~~

WebDAV and REST server functionality has been moved to separate packages:

* **pywebdavserver** - Mount Drime Cloud as a network drive via WebDAV
* **pyrestserver** - Use Drime Cloud as a restic backup destination

These can be installed separately if needed. See their respective documentation for more information.

Quick Example
-------------

Upload a file:

.. code-block:: bash

   pydrime upload myfile.txt

Upload a directory:

.. code-block:: bash

   pydrime upload /path/to/directory

Download a file:

.. code-block:: bash

   pydrime download abc123hash

Sync a directory:

.. code-block:: bash

   pydrime sync ./my_folder

Using the Python API:

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
       print(f"{file['name']} ({file['type']})")

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
