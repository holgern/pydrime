Exception Handling
==================

PyDrime provides a comprehensive exception hierarchy for fine-grained error handling.

Exception Hierarchy
-------------------

All PyDrime exceptions inherit from ``DrimeAPIError``:

.. code-block:: text

   DrimeAPIError (base exception)
   ├── DrimeConfigError - Configuration/setup issues
   ├── DrimeAuthenticationError - Authentication failures (401)
   ├── DrimePermissionError - Permission/authorization errors (403)
   ├── DrimeNotFoundError - Resource not found (404)
   ├── DrimeRateLimitError - Rate limit exceeded (429)
   ├── DrimeNetworkError - Network-related errors
   ├── DrimeUploadError - File upload errors
   ├── DrimeDownloadError - File download errors
   ├── DrimeInvalidResponseError - Invalid/unexpected server responses
   └── DrimeFileNotFoundError - Local file not found

Exception Classes
-----------------

.. automodule:: pydrime.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Basic Error Handling
--------------------

Catch All Errors
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeAPIError

   try:
       client = DrimeClient(api_key="your_key")
       result = client.upload_file(Path("myfile.txt"))
   except DrimeAPIError as e:
       print(f"API error: {e}")

This catches all PyDrime-related errors.

Specific Error Handling
-----------------------

Authentication Errors
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeAuthenticationError

   try:
       client = DrimeClient(api_key="invalid_key")
       result = client.get_logged_user()
   except DrimeAuthenticationError:
       print("Invalid API key! Please check your credentials.")

Configuration Errors
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeConfigError

   try:
       # No API key provided
       client = DrimeClient()
   except DrimeConfigError as e:
       print(f"Configuration error: {e}")
       print("Please set DRIME_API_KEY environment variable")

Permission Errors
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimePermissionError

   try:
       client = DrimeClient(api_key="your_key")
       result = client.delete_file_entries([12345], delete_forever=True)
   except DrimePermissionError:
       print("You don't have permission to delete this file")

Not Found Errors
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeNotFoundError

   try:
       client = DrimeClient(api_key="your_key")
       result = client.download_file("nonexistent_hash")
   except DrimeNotFoundError:
       print("File not found on server")

Upload Errors
~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeUploadError, DrimeFileNotFoundError

   try:
       client = DrimeClient(api_key="your_key")
       result = client.upload_file(Path("myfile.txt"))
   except DrimeFileNotFoundError as e:
       print(f"Local file not found: {e.file_path}")
   except DrimeUploadError as e:
       print(f"Upload failed: {e}")

Download Errors
~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeDownloadError

   try:
       client = DrimeClient(api_key="your_key")
       saved_path = client.download_file("abc123hash")
   except DrimeDownloadError as e:
       print(f"Download failed: {e}")

Network Errors
~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeNetworkError

   try:
       client = DrimeClient(api_key="your_key")
       result = client.list_files()
   except DrimeNetworkError as e:
       print(f"Network error: {e}")
       print("Please check your internet connection")

Rate Limit Errors
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pydrime import DrimeClient, DrimeRateLimitError
   import time

   try:
       client = DrimeClient(api_key="your_key")
       result = client.list_files()
   except DrimeRateLimitError:
       print("Rate limit exceeded. Waiting 60 seconds...")
       time.sleep(60)
       # Retry the request
       result = client.list_files()

Multi-Level Error Handling
---------------------------

.. code-block:: python

   from pydrime import (
       DrimeClient,
       DrimeAPIError,
       DrimeAuthenticationError,
       DrimeUploadError,
       DrimeNetworkError,
   )
   from pathlib import Path

   def upload_with_retry(client, file_path, max_retries=3):
       """Upload a file with retry logic."""
       for attempt in range(max_retries):
           try:
               return client.upload_file(file_path)
           except DrimeAuthenticationError:
               # Don't retry auth errors
               print("Authentication failed - check your API key")
               raise
           except DrimeNetworkError as e:
               # Retry network errors
               if attempt < max_retries - 1:
                   print(f"Network error, retrying... (attempt {attempt + 1})")
                   time.sleep(2 ** attempt)  # Exponential backoff
               else:
                   print(f"Failed after {max_retries} attempts")
                   raise
           except DrimeUploadError as e:
               # Don't retry upload errors
               print(f"Upload error: {e}")
               raise

   # Usage
   try:
       client = DrimeClient(api_key="your_key")
       result = upload_with_retry(client, Path("myfile.txt"))
       print("Upload successful!")
   except DrimeAPIError as e:
       print(f"Failed to upload: {e}")

Best Practices
--------------

1. **Catch Specific Exceptions First**

   Always catch more specific exceptions before the base exception:

   .. code-block:: python

      try:
          result = client.upload_file(Path("file.txt"))
      except DrimeAuthenticationError:
          # Handle auth error
          pass
      except DrimeUploadError:
          # Handle upload error
          pass
      except DrimeAPIError:
          # Handle any other API error
          pass

2. **Use Context Managers for Resources**

   .. code-block:: python

      from pathlib import Path

      try:
          result = client.upload_file(Path("file.txt"))
      except DrimeAPIError as e:
          logging.error(f"Upload failed: {e}")
          raise

3. **Log Errors Appropriately**

   .. code-block:: python

      import logging

      try:
          result = client.upload_file(Path("file.txt"))
      except DrimeAuthenticationError as e:
          logging.error(f"Authentication failed: {e}")
      except DrimeNetworkError as e:
          logging.warning(f"Network issue: {e}")
      except DrimeAPIError as e:
          logging.error(f"API error: {e}")

4. **Provide User-Friendly Messages**

   .. code-block:: python

      try:
          result = client.upload_file(Path("file.txt"))
      except DrimeAuthenticationError:
          print("❌ Invalid API key. Run 'pydrime init' to configure.")
      except DrimeFileNotFoundError as e:
          print(f"❌ File not found: {e.file_path}")
      except DrimeUploadError as e:
          print(f"❌ Upload failed: {e}")
      except DrimeAPIError as e:
          print(f"❌ Error: {e}")

CLI Error Handling
------------------

The CLI automatically handles exceptions and displays user-friendly error messages:

.. code-block:: bash

   $ pydrime upload missing_file.txt
   Error: File not found: missing_file.txt

   $ pydrime --api-key invalid_key status
   Error: Invalid API key or unauthorized access

   $ pydrime upload file.txt --workspace 99999
   Error: Resource not found

Custom Exception Attributes
----------------------------

Some exceptions provide additional attributes:

DrimeFileNotFoundError
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   try:
       result = client.upload_file(Path("missing.txt"))
   except DrimeFileNotFoundError as e:
       print(f"File path: {e.file_path}")
       print(f"Error message: {str(e)}")
