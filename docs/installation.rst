Installation
============

Requirements
------------

* Python 3.9 or higher
* pip or uv package manager

From Source
-----------

Clone the repository and install:

.. code-block:: bash

   git clone https://github.com/holgern/pydrime.git
   cd pydrime

   # Using pip
   pip install -e .

   # Or using uv (recommended)
   uv pip install -e .

With Development Dependencies
-----------------------------

To install with development dependencies (for testing and linting):

.. code-block:: bash

   # Using pip
   pip install -e ".[dev]"

   # Or using uv
   uv pip install -e ".[dev]"

Verify Installation
-------------------

Check that pydrime is installed correctly:

.. code-block:: bash

   pydrime --version
   pydrime --help

Configuration
-------------

Before using pydrime, you need to configure your Drime Cloud API key. See :doc:`quickstart` for configuration instructions.
