# Changelog

All notable changes to this project will be documented in this file.

## 0.6.1

### ⛰️ Features

- (No Category) Do not automatically let folder created, cache for sufficient folder
  handling, threadsafe chaching

### 🐛 Bug Fixes

- (No Category) Add wsgidav to test req
- (No Category) Typo
- (No Category) Mypy errors fixed
- (No Category) Test for windows

### 🧪 Testing

- (No Category) Add unit tests for webdav and rest
- (No Category) Add missing req

### ⚙️ Miscellaneous Tasks

- (No Category) Fix pre-commit

## 0.6.0

### ⛰️ Features

- (rest) Adding REST api for restic
- (webdav) Pass webdav litmus test
- (No Category) Pydrime webdav starts a webdav server

### 📚 Documentation

- (No Category) Update doc (webdav and rest)

## 0.5.15

### 🐛 Bug Fixes

- (No Category) Remove dead code and fix typing for 3.9
- (No Category) Remove unused code
- (No Category) Python 3.9 support
- (No Category) Fix validation for python 3.9

### ⚙️ Miscellaneous Tasks

- (No Category) Fix pre-commit
- (No Category) Fix pre-commit

## 0.5.14

### ⛰️ Features

- (benchmark) New upload benchmark file for testing upload and replacing
- (upload) Use simpler upload endpoint as it is more reliable, do not delete before
  upload the same file again

### 🐛 Bug Fixes

- (api) Full relative path from file name

### 🚜 Refactor

- (validation) Reduce function complexity
- (No Category) Validation moved into own file

## 0.5.13

### <!-- 0 -->⛰️ Features

- (api) Add notes related api calls and add workspace_id to notification api
- (cli) Duplicate finder finds renamed files
- (cli) New cli commands: recent, trash and starred

## 0.5.12

⛰️ Features

- (cli) Increase default start-delay to 3 seconds
- (cli) Wet default workers to 1 and start-delay to 0

## 0.5.10

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Error logging and handling of remote "/" root folder sync

## 0.5.9 - 2025-11-29

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ "/" is correctly used as root now when using a config

## 0.5.8 - 2025-11-29

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Parent folder listing in file entries manager

## 0.5.7 - 2025-11-29

### <!-- 0 -->⛰️ Features

- _(No Category)_ More verbose logging

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Unicde signs removed for windows

## 0.5.6 - 2025-11-29

### <!-- 0 -->⛰️ Features

- _(No Category)_ Handle SSL errors

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Fix symbols for windows
- _(No Category)_ Missing parent_id added
- _(No Category)_ Unit test and add new benchmark

### <!-- 3 -->📚 Documentation

- _(No Category)_ Update changelog

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.5.5 - 2025-11-29

### <!-- 0 -->⛰️ Features

- _(progress)_ Show per folder statistics
- _(upload)_ Use presign upload method and verify uploads
- _(No Category)_ Display sync summary helper function

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Fix outpout and unit tests
- _(No Category)_ Verify upload
- _(No Category)_ Sync progress tracker for benchmark check

## 0.5.4 - 2025-11-28

### <!-- 0 -->⛰️ Features

- _(No Category)_ Better output for reflect more what is happening
- _(No Category)_ Process bar

## 0.5.3 - 2025-11-28

### <!-- 0 -->⛰️ Features

- _(No Category)_ Improve speed for dry-run and fix ignore list

## 0.5.2 - 2025-11-28

### <!-- 0 -->⛰️ Features

- _(No Category)_ Do not scan all local files upfront when not needed

## 0.5.1 - 2025-11-28

### <!-- 0 -->⛰️ Features

- _(No Category)_ Allow to set workspace as string in config, use default workspace when
  not set
- _(No Category)_ Use more efficient local file scan

## 0.5.0 - 2025-11-28

### <!-- 0 -->⛰️ Features

- _(benchmark)_ Run all benchmarks with one script
- _(cli)_ Sync can now process a json with sync instructions, pydrignore files can be
  used to define exluded files
- _(cli)_ New commands: cat head and tail for printing file content
- _(cli)_ Improved workspace and ls display, cd .. is possible
- _(sync)_ Add local trash
- _(vault)_ Allow folder upload and download
- _(No Category)_ Better rename/move detection
- _(No Category)_ Improved concurrency with semaphore

### <!-- 1 -->🐛 Bug Fixes

- _(benchmark)_ Fix run_benchmark script
- _(No Category)_ Small fixes
- _(No Category)_ Pre-commit fix and better output in run_benchmarks
- _(No Category)_ Glob_match on windows

### <!-- 6 -->🧪 Testing

- _(No Category)_ Improve coverage
- _(No Category)_ Fix unit test
- _(No Category)_ Fix unit test

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Switch to AGPL

## 0.4.1 - 2025-11-27

### <!-- 0 -->⛰️ Features

- _(sync)_ State added, several other fixes, fix cli du command

### <!-- 1 -->🐛 Bug Fixes

- _(benchmarks)_ Fix wrong parameter in benchmark
- _(benchmarks)_ Pre creating folder to fix upload bug

## 0.4.0 - 2025-11-26

### <!-- 0 -->⛰️ Features

- _(cli)_ Enhanced sync commands
- _(cli)_ Improve stat command
- _(cli)_ Add more vault commands (upload, download, rm), encryption/decryption of vault
  files
- _(sync)_ Introduce batch_size and parallel workers for speed up
- _(No Category)_ Add several missing API methods, add command for vaults
- _(No Category)_ Switch to httpx for better performance, switch cli download to the
  sync framework
- _(No Category)_ Add simple S3 upload, add start-delay option
- _(No Category)_ New benchmarks and several fixes

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ File entries manager fix for root folder
- _(No Category)_ Some fixes on engine and api
- _(No Category)_ Windows fixes
- _(No Category)_ Cloud to local fixed
- _(No Category)_ Two way sync

### <!-- 2 -->🚜 Refactor

- _(No Category)_ Move sync logic into own sub folder
- _(No Category)_ Too complex engine.py was refactored
- _(No Category)_ Use sync engine in upload

### <!-- 3 -->📚 Documentation

- _(No Category)_ Update docs
- _(No Category)_ Update readme

### <!-- 6 -->🧪 Testing

- _(api)_ Improved unit tests for mime type check
- _(api)_ Reduce max_retries in test to 0, in order to speed them up
- _(No Category)_ Fix unit test

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.3.3 - 2025-11-24

### <!-- 1 -->🐛 Bug Fixes

- _(mimetype)_ Mimetype detection fixed for small files

## 0.3.2 - 2025-11-23

### <!-- 0 -->⛰️ Features

- _(api)_ Intoduce api retry on failure

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.3.1 - 2025-11-23

### <!-- 0 -->⛰️ Features

- _(cli)_ Show time and improve sync command

### <!-- 1 -->🐛 Bug Fixes

- _(sync)_ Fix cli sync for remove files

## 0.3.0 - 2025-11-22

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Update changelog

## 0.2.6 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(duplicate_handler)_ User cache to improve performance

## 0.2.5 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(cli)_ The ls command has page and page_size now
- _(dulicate_handler)_ Opimization for reducing the amount of api calls

### <!-- 1 -->🐛 Bug Fixes

- _(duplicate_handler)_ Improved search

## 0.2.4 - 2025-11-22

### <!-- 1 -->🐛 Bug Fixes

- _(duplicate_handler)_ Add missing parent_id

## 0.2.3 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(duplicate_handler)_ Fix duplicate check for a lot of files

## 0.2.2 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(cli)_ New find duplicate command

### <!-- 1 -->🐛 Bug Fixes

- _(cli)_ Set progress bar visible on new files
- _(cli)_ Fix find-uplicates folder parameter

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.2.1 - 2025-11-22

### <!-- 2 -->🚜 Refactor

- _(No Category)_ Refactor sync by using file manager

### <!-- 6 -->🧪 Testing

- _(No Category)_ Increase coverage
- _(No Category)_ Increase coverage and fix mypy check

## 0.2.0 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(No Category)_ New sync command
- _(No Category)_ Missing page parameter has beed added to api

### <!-- 2 -->🚜 Refactor

- _(No Category)_ Get file entries call are moved into an own class
- _(No Category)_ Use FileEntriesManager in cli

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.1.9 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(duplicate_handler)_ Speed improvement

## 0.1.8 - 2025-11-22

### <!-- 1 -->🐛 Bug Fixes

- _(duplicate_handler)_ Take parent_id into account

## 0.1.7 - 2025-11-22

### <!-- 0 -->⛰️ Features

- _(No Category)_ Limit upload progress bars to jobs + 1
- _(No Category)_ Limit shown download progress bars
- _(No Category)_ Show number of uploaded/downloaded files

### <!-- 1 -->🐛 Bug Fixes

- _(duplicate_handler)_ Posix file handler for windows
- _(No Category)_ Allow to abort upload on windows, improve error handling on delete

### <!-- 3 -->📚 Documentation

- _(No Category)_ Add changelog

### <!-- 6 -->🧪 Testing

- _(No Category)_ Fix mocking after refactoring
- _(No Category)_ Fix mocking after refactoring

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Fix pre-commit

## 0.1.6 - 2025-11-21

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Fix duplicate detection and refactoring

## 0.1.5 - 2025-11-21

### <!-- 0 -->⛰️ Features

- _(No Category)_ Improve upload output
- _(No Category)_ Filter out folders from duplicates
- _(No Category)_ Improve folder handing in the download command

### <!-- 6 -->🧪 Testing

- _(No Category)_ Improve coverage

## 0.1.4 - 2025-11-21

### <!-- 0 -->⛰️ Features

- _(No Category)_ To not detect remote folder as duplicate

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Base_path added in path for upload

## 0.1.3 - 2025-11-21

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Remote path for upload is fixed

### <!-- 7 -->⚙️ Miscellaneous Tasks

- _(No Category)_ Add python 3.13

## 0.1.2 - 2025-11-21

### <!-- 0 -->⛰️ Features

- _(No Category)_ Improved dry-run output, remote-path parameter for validate and
  windows path is fixed

## 0.1.1 - 2025-11-21

### <!-- 0 -->⛰️ Features

- _(No Category)_ Print more information for upload and allow so set default workspace
  by name

### <!-- 1 -->🐛 Bug Fixes

- _(No Category)_ Current set workspace was not taken into account for pwd and cd
  commands
- _(No Category)_ Add missing parent_id

## 0.1.0 - 2025-11-21

### <!-- 0 -->⛰️ Features

- _(No Category)_ Initial release

### <!-- 3 -->📚 Documentation

- _(No Category)_ Add badges

### <!-- 6 -->🧪 Testing

- _(No Category)_ Fix missing mock
- _(No Category)_ Fix test for windows

<!-- generated by git-cliff -->
