# Changelog

All notable changes to this project will be documented in this file.

## 0.5.5 - 2025-11-29

### <!-- 0 -->⛰️  Features

- *(progress)* Show per folder statistics
- *(upload)* Use presign upload method and verify uploads
- *(No Category)* Display sync summary helper function


### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* Fix outpout and unit tests
- *(No Category)* Verify upload
- *(No Category)* Sync progress tracker for benchmark check


## 0.5.4 - 2025-11-28

### <!-- 0 -->⛰️  Features

- *(No Category)* Better output for reflect more what is happening
- *(No Category)* Process bar


## 0.5.3 - 2025-11-28

### <!-- 0 -->⛰️  Features

- *(No Category)* Improve speed for dry-run and fix ignore list


## 0.5.2 - 2025-11-28

### <!-- 0 -->⛰️  Features

- *(No Category)* Do not scan all local files upfront when not needed


## 0.5.1 - 2025-11-28

### <!-- 0 -->⛰️  Features

- *(No Category)* Allow to set workspace as string in config, use default workspace when not set
- *(No Category)* Use more efficient local file scan


## 0.5.0 - 2025-11-28

### <!-- 0 -->⛰️  Features

- *(benchmark)* Run all benchmarks with one script
- *(cli)* Sync can now process a json with sync instructions, pydrignore files can be used to define exluded files
- *(cli)* New commands: cat head and tail for printing file content
- *(cli)* Improved workspace and ls display, cd .. is possible
- *(sync)* Add local trash
- *(vault)* Allow folder upload and download
- *(No Category)* Better rename/move detection
- *(No Category)* Improved concurrency with semaphore


### <!-- 1 -->🐛 Bug Fixes

- *(benchmark)* Fix run_benchmark script
- *(No Category)* Small fixes
- *(No Category)* Pre-commit fix and better output in run_benchmarks
- *(No Category)* Glob_match on windows


### <!-- 6 -->🧪 Testing

- *(No Category)* Improve coverage
- *(No Category)* Fix unit test
- *(No Category)* Fix unit test


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Switch to AGPL


## 0.4.1 - 2025-11-27

### <!-- 0 -->⛰️  Features

- *(sync)* State added, several other fixes, fix cli du command


### <!-- 1 -->🐛 Bug Fixes

- *(benchmarks)* Fix wrong parameter in benchmark
- *(benchmarks)* Pre creating folder to fix upload bug


## 0.4.0 - 2025-11-26

### <!-- 0 -->⛰️  Features

- *(cli)* Enhanced sync commands
- *(cli)* Improve stat command
- *(cli)* Add more vault commands (upload, download, rm), encryption/decryption of vault files
- *(sync)* Introduce batch_size and parallel workers for speed up
- *(No Category)* Add several missing API methods, add command for vaults
- *(No Category)* Switch to httpx for better performance, switch cli download to the sync framework
- *(No Category)* Add simple S3 upload, add start-delay option
- *(No Category)* New benchmarks and several fixes


### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* File entries manager fix for root folder
- *(No Category)* Some fixes on engine and api
- *(No Category)* Windows fixes
- *(No Category)* Cloud to local fixed
- *(No Category)* Two way sync


### <!-- 2 -->🚜 Refactor

- *(No Category)* Move sync logic into own sub folder
- *(No Category)* Too complex engine.py was refactored
- *(No Category)* Use sync engine in upload


### <!-- 3 -->📚 Documentation

- *(No Category)* Update docs
- *(No Category)* Update readme


### <!-- 6 -->🧪 Testing

- *(api)* Improved unit tests for mime type check
- *(api)* Reduce max_retries in test to 0, in order to speed them up
- *(No Category)* Fix unit test


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Fix pre-commit


## 0.3.3 - 2025-11-24

### <!-- 1 -->🐛 Bug Fixes

- *(mimetype)* Mimetype detection fixed for small files


## 0.3.2 - 2025-11-23

### <!-- 0 -->⛰️  Features

- *(api)* Intoduce api retry on failure


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Fix pre-commit


## 0.3.1 - 2025-11-23

### <!-- 0 -->⛰️  Features

- *(cli)* Show time and improve sync command


### <!-- 1 -->🐛 Bug Fixes

- *(sync)* Fix cli sync for remove files


## 0.3.0 - 2025-11-22

### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Update changelog


## 0.2.6 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(duplicate_handler)* User cache to improve performance


## 0.2.5 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(cli)* The ls command has page and page_size now
- *(dulicate_handler)* Opimization for reducing the amount of api calls


### <!-- 1 -->🐛 Bug Fixes

- *(duplicate_handler)* Improved search


## 0.2.4 - 2025-11-22

### <!-- 1 -->🐛 Bug Fixes

- *(duplicate_handler)* Add missing parent_id


## 0.2.3 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(duplicate_handler)* Fix duplicate check for a lot of files


## 0.2.2 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(cli)* New find duplicate command


### <!-- 1 -->🐛 Bug Fixes

- *(cli)* Set progress bar visible on new files
- *(cli)* Fix find-uplicates folder parameter


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Fix pre-commit


## 0.2.1 - 2025-11-22

### <!-- 2 -->🚜 Refactor

- *(No Category)* Refactor sync by using file manager


### <!-- 6 -->🧪 Testing

- *(No Category)* Increase coverage
- *(No Category)* Increase coverage and fix mypy check


## 0.2.0 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(No Category)* New sync command
- *(No Category)* Missing page parameter has beed added to api


### <!-- 2 -->🚜 Refactor

- *(No Category)* Get file entries call are moved into an own class
- *(No Category)* Use FileEntriesManager in cli


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Fix pre-commit


## 0.1.9 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(duplicate_handler)* Speed improvement


## 0.1.8 - 2025-11-22

### <!-- 1 -->🐛 Bug Fixes

- *(duplicate_handler)* Take parent_id into account


## 0.1.7 - 2025-11-22

### <!-- 0 -->⛰️  Features

- *(No Category)* Limit upload progress bars to jobs + 1
- *(No Category)* Limit shown download progress bars
- *(No Category)* Show number of uploaded/downloaded files


### <!-- 1 -->🐛 Bug Fixes

- *(duplicate_handler)* Posix file handler for windows
- *(No Category)* Allow to abort upload on windows, improve error handling on delete


### <!-- 3 -->📚 Documentation

- *(No Category)* Add changelog


### <!-- 6 -->🧪 Testing

- *(No Category)* Fix mocking after refactoring
- *(No Category)* Fix mocking after refactoring


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Fix pre-commit


## 0.1.6 - 2025-11-21

### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* Fix duplicate detection and refactoring


## 0.1.5 - 2025-11-21

### <!-- 0 -->⛰️  Features

- *(No Category)* Improve upload output
- *(No Category)* Filter out folders from duplicates
- *(No Category)* Improve folder handing in the download command


### <!-- 6 -->🧪 Testing

- *(No Category)* Improve coverage


## 0.1.4 - 2025-11-21

### <!-- 0 -->⛰️  Features

- *(No Category)* To not detect remote folder as duplicate


### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* Base_path added in path for upload


## 0.1.3 - 2025-11-21

### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* Remote path for upload is fixed


### <!-- 7 -->⚙️ Miscellaneous Tasks

- *(No Category)* Add python 3.13


## 0.1.2 - 2025-11-21

### <!-- 0 -->⛰️  Features

- *(No Category)* Improved dry-run output, remote-path parameter for validate and windows path is fixed


## 0.1.1 - 2025-11-21

### <!-- 0 -->⛰️  Features

- *(No Category)* Print more information for upload and allow so set default workspace by name


### <!-- 1 -->🐛 Bug Fixes

- *(No Category)* Current set workspace was not taken into account for pwd and cd commands
- *(No Category)* Add missing parent_id


## 0.1.0 - 2025-11-21

### <!-- 0 -->⛰️  Features

- *(No Category)* Initial release


### <!-- 3 -->📚 Documentation

- *(No Category)* Add badges


### <!-- 6 -->🧪 Testing

- *(No Category)* Fix missing mock
- *(No Category)* Fix test for windows


<!-- generated by git-cliff -->
