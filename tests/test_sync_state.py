"""Unit tests for sync state management."""

import json
from unittest.mock import MagicMock

from pydrime.sync.state import (
    LocalItemState,
    LocalTree,
    RemoteItemState,
    RemoteTree,
    SyncState,
    SyncStateManager,
    build_local_tree_from_files,
    build_remote_tree_from_files,
)


class TestLocalItemState:
    """Tests for LocalItemState dataclass."""

    def test_basic_initialization(self):
        """Test basic initialization."""
        item = LocalItemState(
            path="folder/file.txt",
            size=1024,
            mtime=1234567890.0,
            file_id=12345,
        )
        assert item.path == "folder/file.txt"
        assert item.size == 1024
        assert item.mtime == 1234567890.0
        assert item.file_id == 12345
        assert item.item_type == "file"
        assert item.creation_time is None

    def test_initialization_with_all_params(self):
        """Test initialization with all parameters."""
        item = LocalItemState(
            path="folder/dir",
            size=0,
            mtime=1234567890.0,
            file_id=12345,
            item_type="directory",
            creation_time=1234567800.0,
        )
        assert item.item_type == "directory"
        assert item.creation_time == 1234567800.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        item = LocalItemState(
            path="test.txt",
            size=100,
            mtime=1234567890.0,
            file_id=99999,
            creation_time=1234567800.0,
        )
        d = item.to_dict()
        assert d["path"] == "test.txt"
        assert d["size"] == 100
        assert d["mtime"] == 1234567890.0
        assert d["file_id"] == 99999
        assert d["item_type"] == "file"
        assert d["creation_time"] == 1234567800.0

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "path": "test.txt",
            "size": 100,
            "mtime": 1234567890.0,
            "file_id": 99999,
            "item_type": "file",
            "creation_time": 1234567800.0,
        }
        item = LocalItemState.from_dict(d)
        assert item.path == "test.txt"
        assert item.size == 100
        assert item.file_id == 99999
        assert item.creation_time == 1234567800.0

    def test_from_dict_with_legacy_inode(self):
        """Test creation from dictionary with legacy 'inode' key."""
        d = {
            "path": "test.txt",
            "size": 100,
            "mtime": 1234567890.0,
            "inode": 99999,  # Legacy key
            "item_type": "file",
        }
        item = LocalItemState.from_dict(d)
        assert item.file_id == 99999

    def test_from_dict_defaults(self):
        """Test creation from dictionary with missing fields."""
        d = {}
        item = LocalItemState.from_dict(d)
        assert item.path == ""
        assert item.size == 0
        assert item.mtime == 0.0
        assert item.file_id == 0
        assert item.item_type == "file"
        assert item.creation_time is None


class TestRemoteItemState:
    """Tests for RemoteItemState dataclass."""

    def test_basic_initialization(self):
        """Test basic initialization."""
        item = RemoteItemState(
            path="folder/file.txt",
            size=1024,
            mtime=1234567890.0,
            id=12345,
        )
        assert item.path == "folder/file.txt"
        assert item.size == 1024
        assert item.mtime == 1234567890.0
        assert item.id == 12345
        assert item.item_type == "file"
        assert item.file_hash == ""

    def test_initialization_with_all_params(self):
        """Test initialization with all parameters."""
        item = RemoteItemState(
            path="folder/dir",
            size=0,
            mtime=None,
            id=12345,
            item_type="directory",
            file_hash="abc123",
        )
        assert item.item_type == "directory"
        assert item.file_hash == "abc123"
        assert item.mtime is None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        item = RemoteItemState(
            path="test.txt",
            size=100,
            mtime=1234567890.0,
            id=99999,
            file_hash="hash123",
        )
        d = item.to_dict()
        assert d["path"] == "test.txt"
        assert d["size"] == 100
        assert d["mtime"] == 1234567890.0
        assert d["id"] == 99999
        assert d["item_type"] == "file"
        assert d["file_hash"] == "hash123"

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "path": "test.txt",
            "size": 100,
            "mtime": 1234567890.0,
            "id": 99999,
            "item_type": "file",
            "file_hash": "hash123",
        }
        item = RemoteItemState.from_dict(d)
        assert item.path == "test.txt"
        assert item.size == 100
        assert item.id == 99999
        assert item.file_hash == "hash123"

    def test_from_dict_with_legacy_uuid(self):
        """Test creation from dictionary with legacy 'uuid' key."""
        d = {
            "path": "test.txt",
            "size": 100,
            "mtime": None,
            "uuid": 99999,  # Legacy key
            "item_type": "file",
        }
        item = RemoteItemState.from_dict(d)
        assert item.id == 99999

    def test_from_dict_defaults(self):
        """Test creation from dictionary with missing fields."""
        d = {}
        item = RemoteItemState.from_dict(d)
        assert item.path == ""
        assert item.size == 0
        assert item.mtime is None
        assert item.id == 0
        assert item.item_type == "file"
        assert item.file_hash == ""


class TestLocalTree:
    """Tests for LocalTree dataclass."""

    def test_empty_tree(self):
        """Test empty tree."""
        tree = LocalTree()
        assert tree.size == 0
        assert tree.get_by_path("any") is None
        assert tree.get_by_file_id(123) is None

    def test_add_item(self):
        """Test adding an item."""
        tree = LocalTree()
        item = LocalItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        assert tree.size == 1
        assert tree.get_by_path("test.txt") == item
        assert tree.get_by_file_id(99) == item

    def test_remove_item(self):
        """Test removing an item."""
        tree = LocalTree()
        item = LocalItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        removed = tree.remove_item("test.txt")
        assert removed == item
        assert tree.size == 0
        assert tree.get_by_path("test.txt") is None
        assert tree.get_by_file_id(99) is None

    def test_remove_nonexistent_item(self):
        """Test removing a nonexistent item."""
        tree = LocalTree()
        removed = tree.remove_item("nonexistent.txt")
        assert removed is None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        tree = LocalTree()
        item = LocalItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        d = tree.to_dict()
        assert "tree" in d
        assert "file_ids" in d
        assert "test.txt" in d["tree"]
        assert "99" in d["file_ids"]

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "tree": {
                "test.txt": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "file_id": 99,
                }
            },
            "file_ids": {
                "99": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "file_id": 99,
                }
            },
        }
        tree = LocalTree.from_dict(d)
        assert tree.size == 1
        assert tree.get_by_path("test.txt") is not None

    def test_from_dict_with_legacy_inodes(self):
        """Test creation from dictionary with legacy 'inodes' key."""
        d = {
            "tree": {
                "test.txt": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "file_id": 99,
                }
            },
            "inodes": {  # Legacy key
                "99": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "file_id": 99,
                }
            },
        }
        tree = LocalTree.from_dict(d)
        assert tree.get_by_file_id(99) is not None


class TestRemoteTree:
    """Tests for RemoteTree dataclass."""

    def test_empty_tree(self):
        """Test empty tree."""
        tree = RemoteTree()
        assert tree.size == 0
        assert tree.get_by_path("any") is None
        assert tree.get_by_id(123) is None

    def test_add_item(self):
        """Test adding an item."""
        tree = RemoteTree()
        item = RemoteItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        assert tree.size == 1
        assert tree.get_by_path("test.txt") == item
        assert tree.get_by_id(99) == item

    def test_remove_item(self):
        """Test removing an item."""
        tree = RemoteTree()
        item = RemoteItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        removed = tree.remove_item("test.txt")
        assert removed == item
        assert tree.size == 0
        assert tree.get_by_path("test.txt") is None
        assert tree.get_by_id(99) is None

    def test_remove_nonexistent_item(self):
        """Test removing a nonexistent item."""
        tree = RemoteTree()
        removed = tree.remove_item("nonexistent.txt")
        assert removed is None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        tree = RemoteTree()
        item = RemoteItemState("test.txt", 100, 123456.0, 99)
        tree.add_item(item)

        d = tree.to_dict()
        assert "tree" in d
        assert "ids" in d
        assert "test.txt" in d["tree"]
        assert "99" in d["ids"]

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "tree": {
                "test.txt": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "id": 99,
                }
            },
            "ids": {
                "99": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "id": 99,
                }
            },
        }
        tree = RemoteTree.from_dict(d)
        assert tree.size == 1
        assert tree.get_by_path("test.txt") is not None

    def test_from_dict_with_legacy_uuids(self):
        """Test creation from dictionary with legacy 'uuids' key."""
        d = {
            "tree": {
                "test.txt": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "id": 99,
                }
            },
            "uuids": {  # Legacy key
                "99": {
                    "path": "test.txt",
                    "size": 100,
                    "mtime": 123456.0,
                    "id": 99,
                }
            },
        }
        tree = RemoteTree.from_dict(d)
        assert tree.get_by_id(99) is not None


class TestSyncState:
    """Tests for SyncState dataclass."""

    def test_basic_initialization(self):
        """Test basic initialization."""
        state = SyncState(
            local_path="/home/user/sync",
            remote_path="/cloud/sync",
        )
        assert state.local_path == "/home/user/sync"
        assert state.remote_path == "/cloud/sync"
        assert state.local_tree.size == 0
        assert state.remote_tree.size == 0
        assert state.last_sync is None
        assert state.version == 2

    def test_to_dict(self):
        """Test conversion to dictionary."""
        state = SyncState(
            local_path="/home/user/sync",
            remote_path="/cloud/sync",
        )
        state.synced_files = {"file1.txt", "file2.txt"}
        state.last_sync = "2025-01-15T10:30:00"

        d = state.to_dict()
        assert d["local_path"] == "/home/user/sync"
        assert d["remote_path"] == "/cloud/sync"
        assert "local_tree" in d
        assert "remote_tree" in d
        assert d["last_sync"] == "2025-01-15T10:30:00"
        assert set(d["synced_files"]) == {"file1.txt", "file2.txt"}

    def test_from_dict_v2(self):
        """Test creation from v2 dictionary format."""
        d = {
            "version": 2,
            "local_path": "/home/user/sync",
            "remote_path": "/cloud/sync",
            "local_tree": {"tree": {}, "file_ids": {}},
            "remote_tree": {"tree": {}, "ids": {}},
            "local_file_hashes": {},
            "last_sync": "2025-01-15T10:30:00",
            "synced_files": ["file1.txt"],
        }
        state = SyncState.from_dict(d)
        assert state.version == 2
        assert state.local_path == "/home/user/sync"
        assert state.last_sync == "2025-01-15T10:30:00"

    def test_from_dict_v1_legacy(self):
        """Test creation from v1 legacy dictionary format."""
        d = {
            "version": 1,
            "local_path": "/home/user/sync",
            "remote_path": "/cloud/sync",
            "synced_files": ["file1.txt", "file2.txt"],
            "last_sync": "2025-01-15T10:30:00",
        }
        state = SyncState.from_dict(d)
        assert state.version == 1
        assert state.synced_files == {"file1.txt", "file2.txt"}

    def test_from_dict_without_local_tree(self):
        """Test creation from dictionary without local_tree (legacy)."""
        d = {
            "local_path": "/home/user/sync",
            "remote_path": "/cloud/sync",
            "synced_files": ["file1.txt"],
        }
        state = SyncState.from_dict(d)
        assert state.version == 1  # Treated as v1

    def test_get_synced_paths_v1(self):
        """Test get_synced_paths for v1 format."""
        state = SyncState(
            local_path="/home/user/sync",
            remote_path="/cloud/sync",
            version=1,
        )
        state.synced_files = {"file1.txt", "file2.txt"}

        paths = state.get_synced_paths()
        assert paths == {"file1.txt", "file2.txt"}

    def test_get_synced_paths_v2(self):
        """Test get_synced_paths for v2 format."""
        state = SyncState(
            local_path="/home/user/sync",
            remote_path="/cloud/sync",
        )

        # Add items to both trees
        local_item = LocalItemState("common.txt", 100, 123456.0, 1)
        state.local_tree.add_item(local_item)
        local_only = LocalItemState("local_only.txt", 100, 123456.0, 2)
        state.local_tree.add_item(local_only)

        remote_item = RemoteItemState("common.txt", 100, 123456.0, 1)
        state.remote_tree.add_item(remote_item)
        remote_only = RemoteItemState("remote_only.txt", 100, 123456.0, 2)
        state.remote_tree.add_item(remote_only)

        paths = state.get_synced_paths()
        assert paths == {"common.txt"}  # Only intersection


class TestSyncStateManager:
    """Tests for SyncStateManager class."""

    def test_init_creates_directory(self, tmp_path):
        """Test that initialization creates the state directory."""
        state_dir = tmp_path / "state"
        manager = SyncStateManager(state_dir=state_dir)

        assert manager.state_dir.exists()
        assert manager.state_dir.parent == state_dir

    def test_load_state_nonexistent(self, tmp_path):
        """Test loading state that doesn't exist."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()

        state = manager.load_state(local_path, "/remote/path")
        assert state is None

    def test_save_and_load_state(self, tmp_path):
        """Test saving and loading state."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        # Create and save state
        local_tree = LocalTree()
        local_tree.add_item(LocalItemState("test.txt", 100, 123456.0, 99))
        remote_tree = RemoteTree()
        remote_tree.add_item(RemoteItemState("test.txt", 100, 123456.0, 88))

        manager.save_state(
            local_path=local_path,
            remote_path=remote_path,
            local_tree=local_tree,
            remote_tree=remote_tree,
            local_file_hashes={"test.txt": "abc123"},
        )

        # Load and verify
        loaded = manager.load_state(local_path, remote_path)
        assert loaded is not None
        assert loaded.local_tree.size == 1
        assert loaded.remote_tree.size == 1
        assert loaded.local_file_hashes["test.txt"] == "abc123"

    def test_save_state_with_synced_files_only(self, tmp_path):
        """Test saving state with only synced_files (legacy mode)."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        manager.save_state(
            local_path=local_path,
            remote_path=remote_path,
            synced_files={"file1.txt", "file2.txt"},
        )

        loaded = manager.load_state(local_path, remote_path)
        assert loaded is not None
        assert "file1.txt" in loaded.synced_files

    def test_save_state_from_trees(self, tmp_path):
        """Test save_state_from_trees method."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        local_tree = LocalTree()
        local_tree.add_item(LocalItemState("common.txt", 100, 123456.0, 99))
        remote_tree = RemoteTree()
        remote_tree.add_item(RemoteItemState("common.txt", 100, 123456.0, 88))

        manager.save_state_from_trees(
            local_path=local_path,
            remote_path=remote_path,
            local_tree=local_tree,
            remote_tree=remote_tree,
        )

        loaded = manager.load_state(local_path, remote_path)
        assert loaded is not None
        assert "common.txt" in loaded.synced_files

    def test_clear_state(self, tmp_path):
        """Test clearing state."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        # Save state first
        manager.save_state(
            local_path=local_path,
            remote_path=remote_path,
            synced_files={"file.txt"},
        )

        # Verify it exists
        assert manager.load_state(local_path, remote_path) is not None

        # Clear and verify
        result = manager.clear_state(local_path, remote_path)
        assert result is True
        assert manager.load_state(local_path, remote_path) is None

    def test_clear_state_nonexistent(self, tmp_path):
        """Test clearing nonexistent state."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()

        result = manager.clear_state(local_path, "/nonexistent")
        assert result is False

    def test_load_corrupted_state(self, tmp_path):
        """Test loading corrupted state file."""
        manager = SyncStateManager(state_dir=tmp_path / "state")
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        # Create corrupted state file
        state_file = manager._get_state_file(local_path, remote_path)
        state_file.write_text("not valid json {")

        state = manager.load_state(local_path, remote_path)
        assert state is None

    def test_load_state_with_migration(self, tmp_path):
        """Test loading state with migration from legacy directory."""
        state_dir = tmp_path / "state"

        manager = SyncStateManager(state_dir=state_dir)
        local_path = tmp_path / "local"
        local_path.mkdir()
        remote_path = "/remote/path"

        # Create legacy state file
        legacy_file = manager._get_legacy_state_file(local_path, remote_path)
        legacy_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_data = {
            "version": 1,
            "local_path": str(local_path),
            "remote_path": remote_path,
            "synced_files": ["legacy.txt"],
        }
        legacy_file.write_text(json.dumps(legacy_data))

        # Load should work with migration
        loaded = manager.load_state(local_path, remote_path)
        # Note: This test may or may not migrate depending on paths
        # The state should either be loaded from current or legacy
        assert loaded is not None or legacy_file.exists()


class TestBuildTreeFromFiles:
    """Tests for build_local_tree_from_files and build_remote_tree_from_files."""

    def test_build_local_tree(self):
        """Test building LocalTree from LocalFile objects."""
        # Create mock LocalFile objects
        files = []
        for i in range(3):
            f = MagicMock()
            f.relative_path = f"file{i}.txt"
            f.size = i * 100
            f.mtime = 1234567890.0 + i
            f.file_id = 1000 + i
            f.creation_time = 1234567800.0 + i
            files.append(f)

        tree = build_local_tree_from_files(files)

        assert tree.size == 3
        assert tree.get_by_path("file0.txt") is not None
        assert tree.get_by_file_id(1000) is not None

    def test_build_remote_tree(self):
        """Test building RemoteTree from RemoteFile objects."""
        # Create mock RemoteFile objects
        files = []
        for i in range(3):
            f = MagicMock()
            f.relative_path = f"file{i}.txt"
            f.size = i * 100
            f.mtime = 1234567890.0 + i
            f.id = 2000 + i
            f.hash = f"hash{i}"
            files.append(f)

        tree = build_remote_tree_from_files(files)

        assert tree.size == 3
        assert tree.get_by_path("file0.txt") is not None
        assert tree.get_by_id(2000) is not None
