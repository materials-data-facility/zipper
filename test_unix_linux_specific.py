#!/usr/bin/env python3
"""
UNIX/Linux Specific Tests for MDF Zipper - High-Value Dataset Protection

This test suite focuses on UNIX/Linux specific filesystem features, signal handling,
special file types, and platform-specific edge cases to ensure comprehensive
protection for high-value datasets on UNIX/Linux systems.
"""

import pytest
import os
import sys
import pwd
import grp
import stat
import signal
import time
import subprocess
import threading
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

from mdf_zipper import MDFZipper
from conftest import calculate_file_checksum


# Skip all tests if not on UNIX/Linux
pytestmark = pytest.mark.skipif(
    sys.platform == "win32", 
    reason="UNIX/Linux specific tests not applicable on Windows"
)


class TestUnixFilePermissions:
    """Tests for UNIX file permission scenarios that could affect high-value datasets."""
    
    def test_setuid_setgid_files(self, temp_test_dir):
        """Test handling of setuid/setgid files without compromising security."""
        test_dir = temp_test_dir / "setuid_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create regular file first
        setuid_file = dataset_dir / "setuid_program"
        setuid_file.write_text("#!/bin/bash\necho 'test program'\n")
        
        # Attempt to set setuid bit (may fail without root, which is expected)
        try:
            os.chmod(setuid_file, 0o4755)  # setuid + executable
            has_setuid = (setuid_file.stat().st_mode & stat.S_ISUID) != 0
        except (OSError, PermissionError):
            has_setuid = False
        
        original_checksum = calculate_file_checksum(setuid_file)
        original_mode = setuid_file.stat().st_mode
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify file integrity
        assert setuid_file.exists(), "Setuid file was moved or deleted"
        current_checksum = calculate_file_checksum(setuid_file)
        assert current_checksum == original_checksum, "Setuid file content was modified"
        
        # Verify permissions weren't altered
        current_mode = setuid_file.stat().st_mode
        assert current_mode == original_mode, "File permissions were altered during compression"
    
    def test_files_with_no_permissions(self, temp_test_dir):
        """Test handling of files with no permissions (000)."""
        test_dir = temp_test_dir / "no_perms_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create file with content
        no_perm_file = dataset_dir / "no_permissions.txt"
        no_perm_file.write_text("Content that should remain unchanged\n" * 100)
        
        original_checksum = calculate_file_checksum(no_perm_file)
        
        # Remove all permissions
        os.chmod(no_perm_file, 0o000)
        
        try:
            # Process dataset
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            # Should handle gracefully (may skip file or include it)
            assert 'processed' in results
            
        finally:
            # Restore permissions for cleanup
            os.chmod(no_perm_file, 0o644)
        
        # Verify original file integrity
        current_checksum = calculate_file_checksum(no_perm_file)
        assert current_checksum == original_checksum, "File content was modified"
    
    def test_sticky_bit_directories(self, temp_test_dir):
        """Test handling of directories with sticky bit set."""
        test_dir = temp_test_dir / "sticky_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create subdirectory with sticky bit
        sticky_dir = dataset_dir / "sticky_subdir"
        sticky_dir.mkdir()
        os.chmod(sticky_dir, 0o1755)  # sticky bit + normal permissions
        
        # Add file to sticky directory
        test_file = sticky_dir / "file_in_sticky.txt"
        test_file.write_text("File in sticky directory\n" * 100)
        
        original_checksum = calculate_file_checksum(test_file)
        original_dir_mode = sticky_dir.stat().st_mode
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify directory and file integrity
        assert sticky_dir.exists(), "Sticky directory was removed"
        assert test_file.exists(), "File in sticky directory was moved or deleted"
        
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File in sticky directory was modified"
        
        current_dir_mode = sticky_dir.stat().st_mode
        assert current_dir_mode == original_dir_mode, "Sticky directory permissions changed"
    
    def test_different_owner_files(self, temp_test_dir):
        """Test handling of files owned by different users (if running as root or with sudo)."""
        test_dir = temp_test_dir / "owner_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "owned_file.txt"
        test_file.write_text("File with different ownership\n" * 100)
        
        original_checksum = calculate_file_checksum(test_file)
        original_stat = test_file.stat()
        
        # Try to change ownership (will only work if running as root)
        try:
            # Get a different user (usually 'nobody' exists)
            nobody_uid = pwd.getpwnam('nobody').pw_uid
            os.chown(test_file, nobody_uid, -1)  # Change owner, keep group
            ownership_changed = True
        except (KeyError, PermissionError, OSError):
            ownership_changed = False
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify file integrity regardless of ownership change
        assert test_file.exists(), "File was moved or deleted"
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File content was modified"


class TestUnixSpecialFiles:
    """Tests for UNIX special file types and filesystem features."""
    
    def test_named_pipes_fifos(self, temp_test_dir):
        """Test handling of named pipes (FIFOs)."""
        test_dir = temp_test_dir / "fifo_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create regular file
        regular_file = dataset_dir / "regular.txt"
        regular_file.write_text("Regular file content\n" * 100)
        
        # Create named pipe
        fifo_path = dataset_dir / "test_fifo"
        try:
            os.mkfifo(str(fifo_path))
            has_fifo = True
        except (OSError, AttributeError):
            # mkfifo not available or failed
            has_fifo = False
        
        original_checksum = calculate_file_checksum(regular_file)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Should handle gracefully and preserve regular files
        assert regular_file.exists(), "Regular file was affected by FIFO presence"
        current_checksum = calculate_file_checksum(regular_file)
        assert current_checksum == original_checksum, "Regular file was modified"
        
        if has_fifo:
            # Verify FIFO still exists and is unchanged
            assert fifo_path.exists(), "FIFO was removed"
            assert stat.S_ISFIFO(fifo_path.stat().st_mode), "FIFO type was changed"
    
    def test_device_files(self, temp_test_dir):
        """Test handling of device files (if accessible)."""
        test_dir = temp_test_dir / "device_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create regular files
        regular_file = dataset_dir / "regular.txt"
        regular_file.write_text("Regular file content\n" * 100)
        
        # Create symlink to a device file (safer than copying actual device files)
        try:
            device_link = dataset_dir / "null_device"
            device_link.symlink_to("/dev/null")
            has_device_link = True
        except (OSError, NotImplementedError):
            has_device_link = False
        
        original_checksum = calculate_file_checksum(regular_file)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Should handle gracefully
        assert regular_file.exists(), "Regular file was affected by device file presence"
        current_checksum = calculate_file_checksum(regular_file)
        assert current_checksum == original_checksum, "Regular file was modified"
        
        if has_device_link:
            assert device_link.exists(), "Device symlink was removed"
    
    def test_hard_links(self, temp_test_dir):
        """Test handling of hard links."""
        test_dir = temp_test_dir / "hardlink_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create original file
        original_file = dataset_dir / "original.txt"
        original_file.write_text("Hard linked content\n" * 200)
        
        # Create hard link
        try:
            hard_link = dataset_dir / "hardlink.txt"
            hard_link.hardlink_to(original_file)
            has_hardlink = True
        except (OSError, AttributeError):
            has_hardlink = False
        
        original_checksum = calculate_file_checksum(original_file)
        if has_hardlink:
            link_checksum = calculate_file_checksum(hard_link)
            assert original_checksum == link_checksum, "Hard link content differs from original"
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify both files remain unchanged
        assert original_file.exists(), "Original file was removed"
        current_checksum = calculate_file_checksum(original_file)
        assert current_checksum == original_checksum, "Original file was modified"
        
        if has_hardlink:
            assert hard_link.exists(), "Hard link was removed"
            current_link_checksum = calculate_file_checksum(hard_link)
            assert current_link_checksum == original_checksum, "Hard link was modified"
    
    def test_sparse_files(self, temp_test_dir):
        """Test handling of sparse files."""
        test_dir = temp_test_dir / "sparse_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create sparse file
        sparse_file = dataset_dir / "sparse.dat"
        try:
            with open(sparse_file, 'wb') as f:
                f.write(b'start')
                f.seek(1024 * 1024)  # Seek 1MB
                f.write(b'end')
            
            # Verify it's actually sparse
            file_size = sparse_file.stat().st_size
            blocks_used = sparse_file.stat().st_blocks
            is_sparse = (blocks_used * 512) < file_size  # Rough check
            
        except (OSError, AttributeError):
            is_sparse = False
        
        if not is_sparse:
            # Create a regular file instead for testing
            sparse_file.write_text("Regular file content\n" * 100)
        
        original_checksum = calculate_file_checksum(sparse_file)
        original_size = sparse_file.stat().st_size
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify file integrity
        assert sparse_file.exists(), "Sparse file was removed"
        current_checksum = calculate_file_checksum(sparse_file)
        assert current_checksum == original_checksum, "Sparse file was modified"
        
        current_size = sparse_file.stat().st_size
        assert current_size == original_size, "Sparse file size changed"


class TestUnixSignalHandling:
    """Tests for UNIX signal handling during compression."""
    
    def test_sigterm_handling(self, temp_test_dir):
        """Test graceful handling of SIGTERM signal."""
        test_dir = temp_test_dir / "sigterm_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create multiple files to give time for signal
        for i in range(10):
            test_file = dataset_dir / f"file_{i:02d}.txt"
            test_file.write_text(f"File {i} content\n" * 500)
        
        # Record original checksums
        original_checksums = {}
        for file_path in dataset_dir.rglob('*.txt'):
            original_checksums[str(file_path)] = calculate_file_checksum(file_path)
        
        def signal_after_delay():
            """Send SIGTERM after a short delay."""
            time.sleep(0.2)
            os.kill(os.getpid(), signal.SIGTERM)
        
        # Start signal thread
        signal_thread = threading.Thread(target=signal_after_delay)
        signal_thread.daemon = True
        
        # Process with signal interruption
        try:
            signal_thread.start()
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
        except KeyboardInterrupt:
            pass  # Expected due to SIGTERM
        
        # Verify all original files are intact
        for file_path_str, original_checksum in original_checksums.items():
            file_path = Path(file_path_str)
            assert file_path.exists(), f"File missing after SIGTERM: {file_path}"
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, f"File corrupted after SIGTERM: {file_path}"
    
    def test_sighup_handling(self, temp_test_dir):
        """Test handling of SIGHUP signal (hangup)."""
        test_dir = temp_test_dir / "sighup_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "hangup_test.txt"
        test_file.write_text("Hangup test content\n" * 1000)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Set up SIGHUP handler to avoid default terminate behavior
        def sighup_handler(signum, frame):
            pass  # Ignore SIGHUP
        
        old_handler = signal.signal(signal.SIGHUP, sighup_handler)
        
        try:
            def send_sighup():
                time.sleep(0.1)
                os.kill(os.getpid(), signal.SIGHUP)
            
            sighup_thread = threading.Thread(target=send_sighup)
            sighup_thread.daemon = True
            sighup_thread.start()
            
            # Process should continue despite SIGHUP
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            # Should complete successfully
            assert results['processed'] == 1
            
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGHUP, old_handler)
        
        # Verify file integrity
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File corrupted during SIGHUP handling"


class TestUnixFilesystemFeatures:
    """Tests for UNIX filesystem-specific features."""
    
    def test_case_sensitive_filesystem_edge_cases(self, temp_test_dir):
        """Test edge cases on case-sensitive filesystems."""
        test_dir = temp_test_dir / "case_edge_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create files with various case combinations
        case_files = [
            "lowercase.txt",
            "UPPERCASE.TXT", 
            "MixedCase.Txt",
            "camelCase.txt",
            "PascalCase.txt",
            "snake_case.txt",
            "SCREAMING_SNAKE_CASE.TXT"
        ]
        
        checksums = {}
        for filename in case_files:
            file_path = dataset_dir / filename
            content = f"Content for {filename}\n" * 100
            file_path.write_text(content)
            checksums[filename] = calculate_file_checksum(file_path)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify all case variations are preserved
        for filename, original_checksum in checksums.items():
            file_path = dataset_dir / filename
            assert file_path.exists(), f"Case-sensitive file missing: {filename}"
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, f"Case-sensitive file modified: {filename}"
    
    def test_extended_attributes_xattr(self, temp_test_dir):
        """Test handling of extended attributes (if supported)."""
        test_dir = temp_test_dir / "xattr_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "xattr_file.txt"
        test_file.write_text("File with extended attributes\n" * 100)
        
        # Try to set extended attributes
        try:
            import xattr
            xattr.setxattr(str(test_file), b'user.test_attr', b'test_value')
            has_xattr = True
            original_xattr = xattr.getxattr(str(test_file), b'user.test_attr')
        except (ImportError, OSError, AttributeError):
            has_xattr = False
            original_xattr = None
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify file content integrity
        assert test_file.exists(), "File with xattr was removed"
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File with xattr was modified"
        
        # Extended attributes may or may not be preserved (depends on filesystem/archive format)
        # The important thing is that the file content is intact
    
    def test_mount_point_boundaries(self, temp_test_dir):
        """Test behavior across filesystem mount point boundaries."""
        test_dir = temp_test_dir / "mount_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create files
        regular_file = dataset_dir / "regular.txt"
        regular_file.write_text("Regular file content\n" * 100)
        
        # Create what looks like a mount point subdirectory
        # (We can't create actual mount points in tests, but we can test the scenario)
        mount_like_dir = dataset_dir / "mnt" / "external"
        mount_like_dir.mkdir(parents=True)
        mount_file = mount_like_dir / "mounted_file.txt"
        mount_file.write_text("File in mount-like directory\n" * 100)
        
        original_checksums = {
            str(regular_file): calculate_file_checksum(regular_file),
            str(mount_file): calculate_file_checksum(mount_file)
        }
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify all files remain intact
        for file_path_str, original_checksum in original_checksums.items():
            file_path = Path(file_path_str)
            assert file_path.exists(), f"File missing after mount point test: {file_path}"
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, f"File modified in mount point test: {file_path}"
    
    def test_very_long_paths(self, temp_test_dir):
        """Test handling of very long paths (approaching PATH_MAX)."""
        test_dir = temp_test_dir / "long_path_test"
        test_dir.mkdir()
        
        # Create nested directory structure with long names
        current_dir = test_dir
        path_components = []
        
        # Build path close to system limits
        for i in range(20):  # 20 levels deep
            dir_name = f"very_long_directory_name_level_{i:02d}_with_extra_length"
            path_components.append(dir_name)
            current_dir = current_dir / dir_name
            
            try:
                current_dir.mkdir()
            except OSError as e:
                # Path too long for filesystem
                if "File name too long" in str(e) or "path too long" in str(e).lower():
                    break
                raise
        
        # Add file at deepest level
        if current_dir.exists():
            test_file = current_dir / "deep_file.txt"
            try:
                test_file.write_text("File at maximum depth\n" * 100)
                original_checksum = calculate_file_checksum(test_file)
                file_created = True
            except OSError:
                file_created = False
        else:
            file_created = False
        
        if file_created:
            # Process dataset
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(test_dir))
            
            # Verify deep file integrity
            assert test_file.exists(), "Deep file was removed"
            current_checksum = calculate_file_checksum(test_file)
            assert current_checksum == original_checksum, "Deep file was modified"


class TestUnixNetworkFilesystems:
    """Tests for network filesystem considerations."""
    
    def test_nfs_like_latency_simulation(self, temp_test_dir):
        """Test behavior with simulated network filesystem latency."""
        test_dir = temp_test_dir / "nfs_latency_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "network_file.txt"
        test_file.write_text("Network filesystem file\n" * 500)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Mock file operations to simulate network latency
        original_open = open
        
        def slow_open(*args, **kwargs):
            time.sleep(0.01)  # 10ms delay per operation
            return original_open(*args, **kwargs)
        
        with patch('builtins.open', side_effect=slow_open):
            # Process with simulated network delays
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
        
        # Should handle latency gracefully
        assert results['processed'] == 1
        
        # Verify file integrity despite latency
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File corrupted during network latency simulation"
    
    def test_stale_nfs_handle_simulation(self, temp_test_dir):
        """Test handling of stale NFS handle errors."""
        test_dir = temp_test_dir / "stale_nfs_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "stale_handle_file.txt"
        test_file.write_text("File with potential stale handle\n" * 500)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Mock file operations to simulate stale NFS handle
        call_count = 0
        
        def stale_handle_stat(path):
            nonlocal call_count
            call_count += 1
            if call_count == 3:  # Fail on third call
                raise OSError(116, "Stale file handle")  # ESTALE
            return os.stat(path)
        
        with patch('os.stat', side_effect=stale_handle_stat):
            # Should handle stale handle errors gracefully
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
        
        # Verify original file remains intact
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File corrupted during stale handle simulation"


class TestUnixResourceLimits:
    """Tests for UNIX resource limit scenarios."""
    
    def test_file_descriptor_limits(self, temp_test_dir):
        """Test behavior when approaching file descriptor limits."""
        test_dir = temp_test_dir / "fd_limit_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create many files to potentially stress file descriptor usage
        file_checksums = {}
        for i in range(100):  # Create 100 files
            test_file = dataset_dir / f"fd_test_{i:03d}.txt"
            content = f"File descriptor test {i}\n" * 50
            test_file.write_text(content)
            file_checksums[str(test_file)] = calculate_file_checksum(test_file)
        
        # Process dataset (should handle FD limits gracefully)
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify all files remain intact
        for file_path_str, original_checksum in file_checksums.items():
            file_path = Path(file_path_str)
            assert file_path.exists(), f"File missing: {file_path}"
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, f"File corrupted: {file_path}"
    
    def test_ulimit_simulation(self, temp_test_dir):
        """Test behavior under various ulimit constraints."""
        test_dir = temp_test_dir / "ulimit_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        test_file = dataset_dir / "ulimit_test.txt"
        test_file.write_text("Resource limit test content\n" * 500)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Get current resource limits
        try:
            import resource
            
            # Test with reduced CPU time limit
            old_cpu_limit = resource.getrlimit(resource.RLIMIT_CPU)
            
            # Set a reasonable CPU limit (30 seconds)
            resource.setrlimit(resource.RLIMIT_CPU, (30, old_cpu_limit[1]))
            
            try:
                # Process dataset
                zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
                results = zipper.process_directory(str(dataset_dir))
                
                # Should complete within time limit
                assert results['processed'] == 1
                
            finally:
                # Restore original limit
                resource.setrlimit(resource.RLIMIT_CPU, old_cpu_limit)
                
        except (ImportError, OSError):
            # Resource module not available or permission denied
            pytest.skip("Resource limits not testable in this environment")
        
        # Verify file integrity
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File corrupted under resource limits" 