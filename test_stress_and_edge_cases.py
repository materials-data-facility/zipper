#!/usr/bin/env python3
"""
Stress tests and edge case tests for MDF Zipper.

These tests focus on edge cases and stress scenarios that could occur
with high-value datasets in production environments.
"""

import pytest
import os
import sys
import tempfile
import threading
import time
import zipfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed

from mdf_zipper import MDFZipper, FolderInfo
from conftest import calculate_file_checksum


class TestStressScenarios:
    """Stress tests for high-load scenarios."""
    
    def test_deep_directory_nesting(self, temp_test_dir):
        """Test handling of deeply nested directory structures."""
        # Create deeply nested structure (50 levels deep)
        current_dir = temp_test_dir / "deep_structure"
        current_dir.mkdir()
        
        for i in range(50):
            current_dir = current_dir / f"level_{i:02d}"
            current_dir.mkdir()
            # Add a file at each level
            (current_dir / f"file_at_level_{i}.txt").write_text(f"Content at level {i}\n" * 10)
        
        # Test compression - use single_directory mode since we're processing the root directory
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(temp_test_dir / "deep_structure"))
        
        # Should handle deep nesting
        assert results['processed'] == 1
        assert results['compressed'] == 1
        
        # Verify archive contains all nested files
        archive_path = temp_test_dir / "deep_structure" / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        with zipfile.ZipFile(archive_path, 'r') as zf:
            # Should have 50 files (one at each level)
            assert len(zf.namelist()) == 50
    
    def test_many_subdirectories(self, temp_test_dir):
        """Test processing many subdirectories simultaneously."""
        # Create 100 small subdirectories
        base_dir = temp_test_dir / "many_subdirs"
        base_dir.mkdir()
        
        for i in range(100):
            sub_dir = base_dir / f"subdir_{i:03d}"
            sub_dir.mkdir()
            (sub_dir / "data.txt").write_text(f"Data for subdirectory {i}\n" * 50)
        
        # Process with multiple workers
        zipper = MDFZipper(max_size_gb=0.01, max_workers=8)
        results = zipper.process_directory(str(base_dir))
        
        # Should process all 100 subdirectories
        assert results['processed'] == 100
        assert results['compressed'] == 100  # All should be small enough
        
        # Verify all archives exist
        for i in range(100):
            archive_path = base_dir / f"subdir_{i:03d}" / ".mdf" / "dataset.zip"
            assert archive_path.exists(), f"Archive missing for subdir_{i:03d}"
    
    def test_concurrent_access_same_directory(self, temp_test_dir):
        """Test multiple concurrent zipper instances on same directory."""
        # Create test structure
        test_dir = temp_test_dir / "concurrent_test"
        test_dir.mkdir()
        
        for i in range(10):
            sub_dir = test_dir / f"dataset_{i}"
            sub_dir.mkdir()
            (sub_dir / "data.txt").write_text(f"Dataset {i} content\n" * 100)
        
        # Record initial checksums
        initial_checksums = {}
        for sub_dir in test_dir.iterdir():
            if sub_dir.is_dir():
                for file_path in sub_dir.rglob('*'):
                    if file_path.is_file():
                        initial_checksums[str(file_path)] = calculate_file_checksum(file_path)
        
        def run_zipper():
            zipper = MDFZipper(max_size_gb=0.01)
            return zipper.process_directory(str(test_dir))
        
        # Run 3 instances concurrently
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_zipper) for _ in range(3)]
            results = [f.result() for f in futures]
        
        # Verify all instances completed successfully
        for result in results:
            assert result['processed'] == 10
        
        # Verify data integrity
        for sub_dir in test_dir.iterdir():
            if sub_dir.is_dir():
                for file_path in sub_dir.rglob('*'):
                    if file_path.is_file() and '.mdf' not in file_path.parts:
                        current_checksum = calculate_file_checksum(file_path)
                        assert current_checksum == initial_checksums[str(file_path)], \
                            f"File modified by concurrent access: {file_path}"
    
    def test_memory_usage_large_number_files(self, temp_test_dir):
        """Test memory usage with large number of files."""
        import psutil
        import gc
        
        # Create directory with many files
        large_dir = temp_test_dir / "large_file_count"
        large_dir.mkdir()
        
        # Create 5000 small files
        for i in range(5000):
            (large_dir / f"file_{i:05d}.txt").write_text(f"File {i} content\n")
        
        # Measure initial memory
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process the directory - use single_directory mode for large file count
        zipper = MDFZipper(max_size_gb=0.1, single_directory=True)
        results = zipper.process_directory(str(large_dir))
        
        # Force garbage collection
        gc.collect()
        
        # Measure final memory
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (< 100MB for 5000 small files)
        assert memory_increase < 100, f"Excessive memory usage: {memory_increase:.1f}MB"
        assert results['compressed'] == 1, "Large file count directory should be compressed"


class TestEdgeCases:
    """Tests for edge cases and unusual scenarios."""
    
    def test_symlinks_handling(self, temp_test_dir):
        """Test handling of symbolic links."""
        # Create test structure with symlinks
        test_dir = temp_test_dir / "symlink_test"
        test_dir.mkdir()
        
        # Create original file
        original_file = test_dir / "original.txt"
        original_file.write_text("Original file content\n" * 100)
        
        # Create subdirectory
        sub_dir = test_dir / "subdir"
        sub_dir.mkdir()
        
        # Create symlinks (if supported by OS)
        try:
            symlink_file = test_dir / "symlink_to_original.txt"
            symlink_file.symlink_to(original_file)
            
            symlink_dir = test_dir / "symlink_to_subdir"
            symlink_dir.symlink_to(sub_dir)
            
            has_symlinks = True
        except (OSError, NotImplementedError):
            # Symlinks not supported on this platform
            has_symlinks = False
        
        # Process directory - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(test_dir))
        
        # Should handle symlinks gracefully
        assert results['processed'] == 1
        
        if has_symlinks:
            # Verify symlinks are handled properly (not followed recursively)
            archive_path = test_dir / ".mdf" / "dataset.zip"
            with zipfile.ZipFile(archive_path, 'r') as zf:
                files = zf.namelist()
                # Should not have duplicate content from symlinks
                assert len(files) >= 1  # At least the original file
    
    def test_very_long_filenames(self, temp_test_dir):
        """Test handling of very long filenames."""
        test_dir = temp_test_dir / "long_filename_test"
        test_dir.mkdir()
        
        # Create file with very long name (close to filesystem limit)
        long_name = "a" * 200  # Very long filename
        try:
            long_file = test_dir / f"{long_name}.txt"
            long_file.write_text("File with very long name\n" * 50)
            
            # Also create file in nested directory with long path
            nested_dir = test_dir / ("b" * 50) / ("c" * 50)
            nested_dir.mkdir(parents=True)
            (nested_dir / f"{long_name[:100]}.txt").write_text("Nested long file\n" * 30)
            
        except OSError:
            # Filename too long for this filesystem
            pytest.skip("Filesystem doesn't support very long filenames")
        
        # Process directory - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(test_dir))
        
        # Should handle long filenames
        assert results['processed'] == 1
        assert results['compressed'] == 1
    
    def test_unusual_file_permissions(self, temp_test_dir):
        """Test handling of files with unusual permissions."""
        if sys.platform == "win32":
            pytest.skip("Permission tests not applicable on Windows")
        
        test_dir = temp_test_dir / "permission_test"
        test_dir.mkdir()
        
        # Create files with different permissions
        normal_file = test_dir / "normal.txt"
        normal_file.write_text("Normal file\n" * 50)
        
        readonly_file = test_dir / "readonly.txt"
        readonly_file.write_text("Read-only file\n" * 50)
        os.chmod(readonly_file, 0o444)  # Read-only
        
        executable_file = test_dir / "executable.txt"
        executable_file.write_text("Executable file\n" * 50)
        os.chmod(executable_file, 0o755)  # Executable
        
        try:
            # Process directory - use single_directory mode
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(test_dir))
            
            # Should handle different permissions
            assert results['processed'] == 1
            assert results['compressed'] == 1
            
            # Verify archive contains all files
            archive_path = test_dir / ".mdf" / "dataset.zip"
            with zipfile.ZipFile(archive_path, 'r') as zf:
                assert len(zf.namelist()) == 3
                
        finally:
            # Restore permissions for cleanup
            os.chmod(readonly_file, 0o644)
    
    def test_case_sensitive_filenames(self, temp_test_dir):
        """Test handling of case-sensitive vs case-insensitive filesystems."""
        test_dir = temp_test_dir / "case_test"
        test_dir.mkdir()
        
        # Create files with similar names (different case)
        (test_dir / "file.txt").write_text("lowercase file\n" * 50)
        original_content = (test_dir / "file.txt").read_text()
        
        # Try to create files with different case
        try:
            (test_dir / "FILE.txt").write_text("uppercase file\n" * 50)
            (test_dir / "File.txt").write_text("mixed case file\n" * 50)
            
            # Check if we actually have multiple files or if content was overwritten
            files_created = list(test_dir.glob("*.txt"))
            current_content = (test_dir / "file.txt").read_text()
            
            # If we have 3 files and original content unchanged, filesystem is case-sensitive
            # If we have 1 file and content changed, filesystem is case-insensitive
            has_case_sensitivity = (len(files_created) == 3 and current_content == original_content)
            
        except FileExistsError:
            # Explicit case-insensitive filesystem
            has_case_sensitivity = False
        
        # Process directory - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(test_dir))
        
        # Should handle case sensitivity appropriately
        assert results['processed'] == 1
        assert results['compressed'] == 1
        
        # Verify archive
        archive_path = test_dir / ".mdf" / "dataset.zip"
        with zipfile.ZipFile(archive_path, 'r') as zf:
            files = zf.namelist()
            if has_case_sensitivity:
                assert len(files) == 3  # All three files
            else:
                assert len(files) == 1  # Only one file (case-insensitive filesystem)
    
    def test_filesystem_boundary_conditions(self, temp_test_dir):
        """Test behavior at filesystem boundaries and limits."""
        test_dir = temp_test_dir / "boundary_test"
        test_dir.mkdir()
        
        # Test with file size exactly at various powers of 2
        sizes = [1023, 1024, 1025, 2047, 2048, 2049]  # Around 1KB and 2KB boundaries
        
        for i, size in enumerate(sizes):
            file_path = test_dir / f"boundary_{i}_{size}.txt"
            content = "x" * size
            file_path.write_text(content)
        
        # Process directory - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(test_dir))
        
        # Should handle boundary conditions
        assert results['processed'] == 1
        assert results['compressed'] == 1
        
        # Verify all files are in archive
        archive_path = test_dir / ".mdf" / "dataset.zip"
        with zipfile.ZipFile(archive_path, 'r') as zf:
            assert len(zf.namelist()) == len(sizes)


class TestCorruptionDetection:
    """Tests to detect various forms of potential corruption."""
    
    def test_interrupt_simulation(self, temp_test_dir):
        """Test behavior when process is interrupted during compression."""
        test_dir = temp_test_dir / "interrupt_test"
        test_dir.mkdir()
        
        # Create test data
        for i in range(5):
            sub_dir = test_dir / f"dataset_{i}"
            sub_dir.mkdir()
            (sub_dir / "data.txt").write_text(f"Dataset {i} content\n" * 1000)
        
        # Mock zipfile to simulate interruption partway through
        original_zipfile = zipfile.ZipFile
        call_count = 0
        
        def interrupt_after_two_calls(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 2:
                raise KeyboardInterrupt("Simulated interruption")
            return original_zipfile(*args, **kwargs)
        
        with patch('zipfile.ZipFile', side_effect=interrupt_after_two_calls):
            zipper = MDFZipper(max_size_gb=0.01)
            
            # Should handle interruption gracefully
            with pytest.raises(KeyboardInterrupt):
                zipper.process_directory(str(test_dir))
        
        # Verify no partial or corrupted archives exist
        for sub_dir in test_dir.iterdir():
            if sub_dir.is_dir():
                archive_path = sub_dir / ".mdf" / "dataset.zip"
                if archive_path.exists():
                    # If archive exists, it should be valid
                    assert zipfile.is_zipfile(archive_path), f"Corrupted archive: {archive_path}"
    
    def test_disk_full_simulation(self, temp_test_dir):
        """Test behavior when disk becomes full during compression."""
        test_dir = temp_test_dir / "disk_full_test"
        test_dir.mkdir()
        
        # Create test data
        sub_dir = test_dir / "dataset"
        sub_dir.mkdir()
        (sub_dir / "data.txt").write_text("Test data\n" * 1000)
        
        # Mock zipfile to simulate disk full error
        def simulate_disk_full(*args, **kwargs):
            raise OSError(28, "No space left on device")  # ENOSPC
        
        with patch('zipfile.ZipFile', side_effect=simulate_disk_full):
            zipper = MDFZipper(max_size_gb=0.01)
            results = zipper.process_directory(str(test_dir))
            
            # Should handle disk full gracefully
            assert results['failed'] > 0
            assert 'compressed' in results
        
        # Verify no corrupted archives exist
        archive_path = sub_dir / ".mdf" / "dataset.zip"
        if archive_path.exists():
            assert zipfile.is_zipfile(archive_path), "Archive should be valid or not exist"
    
    def test_zip_corruption_detection(self, temp_test_dir):
        """Test detection of zip file corruption."""
        test_dir = temp_test_dir / "corruption_test"
        test_dir.mkdir()
        
        # Create and process a dataset normally
        sub_dir = test_dir / "dataset"
        sub_dir.mkdir()
        (sub_dir / "data.txt").write_text("Test data\n" * 100)
        
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(test_dir))
        
        archive_path = sub_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        # Verify archive is initially valid
        with zipfile.ZipFile(archive_path, 'r') as zf:
            initial_test = zf.testzip()
            assert initial_test is None, "Archive should be initially valid"
        
        # Corrupt the archive by overwriting part of the content
        with open(archive_path, 'r+b') as f:
            f.seek(50)  # Seek to middle of file
            f.write(b'CORRUPTED_DATA_HERE!')  # Overwrite with corruption
        
        # Test that corrupted archive is detected - either by BadZipFile exception or testzip
        corruption_detected = False
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                corrupted_file = zf.testzip()
                if corrupted_file is not None:
                    corruption_detected = True
        except zipfile.BadZipFile:
            # Archive is so corrupted it can't even be opened
            corruption_detected = True
        
        assert corruption_detected, "Corruption should be detected either by BadZipFile exception or testzip"


class TestRealWorldScenarios:
    """Tests simulating real-world dataset scenarios."""
    
    def test_mixed_file_types_dataset(self, temp_test_dir):
        """Test dataset with mixed file types (text, binary, empty)."""
        dataset_dir = temp_test_dir / "mixed_dataset"
        dataset_dir.mkdir()
        
        # Text files
        (dataset_dir / "readme.txt").write_text("Dataset documentation\n" * 100)
        (dataset_dir / "metadata.json").write_text('{"version": "1.0", "files": 100}\n' * 50)
        
        # Binary-like files
        (dataset_dir / "data.bin").write_bytes(b'\x89PNG\r\n\x1a\n' * 1000)  # Fake PNG
        (dataset_dir / "model.pkl").write_bytes(b'\x80\x03}' * 500)  # Fake pickle
        
        # Empty files
        (dataset_dir / "empty.txt").touch()
        (dataset_dir / "placeholder.dat").touch()
        
        # Very small files
        (dataset_dir / "tiny.txt").write_text("x")
        
        # Nested structure
        nested_dir = dataset_dir / "analysis" / "results"
        nested_dir.mkdir(parents=True)
        (nested_dir / "summary.csv").write_text("id,value\n" + "1,test\n" * 500)
        
        # Process dataset - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify processing
        assert results['processed'] == 1
        assert results['compressed'] == 1
        
        # Verify archive integrity
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert zipfile.is_zipfile(archive_path)
        
        with zipfile.ZipFile(archive_path, 'r') as zf:
            # Should contain all files including empty ones
            files = zf.namelist()
            assert len(files) >= 7  # At least 7 files created
            
            # Test specific files
            assert any("readme.txt" in f for f in files)
            assert any("empty.txt" in f for f in files)
            assert any("summary.csv" in f for f in files)
    
    def test_scientific_dataset_structure(self, temp_test_dir):
        """Test structure typical of scientific datasets."""
        experiment_dir = temp_test_dir / "experiment_001"
        experiment_dir.mkdir()
        
        # Raw data
        raw_dir = experiment_dir / "raw_data"
        raw_dir.mkdir()
        for i in range(10):
            (raw_dir / f"measurement_{i:03d}.txt").write_text(f"measurement data {i}\n" * 200)
        
        # Processed data
        processed_dir = experiment_dir / "processed"
        processed_dir.mkdir()
        (processed_dir / "cleaned_data.csv").write_text("time,value,error\n" + "1.0,2.5,0.1\n" * 1000)
        (processed_dir / "aggregated.json").write_text('{"mean": 2.5, "std": 0.3}\n' * 100)
        
        # Analysis
        analysis_dir = experiment_dir / "analysis"
        analysis_dir.mkdir()
        (analysis_dir / "notebook.ipynb").write_text('{"cells": []}\n' * 50)
        (analysis_dir / "plots.png").write_bytes(b'\x89PNG\r\n\x1a\n' * 500)
        
        # Documentation
        (experiment_dir / "README.md").write_text("# Experiment 001\n" * 100)
        (experiment_dir / "protocol.txt").write_text("Experimental protocol\n" * 200)
        
        # Process experiment - use single_directory mode
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(experiment_dir))
        
        # Verify scientific dataset handling
        assert results['processed'] == 1
        assert results['compressed'] == 1
        
        # Verify all scientific data is preserved
        archive_path = experiment_dir / ".mdf" / "dataset.zip"
        with zipfile.ZipFile(archive_path, 'r') as zf:
            files = zf.namelist()
            
            # Check for key scientific files
            assert any("README.md" in f for f in files)
            assert any("protocol.txt" in f for f in files)
            assert any("cleaned_data.csv" in f for f in files)
            assert any("notebook.ipynb" in f for f in files)
            
            # Check raw data files
            raw_files = [f for f in files if "measurement_" in f]
            assert len(raw_files) == 10  # All measurement files 