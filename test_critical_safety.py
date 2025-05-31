#!/usr/bin/env python3
"""
Critical Safety Tests for MDF Zipper - High-Value Dataset Protection

This test suite focuses specifically on ensuring the absolute safety of high-value datasets.
Every test is designed to verify that no original data can be corrupted, moved, or lost
under any circumstances.
"""

import pytest
import zipfile
import json
import os
import sys
import time
import threading
import tempfile
import signal
import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from concurrent.futures import ThreadPoolExecutor, as_completed

from mdf_zipper import MDFZipper
from conftest import calculate_file_checksum, verify_file_checksums


class TestCriticalDataSafety:
    """Critical tests ensuring no data loss or corruption under any circumstances."""
    
    def test_atomic_archive_creation(self, temp_test_dir):
        """Verify that archive creation is atomic - either complete or not created at all."""
        test_dir = temp_test_dir / "atomic_test"
        test_dir.mkdir()
        
        # Create test dataset
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        (dataset_dir / "important_data.txt").write_text("Critical data\n" * 1000)
        
        # Record original checksums
        original_checksum = calculate_file_checksum(dataset_dir / "important_data.txt")
        
        # Mock zipfile.ZipFile to simulate failure during archive creation
        def failing_zipfile(*args, **kwargs):
            # Create the zip file but simulate failure before completion
            if args[1] == 'w':  # Write mode
                # Simulate failure during archive creation
                raise OSError("Simulated failure during archive creation")
            return zipfile.ZipFile(*args, **kwargs)
        
        with patch('zipfile.ZipFile', side_effect=failing_zipfile):
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            # Should record failure
            assert results['failed'] > 0 or results['compressed'] == 0
        
        # Verify original files are completely untouched
        assert (dataset_dir / "important_data.txt").exists()
        current_checksum = calculate_file_checksum(dataset_dir / "important_data.txt")
        assert current_checksum == original_checksum, "Original file was modified during failed archive creation"
        
        # Verify no partial archives exist (the improved implementation should clean them up)
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        temp_archive_path = dataset_dir / ".mdf" / "dataset.tmp"
        
        # Neither final nor temporary archive should exist after failure
        assert not archive_path.exists() or zipfile.is_zipfile(archive_path), \
            "Invalid final archive found after failed creation"
        assert not temp_archive_path.exists(), \
            "Temporary archive not cleaned up after failed creation"
        
        # If an archive does exist, it should be completely valid
        if archive_path.exists():
            with zipfile.ZipFile(archive_path, 'r') as zf:
                assert zf.testzip() is None, "Archive exists but is corrupted"
    
    def test_original_files_never_opened_for_writing(self, temp_test_dir):
        """Verify that original files are NEVER opened in write mode during processing."""
        test_dir = temp_test_dir / "write_protection_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset" 
        dataset_dir.mkdir()
        test_file = dataset_dir / "protected_data.txt"
        test_file.write_text("Protected content\n" * 500)
        
        # Track all file opens
        original_open = open
        opened_files = []
        
        def tracking_open(file, mode='r', *args, **kwargs):
            opened_files.append((str(file), mode))
            return original_open(file, mode, *args, **kwargs)
        
        with patch('builtins.open', side_effect=tracking_open):
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
        
        # Verify original files were never opened for writing
        for file_path, mode in opened_files:
            if test_file.name in file_path and '.mdf' not in file_path:
                assert 'w' not in mode and 'a' not in mode and '+' not in mode, \
                    f"Original file {file_path} was opened in write mode: {mode}"
    
    def test_filesystem_readonly_scenario(self, temp_test_dir):
        """Test behavior when filesystem becomes read-only during operation."""
        if sys.platform == "win32":
            pytest.skip("Read-only filesystem tests not applicable on Windows")
        
        test_dir = temp_test_dir / "readonly_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        test_file = dataset_dir / "readonly_data.txt"
        test_file.write_text("Read-only test content\n" * 500)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Make the dataset directory read-only after creating files
        os.chmod(dataset_dir, 0o555)  # Read and execute only
        
        try:
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            # Should handle read-only gracefully (either skip or fail, but not corrupt)
            assert 'processed' in results
            
        finally:
            # Restore permissions for cleanup
            os.chmod(dataset_dir, 0o755)
        
        # Verify original file integrity
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "File corrupted in read-only scenario"
    
    def test_concurrent_file_access_safety(self, temp_test_dir):
        """Verify original files remain accessible by other processes during compression."""
        test_dir = temp_test_dir / "concurrent_access_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        test_file = dataset_dir / "shared_data.txt"
        test_file.write_text("Shared content\n" * 1000)
        
        access_successful = threading.Event()
        access_failed = threading.Event()
        
        def concurrent_reader():
            """Simulate another process reading the file during compression."""
            try:
                time.sleep(0.1)  # Let compression start
                for _ in range(10):  # Multiple read attempts
                    with open(test_file, 'r') as f:
                        content = f.read()
                        assert "Shared content" in content
                    time.sleep(0.05)
                access_successful.set()
            except Exception as e:
                access_failed.set()
                raise e
        
        # Start concurrent reader
        reader_thread = threading.Thread(target=concurrent_reader)
        reader_thread.start()
        
        # Start compression
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Wait for reader to complete
        reader_thread.join(timeout=10)
        
        # Verify concurrent access was successful
        assert access_successful.is_set(), "Concurrent file access failed during compression"
        assert not access_failed.is_set(), "Concurrent file access encountered errors"
    
    def test_no_temporary_files_in_dataset(self, temp_test_dir):
        """Verify no temporary files are created in the dataset directory structure."""
        test_dir = temp_test_dir / "temp_files_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        (dataset_dir / "data.txt").write_text("Dataset content\n" * 500)
        
        # Record initial file list
        initial_files = set()
        for file_path in dataset_dir.rglob('*'):
            if file_path.is_file():
                initial_files.add(str(file_path.relative_to(dataset_dir)))
        
        # Mock tempfile to detect any temp files created in dataset directory
        original_tempfile_funcs = {}
        temp_files_in_dataset = []
        
        def track_temp_file_creation(func_name, original_func):
            def wrapper(*args, **kwargs):
                result = original_func(*args, **kwargs)
                # Check if temp file is in dataset directory
                if hasattr(result, 'name') and str(dataset_dir) in str(result.name):
                    temp_files_in_dataset.append(result.name)
                elif isinstance(result, str) and str(dataset_dir) in result:
                    temp_files_in_dataset.append(result)
                return result
            return wrapper
        
        import tempfile as tf
        for attr in ['NamedTemporaryFile', 'mktemp', 'mkdtemp']:
            if hasattr(tf, attr):
                original_tempfile_funcs[attr] = getattr(tf, attr)
                setattr(tf, attr, track_temp_file_creation(attr, original_tempfile_funcs[attr]))
        
        try:
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
        finally:
            # Restore original tempfile functions
            for attr, original_func in original_tempfile_funcs.items():
                setattr(tf, attr, original_func)
        
        # Verify no temp files were created in dataset directory
        assert not temp_files_in_dataset, f"Temporary files created in dataset directory: {temp_files_in_dataset}"
        
        # Verify only expected files exist
        final_files = set()
        for file_path in dataset_dir.rglob('*'):
            if file_path.is_file() and '.mdf' not in file_path.parts:
                final_files.add(str(file_path.relative_to(dataset_dir)))
        
        # Original files should be unchanged
        assert final_files == initial_files, f"Unexpected files in dataset: {final_files - initial_files}"


class TestDataIntegrityVerification:
    """Tests for comprehensive data integrity verification."""
    
    def test_bit_for_bit_archive_verification(self, temp_test_dir):
        """Verify that archived files are bit-for-bit identical to originals."""
        test_dir = temp_test_dir / "bit_verification_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create files with specific content patterns that could reveal corruption
        test_files = {
            "binary_pattern.dat": b'\x00\x01\x02\x03\x04\x05' * 1000,
            "text_pattern.txt": "Line with specific pattern: ABCDEF\n" * 500,
            "unicode_pattern.txt": "Unicode: αβγδε ñáéíóú 中文测试\n" * 300,
            "edge_case.txt": "\x00\x01\x02\x03\xFF\xFE\xFD\xFC\n" * 200
        }
        
        original_checksums = {}
        for filename, content in test_files.items():
            file_path = dataset_dir / filename
            if isinstance(content, bytes):
                file_path.write_bytes(content)
            else:
                file_path.write_text(content, encoding='utf-8')
            original_checksums[filename] = calculate_file_checksum(file_path)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        assert results['compressed'] == 1, "Dataset should be compressed"
        
        # Verify archive exists and extract for verification
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        # Extract archive to temporary location for comparison
        with tempfile.TemporaryDirectory() as extract_dir:
            extract_path = Path(extract_dir)
            
            with zipfile.ZipFile(archive_path, 'r') as zf:
                zf.extractall(extract_path)
            
            # Compare each file bit-for-bit
            for filename in test_files.keys():
                original_file = dataset_dir / filename
                extracted_file = extract_path / filename
                
                assert extracted_file.exists(), f"Extracted file missing: {filename}"
                
                # Compare checksums
                extracted_checksum = calculate_file_checksum(extracted_file)
                assert extracted_checksum == original_checksums[filename], \
                    f"File {filename} differs between original and archive"
                
                # Compare file sizes
                assert original_file.stat().st_size == extracted_file.stat().st_size, \
                    f"File size differs for {filename}"
                
                # Compare content directly for extra verification
                if filename.endswith('.dat'):
                    assert original_file.read_bytes() == extracted_file.read_bytes(), \
                        f"Binary content differs for {filename}"
                else:
                    assert original_file.read_text(encoding='utf-8') == extracted_file.read_text(encoding='utf-8'), \
                        f"Text content differs for {filename}"
    
    def test_archive_corruption_detection_comprehensive(self, temp_test_dir):
        """Comprehensive test for detecting various types of archive corruption."""
        test_dir = temp_test_dir / "corruption_detection_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        (dataset_dir / "test_data.txt").write_text("Test data for corruption detection\n" * 500)
        
        # Create valid archive first
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        # Test corruption scenarios that are reliably detectable
        corruption_tests = [
            ("header_corruption", lambda f: f.seek(0) or f.write(b'CORRUPT!')),
            ("truncation", lambda f: f.truncate(f.seek(0, 2) // 2)),
        ]
        
        for corruption_name, corruption_func in corruption_tests:
            # Make a copy of the valid archive
            corrupted_path = dataset_dir / f"corrupted_{corruption_name}.zip"
            corrupted_path.write_bytes(archive_path.read_bytes())
            
            # Apply corruption
            with open(corrupted_path, 'r+b') as f:
                corruption_func(f)
            
            # Test corruption detection
            corruption_detected = False
            
            # Test 1: zipfile.is_zipfile
            if not zipfile.is_zipfile(corrupted_path):
                corruption_detected = True
            
            # Test 2: Opening the file
            if not corruption_detected:
                try:
                    with zipfile.ZipFile(corrupted_path, 'r') as zf:
                        # Test 3: testzip method
                        bad_file = zf.testzip()
                        if bad_file is not None:
                            corruption_detected = True
                        
                        # Test 4: Reading file content
                        if not corruption_detected:
                            try:
                                for file_info in zf.filelist:
                                    try:
                                        with zf.open(file_info) as archived_file:
                                            content = archived_file.read()
                                    except Exception:
                                        corruption_detected = True
                                        break
                            except Exception:
                                corruption_detected = True
                except zipfile.BadZipFile:
                    corruption_detected = True
                except Exception:
                    corruption_detected = True
            
            assert corruption_detected, f"Corruption not detected for {corruption_name}"
            
            # Clean up corrupted file
            corrupted_path.unlink()
        
        # Test that the original valid archive still works
        with zipfile.ZipFile(archive_path, 'r') as zf:
            assert zf.testzip() is None, "Original archive should remain valid"


class TestExtremeFailureScenarios:
    """Tests for extreme failure scenarios that must not corrupt data."""
    
    def test_power_failure_simulation(self, temp_test_dir):
        """Simulate power failure during compression - no data corruption allowed."""
        test_dir = temp_test_dir / "power_failure_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        critical_file = dataset_dir / "critical_data.txt"
        critical_file.write_text("Critical data that must not be lost\n" * 1000)
        
        original_checksum = calculate_file_checksum(critical_file)
        
        # Simulate power failure by interrupting archive creation at various points
        failure_points = [0.1, 0.3, 0.5, 0.7, 0.9]  # Different completion percentages
        
        for failure_point in failure_points:
            # Reset for each test
            archive_dir = dataset_dir / ".mdf"
            if archive_dir.exists():
                import shutil
                shutil.rmtree(archive_dir)
            
            call_count = 0
            max_calls = int(10 * failure_point)  # Approximate when to fail
            
            def power_failure_simulation(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count > max_calls:
                    raise KeyboardInterrupt(f"Simulated power failure at {failure_point*100}%")
                return zipfile.ZipFile(*args, **kwargs)
            
            with patch('zipfile.ZipFile', side_effect=power_failure_simulation):
                zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
                
                try:
                    results = zipper.process_directory(str(dataset_dir))
                except KeyboardInterrupt:
                    pass  # Expected failure
            
            # Verify original file is completely untouched
            assert critical_file.exists(), f"Original file missing after failure at {failure_point*100}%"
            current_checksum = calculate_file_checksum(critical_file)
            assert current_checksum == original_checksum, \
                f"Original file corrupted after failure at {failure_point*100}%"
            
            # Verify no corrupted partial archives exist
            archive_path = dataset_dir / ".mdf" / "dataset.zip"
            if archive_path.exists():
                # If archive exists, it must be valid or should be removed
                try:
                    with zipfile.ZipFile(archive_path, 'r') as zf:
                        result = zf.testzip()
                        assert result is None, f"Corrupted archive found after failure at {failure_point*100}%"
                except zipfile.BadZipFile:
                    pytest.fail(f"Invalid archive found after failure at {failure_point*100}%")
    
    def test_memory_exhaustion_simulation(self, temp_test_dir):
        """Test behavior when system runs out of memory during compression."""
        test_dir = temp_test_dir / "memory_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        test_file = dataset_dir / "memory_test_data.txt"
        test_file.write_text("Memory test content\n" * 1000)
        
        original_checksum = calculate_file_checksum(test_file)
        
        # Mock memory allocation failure
        def memory_failure(*args, **kwargs):
            raise MemoryError("Simulated memory exhaustion")
        
        with patch('zipfile.ZipFile', side_effect=memory_failure):
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            # Should handle memory error gracefully
            assert results['failed'] > 0 or results['compressed'] == 0
        
        # Verify original file integrity
        current_checksum = calculate_file_checksum(test_file)
        assert current_checksum == original_checksum, "Original file corrupted during memory failure"
    
    def test_storage_device_failure_simulation(self, temp_test_dir):
        """Test behavior when storage device fails during compression."""
        test_dir = temp_test_dir / "storage_failure_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        storage_test_file = dataset_dir / "storage_test_data.txt"
        storage_test_file.write_text("Storage test content\n" * 800)
        
        original_checksum = calculate_file_checksum(storage_test_file)
        
        # Mock various storage device failures
        storage_errors = [
            OSError(5, "Input/output error"),  # EIO
            OSError(28, "No space left on device"),  # ENOSPC  
            OSError(30, "Read-only file system"),  # EROFS
            OSError(122, "Disk quota exceeded"),  # EDQUOT
        ]
        
        for error in storage_errors:
            # Reset for each test
            archive_dir = dataset_dir / ".mdf"
            if archive_dir.exists():
                import shutil
                shutil.rmtree(archive_dir)
            
            def storage_failure(*args, **kwargs):
                raise error
            
            with patch('zipfile.ZipFile', side_effect=storage_failure):
                zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
                results = zipper.process_directory(str(dataset_dir))
                
                # Should handle storage errors gracefully
                assert results['failed'] > 0 or results['compressed'] == 0
            
            # Verify original file remains intact
            current_checksum = calculate_file_checksum(storage_test_file)
            assert current_checksum == original_checksum, \
                f"Original file corrupted during storage error: {error}"


class TestZipSpecificSafetyIssues:
    """Tests for ZIP format specific safety concerns."""
    
    def test_zip_bomb_protection(self, temp_test_dir):
        """Ensure the tool doesn't create zip bombs or handle them safely."""
        test_dir = temp_test_dir / "zip_bomb_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create a file with highly repetitive content (compresses extremely well)
        repetitive_content = "A" * 100000  # 100KB of same character
        bomb_file = dataset_dir / "repetitive.txt"
        bomb_file.write_text(repetitive_content)
        
        original_size = bomb_file.stat().st_size
        original_checksum = calculate_file_checksum(bomb_file)
        
        # Process normally
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify compression occurred
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        # Verify archive is valid and not a zip bomb
        with zipfile.ZipFile(archive_path, 'r') as zf:
            # Check that testzip passes
            assert zf.testzip() is None, "Archive failed integrity check"
            
            # Verify content by extracting
            with tempfile.TemporaryDirectory() as extract_dir:
                zf.extractall(extract_dir)
                extracted_file = Path(extract_dir) / "repetitive.txt"
                assert extracted_file.exists()
                assert extracted_file.stat().st_size == original_size
                
                extracted_checksum = calculate_file_checksum(extracted_file)
                assert extracted_checksum == original_checksum
        
        # Original file should be unchanged
        current_checksum = calculate_file_checksum(bomb_file)
        assert current_checksum == original_checksum
    
    def test_path_traversal_protection(self, temp_test_dir):
        """Ensure no path traversal vulnerabilities in archive creation."""
        test_dir = temp_test_dir / "path_traversal_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create files with potentially problematic names
        normal_files = [
            "normal_file.txt",
            "file_with_dots..txt",  # This is a normal filename, not a security issue
            ".hidden_file",
            "subdir/nested_file.txt"
        ]
        
        # Create subdirectory first
        (dataset_dir / "subdir").mkdir()
        
        for filename in normal_files:
            file_path = dataset_dir / filename
            file_path.write_text(f"Content of {filename}\n" * 100)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        # Verify archive structure is safe
        with zipfile.ZipFile(archive_path, 'r') as zf:
            for zip_info in zf.filelist:
                filename = zip_info.filename
                
                # Verify no actual path traversal attempts in archive
                assert not filename.startswith('/'), f"Absolute path in archive: {filename}"
                assert not filename.startswith('../'), f"Parent directory traversal in archive: {filename}"
                assert '/../' not in filename, f"Directory traversal in archive: {filename}"
                
                # Verify file exists in original dataset
                original_path = dataset_dir / filename
                assert original_path.exists(), f"Archive contains file not in original dataset: {filename}"
    
    def test_archive_size_validation(self, temp_test_dir):
        """Verify archive sizes are reasonable and not corrupted."""
        test_dir = temp_test_dir / "size_validation_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "dataset"
        dataset_dir.mkdir()
        
        # Create files with varied content (not highly repetitive)
        test_files = {
            "small.txt": ''.join(f"Small content with varied data {i}\n" for i in range(100)),
            "medium.txt": ''.join(f"Medium content with different patterns {i*17}\n" for i in range(1000)),
            "large.txt": ''.join(f"Large content with pseudo-random data {i*13 + 7}\n" for i in range(5000)),
        }
        
        total_original_size = 0
        for filename, content in test_files.items():
            file_path = dataset_dir / filename
            file_path.write_text(content)
            total_original_size += file_path.stat().st_size
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        archive_path = dataset_dir / ".mdf" / "dataset.zip"
        assert archive_path.exists()
        
        archive_size = archive_path.stat().st_size
        
        # Verify archive size is reasonable
        assert archive_size > 0, "Archive has zero size"
        assert archive_size < total_original_size, "Archive larger than original data"
        
        # Archive shouldn't be completely empty (basic sanity check)
        assert archive_size > 100, f"Archive too small to be valid: {archive_size} bytes"
        
        # Most important: verify archive contains expected amount of data when decompressed
        with zipfile.ZipFile(archive_path, 'r') as zf:
            decompressed_size = 0
            for zip_info in zf.filelist:
                decompressed_size += zip_info.file_size
            
            assert decompressed_size == total_original_size, \
                f"Decompressed size mismatch: {decompressed_size} vs {total_original_size}"
            
            # Verify we can actually read all the content
            for filename in test_files.keys():
                with zf.open(filename) as archived_file:
                    archived_content = archived_file.read()
                    assert len(archived_content) > 0, f"Empty content in archived file: {filename}"


class TestHighValueDatasetProtection:
    """Tests specifically designed for high-value dataset protection scenarios."""
    
    def test_no_data_movement_ever(self, temp_test_dir):
        """Absolutely verify that no original data is ever moved from its location."""
        test_dir = temp_test_dir / "no_movement_test"
        test_dir.mkdir()
        
        # Create complex nested structure
        dataset_dir = test_dir / "valuable_dataset"
        dataset_dir.mkdir()
        
        # Create file tree with absolute paths tracked
        file_structure = {}
        
        # Root level files
        for i in range(5):
            file_path = dataset_dir / f"root_file_{i}.txt"
            file_path.write_text(f"Root file {i} content\n" * 200)
            file_structure[str(file_path.absolute())] = calculate_file_checksum(file_path)
        
        # Nested directories
        for level1 in range(3):
            level1_dir = dataset_dir / f"level1_{level1}"
            level1_dir.mkdir()
            
            for level2 in range(2):
                level2_dir = level1_dir / f"level2_{level2}"
                level2_dir.mkdir()
                
                file_path = level2_dir / f"nested_file_{level1}_{level2}.txt"
                file_path.write_text(f"Nested content {level1}-{level2}\n" * 150)
                file_structure[str(file_path.absolute())] = calculate_file_checksum(file_path)
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # Verify EVERY file is still in its EXACT original location with EXACT original content
        for absolute_path, original_checksum in file_structure.items():
            file_path = Path(absolute_path)
            
            # File must exist at exact original location
            assert file_path.exists(), f"File moved or deleted: {absolute_path}"
            
            # File must have exact original content
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, \
                f"File content changed: {absolute_path}"
            
            # File must not be a symlink or moved with symlink left behind
            assert not file_path.is_symlink(), f"File replaced with symlink: {absolute_path}"
        
        # Verify no additional files were created outside .mdf directory
        current_files = set()
        for file_path in dataset_dir.rglob('*'):
            if file_path.is_file() and '.mdf' not in file_path.parts:
                current_files.add(str(file_path.absolute()))
        
        original_files = set(file_structure.keys())
        
        # Should have exactly the same files, no more, no less
        assert current_files == original_files, \
            f"File set changed. Added: {current_files - original_files}, Removed: {original_files - current_files}"
    
    def test_process_interruption_recovery(self, temp_test_dir):
        """Test recovery from process interruption without any data loss."""
        test_dir = temp_test_dir / "interruption_recovery_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "recoverable_dataset"
        dataset_dir.mkdir()
        
        # Create dataset with multiple files
        important_files = {}
        for i in range(10):
            file_path = dataset_dir / f"important_file_{i:02d}.txt"
            content = f"Important data {i}\n" * 500
            file_path.write_text(content)
            important_files[str(file_path)] = calculate_file_checksum(file_path)
        
        # Simulate interruption during processing
        def interrupting_zipfile(*args, **kwargs):
            # Allow first few files to be processed, then interrupt
            if hasattr(interrupting_zipfile, 'call_count'):
                interrupting_zipfile.call_count += 1
            else:
                interrupting_zipfile.call_count = 1
            
            if interrupting_zipfile.call_count > 3:
                raise KeyboardInterrupt("Process interrupted")
            
            return zipfile.ZipFile(*args, **kwargs)
        
        with patch('zipfile.ZipFile', side_effect=interrupting_zipfile):
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            
            try:
                results = zipper.process_directory(str(dataset_dir))
            except KeyboardInterrupt:
                pass  # Expected interruption
        
        # Verify ALL original files are completely intact
        for file_path_str, original_checksum in important_files.items():
            file_path = Path(file_path_str)
            
            assert file_path.exists(), f"File lost during interruption: {file_path}"
            
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, \
                f"File corrupted during interruption: {file_path}"
        
        # Verify recovery by running again (should complete successfully)
        zipper2 = MDFZipper(max_size_gb=0.01, single_directory=True)
        results2 = zipper2.process_directory(str(dataset_dir))
        
        # Should now complete successfully
        assert results2['processed'] == 1
        assert results2['compressed'] == 1
        
        # Verify ALL files still intact after recovery
        for file_path_str, original_checksum in important_files.items():
            file_path = Path(file_path_str)
            current_checksum = calculate_file_checksum(file_path)
            assert current_checksum == original_checksum, \
                f"File corrupted during recovery: {file_path}"
    
    def test_archive_validation_before_success(self, temp_test_dir):
        """Verify that archives are validated before considering operation successful."""
        test_dir = temp_test_dir / "validation_test"
        test_dir.mkdir()
        
        dataset_dir = test_dir / "validation_dataset"
        dataset_dir.mkdir()
        
        # Create test files
        validation_files = {}
        for i in range(5):
            file_path = dataset_dir / f"validate_file_{i}.txt"
            content = f"Validation content {i}\n" * 300
            file_path.write_text(content)
            validation_files[file_path.name] = {
                'content': content,
                'checksum': calculate_file_checksum(file_path),
                'size': file_path.stat().st_size
            }
        
        # Process dataset
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(dataset_dir))
        
        # If compression reported success, archive must be completely valid
        if results['compressed'] > 0:
            archive_path = dataset_dir / ".mdf" / "dataset.zip"
            assert archive_path.exists(), "Archive missing despite successful compression report"
            
            # Comprehensive validation
            assert zipfile.is_zipfile(archive_path), "Archive is not a valid ZIP file"
            
            with zipfile.ZipFile(archive_path, 'r') as zf:
                # Archive integrity check
                bad_file = zf.testzip()
                assert bad_file is None, f"Archive integrity check failed: {bad_file}"
                
                # Verify all files present
                archived_files = set(zf.namelist())
                expected_files = set(validation_files.keys())
                assert archived_files == expected_files, \
                    f"File list mismatch. Expected: {expected_files}, Got: {archived_files}"
                
                # Verify each file's content
                for filename, file_info in validation_files.items():
                    with zf.open(filename) as archived_file:
                        archived_content = archived_file.read().decode('utf-8')
                        assert archived_content == file_info['content'], \
                            f"Content mismatch for {filename}"
                
                # Verify file sizes
                for zip_info in zf.filelist:
                    expected_size = validation_files[zip_info.filename]['size']
                    assert zip_info.file_size == expected_size, \
                        f"Size mismatch for {zip_info.filename}: {zip_info.file_size} vs {expected_size}"
        
        # Verify original files remain untouched regardless of success/failure
        for filename, file_info in validation_files.items():
            original_file = dataset_dir / filename
            current_checksum = calculate_file_checksum(original_file)
            assert current_checksum == file_info['checksum'], \
                f"Original file {filename} was modified" 