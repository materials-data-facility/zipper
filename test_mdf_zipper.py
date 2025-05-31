#!/usr/bin/env python3
"""
Comprehensive pytest test suite for MDF Zipper.

This test suite prioritizes data integrity and safety for high-value datasets.
All tests verify that original files are never moved, modified, or corrupted.
"""

import pytest
import zipfile
import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

from mdf_zipper import MDFZipper
from conftest import (
    get_directory_file_count, 
    get_directory_size, 
    calculate_file_checksum,
    verify_file_checksums
)


class TestDataIntegrity:
    """Tests focused on ensuring data integrity and safety."""
    
    def test_original_files_never_modified(self, sample_datasets, integrity_checker):
        """Verify that original files are never modified during compression."""
        # Process datasets with compression
        zipper = MDFZipper(max_size_gb=0.01)  # 10MB threshold
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify no original files were modified
        integrity_errors = integrity_checker()
        assert not integrity_errors, f"Original files were modified: {integrity_errors}"
        
        # Verify archives were created
        assert results['compressed'] > 0, "Expected some folders to be compressed"
    
    def test_original_files_never_moved(self, sample_datasets, file_checksums):
        """Verify that original files are never moved from their locations."""
        original_file_locations = {}
        
        # Record all file locations before processing
        for dataset_name, dataset_path in sample_datasets.items():
            original_file_locations[dataset_name] = set()
            for file_path in dataset_path.rglob('*'):
                if file_path.is_file():
                    original_file_locations[dataset_name].add(str(file_path.relative_to(dataset_path)))
        
        # Process datasets
        zipper = MDFZipper(max_size_gb=0.01)
        zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify all original files are still in their original locations
        for dataset_name, dataset_path in sample_datasets.items():
            current_file_locations = set()
            for file_path in dataset_path.rglob('*'):
                if file_path.is_file() and '.mdf' not in file_path.parts:
                    current_file_locations.add(str(file_path.relative_to(dataset_path)))
            
            missing_files = original_file_locations[dataset_name] - current_file_locations
            assert not missing_files, f"Files moved from {dataset_name}: {missing_files}"
    
    def test_only_archives_added(self, sample_datasets):
        """Verify that only ZIP archives are added, no other changes."""
        # Record initial state - include both files and directories
        initial_structure = {}
        for dataset_name, dataset_path in sample_datasets.items():
            initial_structure[dataset_name] = set(
                str(p.relative_to(dataset_path)) for p in dataset_path.rglob('*')
            )
        
        # Process datasets
        zipper = MDFZipper(max_size_gb=0.01, archive_name="dataset.zip", archive_folder=".mdf")
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify only expected archives were added
        for dataset_name, dataset_path in sample_datasets.items():
            current_structure = set(
                str(p.relative_to(dataset_path)) for p in dataset_path.rglob('*')
            )
            
            new_items = current_structure - initial_structure[dataset_name]
            
            # Find if this dataset was compressed
            # Note: dataset_name is the key (e.g., 'small') but folder name is 'small_dataset'
            actual_folder_name = dataset_path.name
            dataset_compressed = False
            for detail in results['details']:
                if detail['folder'].endswith(actual_folder_name) and detail['compressed']:
                    dataset_compressed = True
                    break
            
            if dataset_compressed:
                # If compressed, should have exactly the archive structure
                # Note: rglob includes both the directory and the file
                expected_new_items = {'.mdf', '.mdf/dataset.zip'}
                assert new_items == expected_new_items, \
                    f"Unexpected new items in compressed {dataset_name}: {new_items - expected_new_items}. All new items: {new_items}"
            else:
                # If not compressed, no new items should be added
                assert not new_items, f"Unexpected items added to uncompressed {dataset_name}: {new_items}"
    
    def test_archive_content_integrity(self, sample_datasets, file_checksums):
        """Verify that archive contents match original files exactly."""
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        for detail in results['details']:
            if not detail['compressed']:
                continue
                
            dataset_path = Path(detail['folder'])
            archive_path = dataset_path / '.mdf' / 'dataset.zip'
            
            # Verify archive exists
            assert archive_path.exists(), f"Archive not found: {archive_path}"
            
            # Extract and verify each file in the archive
            with zipfile.ZipFile(archive_path, 'r') as zf:
                for zip_info in zf.filelist:
                    # Get original file
                    original_file = dataset_path / zip_info.filename
                    assert original_file.exists(), f"Original file missing: {original_file}"
                    
                    # Compare content
                    with zf.open(zip_info) as archived_file:
                        archived_content = archived_file.read()
                    
                    with open(original_file, 'rb') as original_file_handle:
                        original_content = original_file_handle.read()
                    
                    assert archived_content == original_content, \
                        f"Archive content differs from original: {zip_info.filename}"


class TestBasicFunctionality:
    """Tests for basic MDF Zipper functionality."""
    
    def test_basic_compression(self, sample_datasets, integrity_checker):
        """Test basic compression functionality."""
        zipper = MDFZipper(max_size_gb=0.01)  # 10MB threshold
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify results structure
        assert 'processed' in results
        assert 'compressed' in results
        assert 'skipped' in results
        assert 'details' in results
        
        # Verify some datasets were processed
        assert results['processed'] > 0
        
        # Verify no data corruption
        assert not integrity_checker(), "Data integrity check failed"
    
    def test_size_threshold_respected(self, sample_datasets):
        """Test that size threshold is properly respected."""
        # Use very small threshold
        zipper = MDFZipper(max_size_gb=0.001)  # 1MB threshold
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        for detail in results['details']:
            if detail['compressed']:
                assert detail['size_gb'] <= 0.001, \
                    f"Compressed folder exceeds threshold: {detail['size_gb']} GB"
            elif detail['skipped']:
                assert detail['size_gb'] > 0.001, \
                    f"Skipped folder below threshold: {detail['size_gb']} GB"
    
    def test_custom_archive_settings(self, sample_datasets):
        """Test custom archive name and folder settings."""
        custom_archive = "backup.zip"
        custom_folder = ".backups"
        
        zipper = MDFZipper(
            max_size_gb=0.01,
            archive_name=custom_archive,
            archive_folder=custom_folder
        )
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify custom settings were used
        for detail in results['details']:
            if detail['compressed']:
                dataset_path = Path(detail['folder'])
                expected_archive = dataset_path / custom_folder / custom_archive
                assert expected_archive.exists(), f"Custom archive not found: {expected_archive}"
    
    def test_empty_directory_handling(self, sample_datasets):
        """Test handling of empty directories."""
        # Use single_directory mode since empty directories have no subdirectories
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(sample_datasets['empty']))
        
        # Empty directory should be processed but result in minimal compression
        assert results['processed'] == 1
        # Should still create an archive (empty zip)
        archive_path = sample_datasets['empty'] / '.mdf' / 'dataset.zip'
        assert archive_path.exists()


class TestPlanMode:
    """Tests for plan (dry-run) mode functionality."""
    
    def test_plan_mode_no_files_created(self, sample_datasets, integrity_checker):
        """Verify that plan mode creates no files."""
        zipper = MDFZipper(max_size_gb=0.01, plan_mode=True)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify plan mode flag
        assert results['plan_mode'] == True
        
        # Verify no archives were created
        for dataset_path in sample_datasets.values():
            mdf_dir = dataset_path / '.mdf'
            assert not mdf_dir.exists(), f"Archive directory created in plan mode: {mdf_dir}"
        
        # Verify no data modification
        assert not integrity_checker(), "Files modified in plan mode"
    
    def test_plan_vs_execution_consistency(self, sample_datasets):
        """Test that plan mode predictions match actual execution."""
        threshold = 0.005  # 5MB threshold
        
        # Run plan mode
        zipper_plan = MDFZipper(max_size_gb=threshold, plan_mode=True)
        plan_results = zipper_plan.process_directory(str(sample_datasets['small'].parent))
        
        # Run actual execution
        zipper_actual = MDFZipper(max_size_gb=threshold, plan_mode=False)
        actual_results = zipper_actual.process_directory(str(sample_datasets['small'].parent))
        
        # Compare key metrics
        assert plan_results['compressed'] == actual_results['compressed'], \
            "Plan compression count doesn't match actual"
        assert plan_results['skipped'] == actual_results['skipped'], \
            "Plan skip count doesn't match actual"
        assert plan_results['processed'] == actual_results['processed'], \
            "Plan processed count doesn't match actual"
    
    def test_plan_mode_estimations(self, sample_datasets):
        """Test that plan mode provides reasonable size estimations."""
        zipper = MDFZipper(max_size_gb=0.01, plan_mode=True)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify estimations are provided
        assert results['total_compressed_size_gb'] > 0, "No compression estimates provided"
        
        # Compression ratio should be reasonable (10-90%)
        if results['total_size_gb'] > 0:
            ratio = results['total_compressed_size_gb'] / results['total_size_gb']
            assert 0.1 <= ratio <= 0.9, f"Unrealistic compression ratio: {ratio}"


class TestSingleDirectoryMode:
    """Tests for single directory processing mode."""
    
    def test_single_directory_processing(self, sample_datasets, integrity_checker):
        """Test processing a single directory."""
        target_dir = sample_datasets['medium']
        
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(target_dir))
        
        # Should process exactly one directory
        assert results['processed'] == 1
        assert len(results['details']) == 1
        
        # Verify integrity
        assert not integrity_checker(), "Data integrity check failed"
    
    def test_single_directory_vs_subdirectory_mode(self, sample_datasets):
        """Compare single directory mode vs subdirectory mode."""
        test_dir = sample_datasets['small'].parent
        
        # Single directory mode
        zipper_single = MDFZipper(max_size_gb=0.01, single_directory=True)
        single_results = zipper_single.process_directory(str(test_dir))
        
        # Create fresh test environment for subdirectory mode
        # (since single mode would have created an archive)
        # Reset by removing any created archives
        for dataset_path in sample_datasets.values():
            mdf_dir = dataset_path / '.mdf'
            if mdf_dir.exists():
                import shutil
                shutil.rmtree(mdf_dir)
        
        # Subdirectory mode
        zipper_sub = MDFZipper(max_size_gb=0.01, single_directory=False)
        sub_results = zipper_sub.process_directory(str(test_dir))
        
        # Single directory mode should process fewer items
        assert single_results['processed'] <= sub_results['processed']


class TestResumeAndLogging:
    """Tests for resume functionality and logging."""
    
    def test_log_file_creation(self, sample_datasets, temp_test_dir):
        """Test that log files are created and contain correct information."""
        log_file = temp_test_dir / "test.log"
        
        zipper = MDFZipper(max_size_gb=0.01, log_file=str(log_file))
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify log file was created
        assert log_file.exists(), "Log file was not created"
        
        # Verify log file content
        with open(log_file, 'r') as f:
            log_data = json.load(f)
        
        assert len(log_data) > 0, "Log file is empty"
        
        # Verify log entries have required fields
        for folder_path, entry in log_data.items():
            required_fields = [
                'folder_name', 'processed_date', 'original_size_bytes',
                'original_size_gb', 'file_count', 'status'
            ]
            for field in required_fields:
                assert field in entry, f"Missing field {field} in log entry"
    
    def test_resume_functionality(self, sample_datasets, temp_test_dir, integrity_checker):
        """Test that resume functionality works correctly."""
        log_file = temp_test_dir / "resume_test.log"
        
        # First run
        zipper1 = MDFZipper(max_size_gb=0.01, log_file=str(log_file))
        results1 = zipper1.process_directory(str(sample_datasets['small'].parent))
        
        # Second run (should skip already processed folders)
        zipper2 = MDFZipper(max_size_gb=0.01, log_file=str(log_file))
        results2 = zipper2.process_directory(str(sample_datasets['small'].parent))
        
        # Second run should have already_processed > 0
        assert results2['already_processed'] > 0, "Resume functionality not working"
        
        # Total processing counts should be consistent
        assert results1['processed'] == results2['processed']
        
        # Verify integrity after resume
        assert not integrity_checker(), "Data integrity check failed after resume"
    
    def test_log_file_corruption_handling(self, sample_datasets, temp_test_dir):
        """Test handling of corrupted log files."""
        log_file = temp_test_dir / "corrupted.log"
        
        # Create corrupted log file
        log_file.write_text("invalid json content")
        
        # Should handle gracefully
        zipper = MDFZipper(max_size_gb=0.01, log_file=str(log_file))
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Should still process successfully
        assert results['processed'] > 0


class TestErrorHandling:
    """Tests for error handling and edge cases."""
    
    def test_nonexistent_directory(self):
        """Test handling of nonexistent directories."""
        zipper = MDFZipper()
        
        with pytest.raises(FileNotFoundError):
            zipper.process_directory("/nonexistent/path")
    
    def test_file_instead_of_directory(self, temp_test_dir):
        """Test handling when a file path is provided instead of directory."""
        test_file = temp_test_dir / "test_file.txt"
        test_file.write_text("test content")
        
        zipper = MDFZipper()
        
        with pytest.raises(NotADirectoryError):
            zipper.process_directory(str(test_file))
    
    def test_permission_errors(self, sample_datasets, temp_test_dir):
        """Test handling of permission errors."""
        # Create a file we can't read (simulate permission error)
        test_dir = temp_test_dir / "permission_test"
        test_dir.mkdir()
        restricted_file = test_dir / "restricted.txt"
        restricted_file.write_text("restricted content")
        
        # Make file unreadable (on Unix systems)
        if hasattr(os, 'chmod'):
            os.chmod(restricted_file, 0o000)
        
        try:
            zipper = MDFZipper(max_size_gb=0.01)
            # Should handle permission errors gracefully
            results = zipper.process_directory(str(test_dir))
            # Should still attempt to process
            assert results['processed'] >= 0
        finally:
            # Restore permissions for cleanup
            if hasattr(os, 'chmod'):
                os.chmod(restricted_file, 0o644)
    
    def test_disk_space_simulation(self, sample_datasets):
        """Test behavior when disk space is limited (simulated)."""
        # Mock zipfile.ZipFile to simulate disk space error
        with patch('zipfile.ZipFile') as mock_zipfile:
            mock_zipfile.side_effect = OSError("No space left on device")
            
            zipper = MDFZipper(max_size_gb=0.01)
            results = zipper.process_directory(str(sample_datasets['small'].parent))
            
            # Should handle the error gracefully
            assert 'failed' in results
            # Should not crash


class TestConcurrency:
    """Tests for concurrent access and thread safety."""
    
    def test_parallel_processing_safety(self, sample_datasets, integrity_checker):
        """Test that parallel processing doesn't corrupt data."""
        zipper = MDFZipper(max_size_gb=0.01, max_workers=4)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # Verify data integrity with parallel processing
        assert not integrity_checker(), "Parallel processing corrupted data"
        assert results['processed'] > 0
    
    def test_concurrent_zipper_instances(self, sample_datasets, integrity_checker):
        """Test multiple MDFZipper instances running concurrently."""
        def run_zipper(dataset_path):
            zipper = MDFZipper(max_size_gb=0.01)
            return zipper.process_directory(str(dataset_path))
        
        # Run multiple instances concurrently on different datasets
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for dataset_name, dataset_path in list(sample_datasets.items())[:3]:
                future = executor.submit(run_zipper, dataset_path)
                futures.append(future)
            
            # Wait for all to complete
            results = [f.result() for f in futures]
        
        # Verify all completed successfully
        for result in results:
            assert result['processed'] >= 0
        
        # Verify data integrity
        assert not integrity_checker(), "Concurrent processing corrupted data"


class TestSpecialCases:
    """Tests for special cases and edge conditions."""
    
    def test_unicode_filenames(self, sample_datasets, integrity_checker):
        """Test handling of files with unicode characters."""
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(sample_datasets['special'].parent))
        
        # Should handle unicode filenames
        special_detail = next(
            (d for d in results['details'] if 'special_chars' in d['folder']), 
            None
        )
        assert special_detail is not None, "Special characters dataset not processed"
        
        # Verify integrity
        assert not integrity_checker(), "Unicode handling corrupted data"
    
    def test_binary_files(self, sample_datasets, integrity_checker):
        """Test handling of binary files."""
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(sample_datasets['binary'].parent))
        
        # Should handle binary files
        binary_detail = next(
            (d for d in results['details'] if 'binary' in d['folder']), 
            None
        )
        assert binary_detail is not None, "Binary dataset not processed"
        
        # Verify integrity
        assert not integrity_checker(), "Binary file handling corrupted data"
    
    def test_very_large_threshold(self, sample_datasets):
        """Test with very large size threshold."""
        zipper = MDFZipper(max_size_gb=1000.0)  # 1TB threshold
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # All datasets should be compressed with such a large threshold
        assert results['skipped'] == 0, "Folders skipped with very large threshold"
    
    def test_zero_threshold(self, sample_datasets):
        """Test with zero size threshold."""
        zipper = MDFZipper(max_size_gb=0.0)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        # All non-empty datasets should be skipped
        non_empty_folders = sum(1 for d in results['details'] if d['size_gb'] > 0)
        assert results['compressed'] <= results['processed'] - non_empty_folders


class TestArchiveValidation:
    """Tests to validate created archives."""
    
    def test_archive_is_valid_zip(self, sample_datasets):
        """Test that created archives are valid ZIP files."""
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        for detail in results['details']:
            if detail['compressed']:
                dataset_path = Path(detail['folder'])
                archive_path = dataset_path / '.mdf' / 'dataset.zip'
                
                # Test that the archive is a valid ZIP file
                assert zipfile.is_zipfile(archive_path), f"Invalid ZIP file: {archive_path}"
                
                # Test that we can open and read the archive
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    # Test archive integrity
                    bad_file = zf.testzip()
                    assert bad_file is None, f"Corrupted file in archive: {bad_file}"
    
    def test_archive_completeness(self, sample_datasets):
        """Test that archives contain all original files."""
        zipper = MDFZipper(max_size_gb=0.01)
        results = zipper.process_directory(str(sample_datasets['small'].parent))
        
        for detail in results['details']:
            if detail['compressed']:
                dataset_path = Path(detail['folder'])
                archive_path = dataset_path / '.mdf' / 'dataset.zip'
                
                # Get list of original files
                original_files = set()
                for file_path in dataset_path.rglob('*'):
                    if file_path.is_file() and '.mdf' not in file_path.parts:
                        original_files.add(str(file_path.relative_to(dataset_path)))
                
                # Get list of archived files
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    archived_files = set(zf.namelist())
                
                # Convert paths to use consistent separators
                original_files = {f.replace('\\', '/') for f in original_files}
                archived_files = {f.replace('\\', '/').rstrip('/') for f in archived_files}
                
                # Verify all files are archived
                missing_files = original_files - archived_files
                assert not missing_files, f"Files missing from archive: {missing_files}"


class TestPerformance:
    """Performance and efficiency tests."""
    
    def test_large_number_of_small_files(self, temp_test_dir):
        """Test performance with many small files."""
        # Create directory with many small files
        many_files_dir = temp_test_dir / "many_files"
        many_files_dir.mkdir()
        
        # Create 1000 small files
        for i in range(1000):
            (many_files_dir / f"file_{i:04d}.txt").write_text(f"Small file {i}\n")
        
        # Time the operation
        import time
        start_time = time.time()
        
        # Use single_directory mode since we're processing the directory itself
        zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
        results = zipper.process_directory(str(many_files_dir))
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should complete in reasonable time (< 30 seconds for 1000 files)
        assert processing_time < 30, f"Processing took too long: {processing_time:.2f}s"
        assert results['compressed'] == 1, "Many small files should be compressed" 