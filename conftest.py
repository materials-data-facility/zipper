#!/usr/bin/env python3
"""
pytest configuration and fixtures for MDF Zipper tests.
"""

import pytest
import tempfile
import shutil
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Tuple
import os


@pytest.fixture
def temp_test_dir():
    """Create a temporary directory for testing that gets cleaned up automatically."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_datasets(temp_test_dir):
    """Create a variety of sample datasets for testing."""
    test_data = {}
    
    # Small dataset (< 1MB) - should be compressed
    small_dir = temp_test_dir / "small_dataset"
    small_dir.mkdir()
    (small_dir / "readme.txt").write_text("Small dataset documentation\n" * 100)
    (small_dir / "config.json").write_text('{"version": "1.0", "type": "small"}\n' * 50)
    
    # Create nested structure
    nested_dir = small_dir / "data" / "nested"
    nested_dir.mkdir(parents=True)
    (nested_dir / "file1.txt").write_text("Nested file content\n" * 200)
    (nested_dir / "file2.txt").write_text("More nested content\n" * 150)
    
    test_data['small'] = small_dir
    
    # Medium dataset (~2-3MB) - should be compressed
    medium_dir = temp_test_dir / "medium_dataset"
    medium_dir.mkdir()
    (medium_dir / "data.csv").write_text("id,value,timestamp\n" + "1,test,2023-01-01\n" * 15000)
    (medium_dir / "metadata.json").write_text('{"records": 15000, "type": "medium"}\n' * 1000)
    
    # Create multiple subdirectories
    for i in range(3):
        sub_dir = medium_dir / f"subset_{i}"
        sub_dir.mkdir()
        (sub_dir / f"data_{i}.txt").write_text(f"Subset {i} data\n" * 2000)
    
    test_data['medium'] = medium_dir
    
    # Large dataset (> 10MB when using low threshold) - should be skipped
    large_dir = temp_test_dir / "large_dataset"
    large_dir.mkdir()
    (large_dir / "big_file1.txt").write_text("Large file content\n" * 100000)
    (large_dir / "big_file2.txt").write_text("Another large file\n" * 150000)
    
    test_data['large'] = large_dir
    
    # Empty dataset
    empty_dir = temp_test_dir / "empty_dataset"
    empty_dir.mkdir()
    test_data['empty'] = empty_dir
    
    # Dataset with special characters and permissions
    special_dir = temp_test_dir / "special_chars_dataset"
    special_dir.mkdir()
    (special_dir / "file with spaces.txt").write_text("File with spaces\n" * 100)
    (special_dir / "file-with-dashes.txt").write_text("File with dashes\n" * 100)
    (special_dir / "file_with_unicode_ñáéíóú.txt").write_text("Unicode content ñáéíóú\n" * 100)
    
    test_data['special'] = special_dir
    
    # Binary-like data (simulated)
    binary_dir = temp_test_dir / "binary_dataset"
    binary_dir.mkdir()
    # Create files with binary-like content
    (binary_dir / "binary1.dat").write_bytes(b'\x00\x01\x02\x03' * 10000)
    (binary_dir / "binary2.dat").write_bytes(b'\xFF\xFE\xFD\xFC' * 8000)
    
    test_data['binary'] = binary_dir
    
    return test_data


@pytest.fixture
def file_checksums(sample_datasets):
    """Calculate checksums for all files in sample datasets to verify integrity."""
    checksums = {}
    
    for dataset_name, dataset_path in sample_datasets.items():
        checksums[dataset_name] = {}
        for file_path in dataset_path.rglob('*'):
            if file_path.is_file():
                rel_path = file_path.relative_to(dataset_path)
                checksums[dataset_name][str(rel_path)] = calculate_file_checksum(file_path)
    
    return checksums


def calculate_file_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception:
        return ""


def verify_file_checksums(checksums: Dict, datasets: Dict) -> List[str]:
    """Verify that file checksums haven't changed. Returns list of errors."""
    errors = []
    
    for dataset_name, dataset_path in datasets.items():
        if dataset_name not in checksums:
            continue
            
        for file_path in dataset_path.rglob('*'):
            if file_path.is_file():
                # Skip archive files
                if '.mdf' in file_path.parts:
                    continue
                    
                rel_path = file_path.relative_to(dataset_path)
                rel_path_str = str(rel_path)
                
                if rel_path_str not in checksums[dataset_name]:
                    errors.append(f"New file found: {dataset_name}/{rel_path_str}")
                    continue
                
                current_checksum = calculate_file_checksum(file_path)
                original_checksum = checksums[dataset_name][rel_path_str]
                
                if current_checksum != original_checksum:
                    errors.append(f"File modified: {dataset_name}/{rel_path_str}")
    
    return errors


@pytest.fixture
def integrity_checker(file_checksums, sample_datasets):
    """Provide a function to check file integrity after operations."""
    def check_integrity():
        return verify_file_checksums(file_checksums, sample_datasets)
    return check_integrity


def get_directory_file_count(path: Path) -> int:
    """Count all files in a directory recursively, excluding archives."""
    count = 0
    for file_path in path.rglob('*'):
        if file_path.is_file() and '.mdf' not in file_path.parts:
            count += 1
    return count


def get_directory_size(path: Path) -> int:
    """Get total size of directory in bytes, excluding archives."""
    total = 0
    for file_path in path.rglob('*'):
        if file_path.is_file() and '.mdf' not in file_path.parts:
            total += file_path.stat().st_size
    return total 