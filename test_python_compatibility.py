#!/usr/bin/env python3
"""
Python Version Compatibility Test for MDF Zipper

This test verifies that MDF Zipper works correctly across different Python versions,
particularly testing the zipfile.ZipFile compresslevel parameter compatibility.
"""

import sys
import tempfile
import zipfile
from pathlib import Path

from mdf_zipper import MDFZipper
from conftest import calculate_file_checksum


def test_zipfile_compatibility():
    """Test that zipfile creation works regardless of Python version."""
    print(f"Testing Python {sys.version}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        test_file = temp_path / "test.zip"
        
        # Test 1: Try with compresslevel (Python 3.7+)
        try:
            with zipfile.ZipFile(test_file, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                zf.writestr("test.txt", "test content")
            print("✅ compresslevel parameter supported")
            compresslevel_supported = True
        except TypeError:
            print("ℹ️  compresslevel parameter not supported (Python < 3.7)")
            compresslevel_supported = False
        
        # Test 2: Try without compresslevel (all Python versions)
        test_file2 = temp_path / "test2.zip"
        try:
            with zipfile.ZipFile(test_file2, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("test.txt", "test content")
            print("✅ Basic zipfile creation works")
        except Exception as e:
            print(f"❌ Basic zipfile creation failed: {e}")
            return False
        
        return True


def test_mdf_zipper_compatibility():
    """Test that MDF Zipper works correctly with current Python version."""
    print(f"\nTesting MDF Zipper compatibility with Python {sys.version}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test dataset
        dataset_dir = temp_path / "test_dataset"
        dataset_dir.mkdir()
        
        # Create test files
        for i in range(5):
            test_file = dataset_dir / f"file_{i}.txt"
            content = f"Test file {i} content\n" * 100
            test_file.write_text(content)
        
        # Record original checksums
        original_checksums = {}
        for file_path in dataset_dir.rglob('*.txt'):
            original_checksums[str(file_path)] = calculate_file_checksum(file_path)
        
        # Test MDF Zipper
        try:
            zipper = MDFZipper(max_size_gb=0.01, single_directory=True)
            results = zipper.process_directory(str(dataset_dir))
            
            print(f"✅ MDF Zipper processing completed successfully")
            print(f"   Processed: {results['processed']}")
            print(f"   Compressed: {results['compressed']}")
            
            # Verify original files are intact
            for file_path_str, original_checksum in original_checksums.items():
                current_checksum = calculate_file_checksum(Path(file_path_str))
                if current_checksum != original_checksum:
                    print(f"❌ File integrity check failed: {file_path_str}")
                    return False
            
            print("✅ All original files remain intact")
            
            # Verify archive was created
            archive_path = dataset_dir / ".mdf" / "dataset.zip"
            if archive_path.exists():
                print("✅ Archive created successfully")
                
                # Test archive validity
                if zipfile.is_zipfile(archive_path):
                    print("✅ Archive is a valid ZIP file")
                    
                    with zipfile.ZipFile(archive_path, 'r') as zf:
                        if zf.testzip() is None:
                            print("✅ Archive integrity test passed")
                        else:
                            print("❌ Archive integrity test failed")
                            return False
                else:
                    print("❌ Archive is not a valid ZIP file")
                    return False
            else:
                print("❌ Archive was not created")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ MDF Zipper test failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Run all compatibility tests."""
    print("=" * 60)
    print("MDF ZIPPER PYTHON COMPATIBILITY TEST")
    print("=" * 60)
    
    # Test basic zipfile compatibility
    if not test_zipfile_compatibility():
        print("\n❌ Basic zipfile compatibility test failed")
        sys.exit(1)
    
    # Test MDF Zipper compatibility
    if not test_mdf_zipper_compatibility():
        print("\n❌ MDF Zipper compatibility test failed")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("🎉 ALL COMPATIBILITY TESTS PASSED")
    print("=" * 60)
    print(f"MDF Zipper is compatible with Python {sys.version}")
    
    # Show version-specific features
    python_version = sys.version_info
    if python_version >= (3, 7):
        print("✅ Optimal compression level support (Python 3.7+)")
    else:
        print("ℹ️  Using default compression (Python < 3.7)")
    
    if python_version >= (3, 8):
        print("✅ Full pathlib support (Python 3.8+)")
    
    print("\n💡 For best performance, Python 3.7+ is recommended")


if __name__ == "__main__":
    main() 