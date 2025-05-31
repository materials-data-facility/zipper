#!/usr/bin/env python3

import zipfile
import tempfile
from pathlib import Path
from mdf_zipper import MDFZipper

# Create test data
temp_dir = Path(tempfile.mkdtemp())
test_dir = temp_dir / 'test'
test_dir.mkdir()
sub_dir = test_dir / 'dataset'
sub_dir.mkdir()
(sub_dir / 'data.txt').write_text('Test data\n' * 100)

# Create ZIP
zipper = MDFZipper(max_size_gb=0.01)
zipper.process_directory(str(test_dir))

archive_path = sub_dir / '.mdf' / 'dataset.zip'
print(f"Archive path: {archive_path}")
print(f"Archive exists: {archive_path.exists()}")

# Check original
print(f"Original is_zipfile: {zipfile.is_zipfile(archive_path)}")
with open(archive_path, 'rb') as f:
    header = f.read(20)
    print(f"Original header: {header}")

# Corrupt the file
with open(archive_path, 'r+b') as f:
    f.seek(0)
    f.write(b'CORRUPT!')

# Check corrupted
with open(archive_path, 'rb') as f:
    header = f.read(20)
    print(f"Corrupted header: {header}")

print(f"Corrupted is_zipfile: {zipfile.is_zipfile(archive_path)}")

# Try to open corrupted file
try:
    with zipfile.ZipFile(archive_path, 'r') as zf:
        print("Successfully opened corrupted ZIP")
        try:
            files = zf.namelist()
            print(f"Files in corrupted ZIP: {files}")
        except Exception as e:
            print(f"Error reading files: {e}")
        try:
            result = zf.testzip()
            print(f"testzip result: {result}")
        except Exception as e:
            print(f"testzip error: {e}")
except Exception as e:
    print(f"Error opening corrupted ZIP: {e}")

# Clean up
import shutil
shutil.rmtree(temp_dir) 