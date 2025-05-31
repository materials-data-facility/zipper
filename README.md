# MDF Zipper

A high-performance Python tool for automatically compressing subfolders based on size criteria. This tool recursively scans directories, calculates folder sizes, and creates compressed archives for folders that meet specified size thresholds.

## Features

- **Size-based compression**: Only compress folders smaller than a specified size threshold
- **Parallel processing**: Multi-threaded processing for optimal performance on large datasets
- **Non-destructive**: Original folders remain unchanged, archives are stored in separate subdirectories
- **Configurable**: Customizable archive names, folder names, and size thresholds
- **Robust error handling**: Graceful handling of permission errors and inaccessible files
- **Detailed logging**: Comprehensive logging with progress tracking and statistics
- **Cross-platform**: Works on Windows, macOS, and Linux
- **Single directory mode**: Option to process only the specified directory (not subdirectories)
- **Resume functionality**: Log file tracking prevents reprocessing already compressed folders
- **Compression statistics**: Detailed reporting of original vs compressed sizes and ratios
- **Plan mode**: Preview what would be processed without creating any archives (dry run)

## Requirements

- Python 3.7 or higher
- No external dependencies (uses only Python standard library)

## Installation

1. Clone or download this repository
2. Make the script executable (optional):
   ```bash
   chmod +x mdf_zipper.py
   ```

## Usage

### Basic Usage

```bash
python mdf_zipper.py /path/to/datasets
```

This will:
- Scan all subfolders in `/path/to/datasets`
- Compress folders smaller than 10 GB (default threshold)
- Create `dataset.zip` files in `.mdf` subdirectories

### Advanced Usage

```bash
# Set custom size threshold (5 GB)
python mdf_zipper.py ~/datasets/abcd --max-size 5.0

# Use custom archive name and folder
python mdf_zipper.py ~/datasets/abcd --archive-name "backup.zip" --archive-folder "archives"

# Increase parallel processing (8 workers)
python mdf_zipper.py ~/datasets/abcd --workers 8

# Enable verbose logging
python mdf_zipper.py ~/datasets/abcd --verbose

# Process only a single directory (not subdirectories)
python mdf_zipper.py ~/datasets/specific_folder --single-directory

# Use log file for resume functionality
python mdf_zipper.py ~/datasets/abcd --log-file "processing.log"

# Preview what would be processed (plan mode)
python mdf_zipper.py ~/datasets/abcd --plan

# Combine features for comprehensive processing
python mdf_zipper.py ~/datasets/abcd --max-size 5.0 --workers 8 --log-file "~/logs/processing.json" --verbose
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `directory` | Root directory to process | Required |
| `--max-size` | Maximum size in GB for compression | 10.0 |
| `--archive-name` | Name of the zip file to create | dataset.zip |
| `--archive-folder` | Folder name to store archives | .mdf |
| `--workers` | Number of parallel worker threads | 4 |
| `--verbose` | Enable verbose logging | False |
| `--single-directory` | Process only the specified directory | False |
| `--log-file` | Path to log file for resume functionality | None |
| `--plan` | Show what would be processed (dry run mode) | False |

## New Features

### Plan Mode (Dry Run)

Use `--plan` to preview what would be processed without creating any archives:

```bash
# See what would be compressed with current settings
python mdf_zipper.py ~/datasets/abcd --plan

# Preview with different threshold
python mdf_zipper.py ~/datasets/abcd --plan --max-size 2.0

# Plan single directory processing
python mdf_zipper.py ~/datasets/experiment1 --plan --single-directory

# Detailed plan with verbose output
python mdf_zipper.py ~/datasets/abcd --plan --verbose
```

**Plan mode shows:**
- Which folders would be compressed
- Which folders would be skipped (too large)
- Estimated compression ratios and space savings
- Total data size that would be processed
- Archive locations that would be created

**Benefits:**
- **Preview operations** before running on large datasets
- **Estimate storage requirements** for compressed archives
- **Validate settings** like size thresholds and folder selection
- **Safe exploration** of directory structures without modifications

### Single Directory Mode

Use `--single-directory` to process only the specified directory itself, rather than its subdirectories:

```bash
# Process only the contents of ~/datasets/experiment1 (not subdirectories)
python mdf_zipper.py ~/datasets/experiment1 --single-directory
```

### Resume Functionality

The `--log-file` option enables resume functionality by tracking processed folders:

```bash
# First run - processes all folders and saves log
python mdf_zipper.py ~/datasets/abcd --log-file "processing.log"

# Second run - skips already processed folders
python mdf_zipper.py ~/datasets/abcd --log-file "processing.log"
```

The log file contains detailed information about each processed folder:
- Processing timestamp
- Original and compressed sizes
- File counts and compression ratios
- Processing status (compressed/skipped/failed)

### Smart Resume Logic

The tool intelligently determines when to reprocess folders:
- **Archive missing**: If the ZIP file was deleted, the folder is reprocessed
- **Content changed**: If folder size changed since last processing, it's reprocessed
- **Settings changed**: Different archive names or folders trigger reprocessing

## How It Works

1. **Directory Scanning**: The tool scans the specified root directory for immediate subfolders
2. **Size Calculation**: For each subfolder, it recursively calculates the total size of all files
3. **Size Filtering**: Folders larger than the specified threshold are skipped
4. **Compression**: Qualifying folders are compressed into ZIP archives
5. **Archive Storage**: ZIP files are stored in a subdirectory within each processed folder

### Example Directory Structure

**Before processing:**
```
datasets/
├── small_dataset/          # 2 GB
│   ├── data1.txt
│   ├── data2.txt
│   └── subfolder/
│       └── data3.txt
├── medium_dataset/         # 8 GB
│   ├── images/
│   └── annotations/
└── large_dataset/          # 15 GB (exceeds 10 GB threshold)
    ├── videos/
    └── metadata/
```

**After processing:**
```
datasets/
├── small_dataset/          # 2 GB
│   ├── data1.txt
│   ├── data2.txt
│   ├── subfolder/
│   │   └── data3.txt
│   └── .mdf/
│       └── dataset.zip     # Contains all files from small_dataset/
├── medium_dataset/         # 8 GB
│   ├── images/
│   ├── annotations/
│   └── .mdf/
│       └── dataset.zip     # Contains all files from medium_dataset/
└── large_dataset/          # 15 GB (unchanged - too large)
    ├── videos/
    └── metadata/
```

## Performance Optimizations

The tool is optimized for large datasets with the following features:

1. **Parallel Processing**: Multiple folders are processed simultaneously using ThreadPoolExecutor
2. **Efficient Size Calculation**: Uses `os.walk()` for fast directory traversal
3. **Memory Efficient**: Processes files one at a time during compression
4. **Skip Logic**: Avoids processing archive folders to prevent infinite loops
5. **Compression Level**: Uses balanced compression (level 6) for good speed/size ratio

## Error Handling

The tool gracefully handles various error conditions:

- **Permission Errors**: Logs warnings for inaccessible files/folders and continues
- **Missing Directories**: Validates directory existence before processing
- **Disk Space**: ZIP creation failures are logged and don't stop other operations
- **Interruption**: Supports Ctrl+C for clean cancellation

## Logging

The tool provides detailed logging information:

- Folder processing progress
- Size calculations and file counts
- Compression status and results
- Error messages and warnings
- Final summary statistics

## Examples

### Preview before processing
```bash
# See what would happen with 2 GB threshold
python mdf_zipper.py ~/research/datasets --plan --max-size 2.0
```

### Process a dataset directory with 2 GB threshold
```bash
python mdf_zipper.py ~/research/datasets --max-size 2.0
```

### High-performance processing with 8 workers and logging
```bash
python mdf_zipper.py /data/experiments --workers 8 --verbose --log-file "processing.json"
```

### Custom archive configuration with resume capability
```bash
python mdf_zipper.py ~/projects --archive-name "project_backup.zip" --archive-folder "backups" --log-file "~/logs/projects.json"
```

### Single directory processing with preview
```bash
# First preview what would happen
python mdf_zipper.py ~/datasets/specific_experiment --plan --single-directory --max-size 1.0

# Then execute if satisfied with the plan
python mdf_zipper.py ~/datasets/specific_experiment --single-directory --max-size 1.0
```

## Output

The tool provides a comprehensive summary after processing, including detailed compression statistics:

**Normal Mode:**
```
============================================================
PROCESSING SUMMARY
============================================================
Total folders processed: 25
Folders compressed: 18
Folders skipped (too large): 5
Folders failed: 2
Folders already processed: 12
Total original data size: 127.45 GB
Total compressed data size: 32.18 GB
Overall compression ratio: 25.2%
Space saved: 95.27 GB
============================================================
```

### Compression Statistics

The tool now tracks and displays:

- **Original data size**: Total size of all processed folders
- **Compressed data size**: Total size of all created ZIP archives
- **Compression ratio**: Percentage of original size after compression
- **Space saved**: Amount of storage space saved through compression
- **Per-folder statistics**: Individual compression ratios for each processed folder
- **Already processed**: Count of folders skipped due to previous processing

This information helps you understand the effectiveness of compression for your specific datasets and make informed decisions about storage optimization.

## Log File Format

The log file is stored in JSON format with detailed information:

```json
{
  "/path/to/folder": {
    "folder_name": "experiment_data",
    "processed_date": "2025-05-29T13:38:35.934567",
    "original_size_bytes": 1073741824,
    "original_size_gb": 1.0,
    "file_count": 1500,
    "compressed_size_bytes": 268435456,
    "compressed_size_gb": 0.25,
    "compression_ratio": 25.0,
    "status": "compressed",
    "archive_path": "/path/to/folder/.mdf/dataset.zip"
  }
}
```

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## Support

If you encounter any issues or have questions, please create an issue in the repository.