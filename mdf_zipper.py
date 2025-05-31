#!/usr/bin/env python3
"""
MDF Zipper - A tool for compressing subfolders based on size criteria.

This tool recursively scans directories, calculates folder sizes, and creates
compressed archives for folders that meet the size criteria.
"""

import os
import sys
import argparse
import zipfile
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass


@dataclass
class FolderInfo:
    """Information about a folder including its size and file count."""
    path: Path
    size_bytes: int
    file_count: int
    
    @property
    def size_gb(self) -> float:
        """Return size in gigabytes."""
        return self.size_bytes / (1024 ** 3)


class MDFZipper:
    """Main class for the MDF Zipper tool."""
    
    def __init__(self, max_size_gb: float = 10.0, archive_name: str = "dataset.zip", 
                 archive_folder: str = ".mdf", max_workers: int = 4, 
                 single_directory: bool = False, log_file: Optional[str] = None,
                 plan_mode: bool = False):
        """
        Initialize the MDF Zipper.
        
        Args:
            max_size_gb: Maximum size in GB for folders to be compressed
            archive_name: Name of the zip file to create
            archive_folder: Name of the folder to store the zip file
            max_workers: Maximum number of worker threads for parallel processing
            single_directory: If True, process only the specified directory (not subdirectories)
            log_file: Path to log file for tracking processed folders (optional)
            plan_mode: If True, only show what would be done without creating archives
        """
        self.max_size_gb = max_size_gb
        self.archive_name = archive_name
        self.archive_folder = archive_folder
        self.max_workers = max_workers
        self.single_directory = single_directory
        self.log_file = Path(log_file) if log_file else None
        self.plan_mode = plan_mode
        self.lock = threading.Lock()
        self.processed_log = {}
        
        # Setup logging first
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load existing log if it exists (but not in plan mode)
        if self.log_file and self.log_file.exists() and not self.plan_mode:
            self.load_processed_log()
    
    def calculate_folder_size(self, folder_path: Path) -> FolderInfo:
        """
        Calculate the total size of a folder and its contents.
        
        Args:
            folder_path: Path to the folder to analyze
            
        Returns:
            FolderInfo object with size and file count information
        """
        total_size = 0
        file_count = 0
        
        try:
            for dirpath, dirnames, filenames in os.walk(folder_path):
                # Skip the archive folder if it already exists
                if self.archive_folder in dirnames:
                    dirnames.remove(self.archive_folder)
                
                for filename in filenames:
                    file_path = Path(dirpath) / filename
                    try:
                        if file_path.exists() and not file_path.is_symlink():
                            total_size += file_path.stat().st_size
                            file_count += 1
                    except (OSError, PermissionError) as e:
                        self.logger.warning(f"Cannot access file {file_path}: {e}")
                        
        except (OSError, PermissionError) as e:
            self.logger.error(f"Cannot access folder {folder_path}: {e}")
            
        return FolderInfo(folder_path, total_size, file_count)
    
    def get_subfolders(self, root_path: Path) -> List[Path]:
        """
        Get all immediate subfolders in the root path.
        
        Args:
            root_path: Root directory to scan
            
        Returns:
            List of subfolder paths
        """
        subfolders = []
        
        try:
            for item in root_path.iterdir():
                if item.is_dir() and not item.name.startswith('.'):
                    subfolders.append(item)
        except (OSError, PermissionError) as e:
            self.logger.error(f"Cannot access directory {root_path}: {e}")
            
        return subfolders
    
    def create_zip_archive(self, folder_path: Path) -> Tuple[bool, int]:
        """
        Create a zip archive of the folder contents with atomic operation guarantee.
        
        Args:
            folder_path: Path to the folder to compress
            
        Returns:
            Tuple of (success, compressed_size_bytes)
        """
        archive_dir = folder_path / self.archive_folder
        archive_path = archive_dir / self.archive_name
        temp_archive_path = archive_path.with_suffix('.tmp')
        
        try:
            # Create the archive directory if it doesn't exist
            archive_dir.mkdir(exist_ok=True)
            
            # Check if archive already exists
            if archive_path.exists():
                # Verify existing archive is valid
                try:
                    with zipfile.ZipFile(archive_path, 'r') as zf:
                        if zf.testzip() is None:
                            self.logger.info(f"Valid archive already exists: {archive_path}")
                            compressed_size = archive_path.stat().st_size
                            return True, compressed_size
                        else:
                            self.logger.warning(f"Existing archive is corrupted, recreating: {archive_path}")
                            archive_path.unlink()  # Remove corrupted archive
                except zipfile.BadZipFile:
                    self.logger.warning(f"Existing archive is invalid, recreating: {archive_path}")
                    archive_path.unlink()  # Remove invalid archive
            
            self.logger.info(f"Creating archive: {archive_path}")
            
            # Create archive with temporary name first (atomic operation)
            # Use compresslevel only if supported (Python 3.7+)
            try:
                zipf = zipfile.ZipFile(temp_archive_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6)
            except TypeError:
                # Fallback for Python < 3.7 without compresslevel parameter
                zipf = zipfile.ZipFile(temp_archive_path, 'w', zipfile.ZIP_DEFLATED)
            
            with zipf:
                for root, dirs, files in os.walk(folder_path):
                    # Skip the archive folder itself
                    if self.archive_folder in dirs:
                        dirs.remove(self.archive_folder)
                    
                    for file in files:
                        file_path = Path(root) / file
                        
                        # Calculate relative path from the folder being zipped
                        try:
                            relative_path = file_path.relative_to(folder_path)
                            zipf.write(file_path, relative_path)
                        except (OSError, PermissionError) as e:
                            self.logger.warning(f"Cannot add file {file_path} to archive: {e}")
                        except ValueError as e:
                            # This can happen if the file is outside the folder being zipped
                            self.logger.warning(f"File path issue {file_path}: {e}")
            
            # Verify the temporary archive is valid before finalizing
            try:
                with zipfile.ZipFile(temp_archive_path, 'r') as zf:
                    if zf.testzip() is not None:
                        raise zipfile.BadZipFile("Archive failed integrity check")
            except Exception as e:
                # Clean up invalid temporary archive
                if temp_archive_path.exists():
                    temp_archive_path.unlink()
                raise e
            
            # Atomically move temporary archive to final location
            temp_archive_path.rename(archive_path)
            
            # Get the size of the created archive
            compressed_size = archive_path.stat().st_size
            
            self.logger.info(f"Successfully created archive: {archive_path}")
            self.logger.info(f"Compressed size: {compressed_size / (1024**2):.2f} MB")
            return True, compressed_size
            
        except Exception as e:
            self.logger.error(f"Failed to create archive for {folder_path}: {e}")
            
            # Clean up any partial files
            if temp_archive_path.exists():
                try:
                    temp_archive_path.unlink()
                    self.logger.debug(f"Cleaned up temporary archive: {temp_archive_path}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to clean up temporary archive {temp_archive_path}: {cleanup_error}")
            
            # Also clean up any corrupted final archive
            if archive_path.exists():
                try:
                    # Only remove if it's invalid/corrupted
                    with zipfile.ZipFile(archive_path, 'r') as zf:
                        if zf.testzip() is not None:
                            archive_path.unlink()
                            self.logger.debug(f"Cleaned up corrupted archive: {archive_path}")
                except zipfile.BadZipFile:
                    # Archive is completely invalid, remove it
                    archive_path.unlink()
                    self.logger.debug(f"Cleaned up invalid archive: {archive_path}")
                except Exception:
                    # Can't determine state, leave it for manual inspection
                    pass
            
            return False, 0
    
    def process_folder(self, folder_path: Path) -> Tuple[Path, bool, FolderInfo, int]:
        """
        Process a single folder: calculate size and create archive if needed.
        
        Args:
            folder_path: Path to the folder to process
            
        Returns:
            Tuple of (folder_path, success, folder_info, compressed_size_bytes)
        """
        folder_info = self.calculate_folder_size(folder_path)
        
        with self.lock:
            if self.plan_mode:
                if folder_info.size_gb <= self.max_size_gb:
                    self.logger.info(
                        f"WOULD COMPRESS: {folder_path.name} | "
                        f"Size: {folder_info.size_gb:.2f} GB | "
                        f"Files: {folder_info.file_count} | "
                        f"Archive: {folder_path / self.archive_folder / self.archive_name}"
                    )
                else:
                    self.logger.info(
                        f"WOULD SKIP: {folder_path.name} | "
                        f"Size: {folder_info.size_gb:.2f} GB | "
                        f"Files: {folder_info.file_count} | "
                        f"Reason: Exceeds {self.max_size_gb} GB threshold"
                    )
            else:
                self.logger.info(
                    f"Folder: {folder_path.name} | "
                    f"Size: {folder_info.size_gb:.2f} GB | "
                    f"Files: {folder_info.file_count}"
                )
        
        if folder_info.size_gb <= self.max_size_gb:
            if self.plan_mode:
                # In plan mode, simulate successful compression
                # Estimate compressed size (rough estimate: 30% of original for mixed content)
                estimated_compressed_size = int(folder_info.size_bytes * 0.3)
                return folder_path, True, folder_info, estimated_compressed_size
            else:
                # Normal processing
                success, compressed_size = self.create_zip_archive(folder_path)
                status = 'compressed' if success else 'failed'
                
                # Log the processing result
                self.log_processed_folder(folder_path, folder_info, compressed_size, status)
                
                return folder_path, success, folder_info, compressed_size
        else:
            if not self.plan_mode:
                with self.lock:
                    self.logger.info(
                        f"Skipping {folder_path.name} - exceeds size limit "
                        f"({folder_info.size_gb:.2f} GB > {self.max_size_gb} GB)"
                    )
                
                # Log as skipped
                self.log_processed_folder(folder_path, folder_info, 0, 'skipped')
            
            return folder_path, False, folder_info, 0
    
    def process_directory(self, root_path: str) -> Dict[str, any]:
        """
        Process all subfolders in the root directory, or the directory itself if single_directory mode.
        
        Args:
            root_path: Root directory path to process
            
        Returns:
            Dictionary with processing results and statistics
        """
        root_path = Path(root_path).expanduser().resolve()
        
        if not root_path.exists():
            raise FileNotFoundError(f"Directory does not exist: {root_path}")
        
        if not root_path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {root_path}")
        
        mode_str = "PLAN MODE - " if self.plan_mode else ""
        self.logger.info(f"{mode_str}Processing directory: {root_path}")
        self.logger.info(f"Max size threshold: {self.max_size_gb} GB")
        self.logger.info(f"Single directory mode: {self.single_directory}")
        
        if self.single_directory:
            # Process only the specified directory itself
            folders_to_process = [root_path]
        else:
            # Process all subfolders
            folders_to_process = self.get_subfolders(root_path)
        
        if not folders_to_process:
            self.logger.warning("No folders found to process")
            return {
                'processed': 0,
                'compressed': 0,
                'skipped': 0,
                'failed': 0,
                'already_processed': 0,
                'total_size_gb': 0.0,
                'total_compressed_size_gb': 0.0,
                'plan_mode': self.plan_mode
            }
        
        self.logger.info(f"Found {len(folders_to_process)} folder(s) to process")
        
        # Check which folders are already processed (skip in plan mode)
        already_processed_count = 0
        folders_needing_processing = []
        
        if not self.plan_mode:
            for folder in folders_to_process:
                if self.is_already_processed(folder):
                    already_processed_count += 1
                    self.logger.info(f"Folder {folder.name} already processed, skipping")
                else:
                    folders_needing_processing.append(folder)
        else:
            # In plan mode, process all folders to show what would happen
            folders_needing_processing = folders_to_process
        
        results = {
            'processed': len(folders_to_process),
            'compressed': 0,
            'skipped': 0,
            'failed': 0,
            'already_processed': already_processed_count,
            'total_size_gb': 0.0,
            'total_compressed_size_gb': 0.0,
            'plan_mode': self.plan_mode,
            'details': []
        }
        
        # Add already processed folders to results (skip in plan mode)
        if not self.plan_mode:
            for folder in folders_to_process:
                if self.is_already_processed(folder):
                    folder_key = str(folder.resolve())
                    log_entry = self.processed_log[folder_key]
                    results['total_size_gb'] += log_entry['original_size_gb']
                    if log_entry['status'] == 'compressed':
                        results['compressed'] += 1
                        results['total_compressed_size_gb'] += log_entry['compressed_size_gb']
                    
                    results['details'].append({
                        'folder': str(folder),
                        'size_gb': log_entry['original_size_gb'],
                        'file_count': log_entry['file_count'],
                        'compressed': log_entry['status'] == 'compressed',
                        'skipped': log_entry['status'] == 'skipped',
                        'compressed_size_gb': log_entry['compressed_size_gb'],
                        'compression_ratio': log_entry['compression_ratio']
                    })
        
        # Process folders that need processing in parallel
        if folders_needing_processing:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_folder = {
                    executor.submit(self.process_folder, folder): folder 
                    for folder in folders_needing_processing
                }
                
                for future in as_completed(future_to_folder):
                    folder_path, success, folder_info, compressed_size = future.result()
                    
                    results['total_size_gb'] += folder_info.size_gb
                    
                    if folder_info.size_gb > self.max_size_gb:
                        results['skipped'] += 1
                    elif success:
                        results['compressed'] += 1
                        results['total_compressed_size_gb'] += compressed_size / (1024 ** 3)
                    else:
                        results['failed'] += 1
                    
                    results['details'].append({
                        'folder': str(folder_path),
                        'size_gb': folder_info.size_gb,
                        'file_count': folder_info.file_count,
                        'compressed': success and folder_info.size_gb <= self.max_size_gb,
                        'skipped': folder_info.size_gb > self.max_size_gb,
                        'compressed_size_gb': compressed_size / (1024 ** 3) if success else 0.0,
                        'compression_ratio': (compressed_size / folder_info.size_bytes * 100) if success and folder_info.size_bytes > 0 else 0.0
                    })
        
        # Save the processed log (skip in plan mode)
        if not self.plan_mode:
            self.save_processed_log()
        
        return results
    
    def load_processed_log(self):
        """Load the processed folders log from file."""
        try:
            with open(self.log_file, 'r') as f:
                self.processed_log = json.load(f)
            self.logger.info(f"Loaded processed log with {len(self.processed_log)} entries")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.warning(f"Could not load processed log: {e}")
            self.processed_log = {}
    
    def save_processed_log(self):
        """Save the processed folders log to file."""
        if not self.log_file:
            return
        
        try:
            # Ensure the log file directory exists
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.log_file, 'w') as f:
                json.dump(self.processed_log, f, indent=2, default=str)
            self.logger.info(f"Saved processed log to {self.log_file}")
        except Exception as e:
            self.logger.error(f"Could not save processed log: {e}")
    
    def is_already_processed(self, folder_path: Path) -> bool:
        """Check if a folder has already been successfully processed."""
        folder_key = str(folder_path.resolve())
        
        if folder_key not in self.processed_log:
            return False
        
        log_entry = self.processed_log[folder_key]
        
        # Check if the archive still exists
        archive_path = folder_path / self.archive_folder / self.archive_name
        if not archive_path.exists():
            # Archive was deleted, need to reprocess
            del self.processed_log[folder_key]
            return False
        
        # Check if folder contents have changed since last processing
        current_size = self.calculate_folder_size(folder_path).size_bytes
        if log_entry.get('original_size_bytes', 0) != current_size:
            # Folder size changed, need to reprocess
            self.logger.info(f"Folder {folder_path.name} size changed, will reprocess")
            return False
        
        return log_entry.get('status') == 'compressed'
    
    def log_processed_folder(self, folder_path: Path, folder_info: FolderInfo, 
                           compressed_size: int, status: str):
        """Log a processed folder to the tracking log."""
        folder_key = str(folder_path.resolve())
        
        self.processed_log[folder_key] = {
            'folder_name': folder_path.name,
            'processed_date': datetime.now().isoformat(),
            'original_size_bytes': folder_info.size_bytes,
            'original_size_gb': folder_info.size_gb,
            'file_count': folder_info.file_count,
            'compressed_size_bytes': compressed_size,
            'compressed_size_gb': compressed_size / (1024 ** 3),
            'compression_ratio': (compressed_size / folder_info.size_bytes * 100) if folder_info.size_bytes > 0 else 0,
            'status': status,
            'archive_path': str(folder_path / self.archive_folder / self.archive_name)
        }


def main():
    """Main entry point for the MDF Zipper tool."""
    parser = argparse.ArgumentParser(
        description="MDF Zipper - Compress subfolders based on size criteria",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ~/datasets/abcd
  %(prog)s ~/datasets/abcd --max-size 5.0
  %(prog)s ~/datasets/abcd --max-size 2.5 --archive-name "backup.zip"
  %(prog)s ~/datasets/abcd --workers 8
  %(prog)s ~/datasets/abcd --single-directory --log-file "processing.log"
  %(prog)s ~/datasets/abcd --log-file "~/logs/mdf_processing.json"
  %(prog)s ~/datasets/abcd --plan --max-size 2.0
  %(prog)s ~/datasets/abcd --plan --single-directory --verbose
        """
    )
    
    parser.add_argument(
        'directory',
        help='Root directory to process'
    )
    
    parser.add_argument(
        '--max-size',
        type=float,
        default=10.0,
        help='Maximum size in GB for folders to be compressed (default: 10.0)'
    )
    
    parser.add_argument(
        '--archive-name',
        default='dataset.zip',
        help='Name of the zip file to create (default: dataset.zip)'
    )
    
    parser.add_argument(
        '--archive-folder',
        default='.mdf',
        help='Name of the folder to store the zip file (default: .mdf)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of worker threads for parallel processing (default: 4)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--single-directory',
        action='store_true',
        help='Process only the specified directory (not its subdirectories)'
    )
    
    parser.add_argument(
        '--log-file',
        help='Path to log file for tracking processed folders (enables resume functionality)'
    )
    
    parser.add_argument(
        '--plan',
        action='store_true',
        help='Show what would be processed without creating any archives (dry run mode)'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        zipper = MDFZipper(
            max_size_gb=args.max_size,
            archive_name=args.archive_name,
            archive_folder=args.archive_folder,
            max_workers=args.workers,
            single_directory=args.single_directory,
            log_file=args.log_file,
            plan_mode=args.plan
        )
        
        results = zipper.process_directory(args.directory)
        
        # Print summary
        plan_prefix = "EXECUTION PLAN - " if results.get('plan_mode') else ""
        print("\n" + "="*60)
        print(f"{plan_prefix}PROCESSING SUMMARY")
        print("="*60)
        
        if results.get('plan_mode'):
            print("🔍 DRY RUN MODE - No files will be created")
            print(f"📁 Total folders that would be processed: {results['processed']}")
            print(f"✅ Folders that would be compressed: {results['compressed']}")
            print(f"⏭️  Folders that would be skipped (too large): {results['skipped']}")
            print(f"📊 Total original data size: {results['total_size_gb']:.2f} GB")
            print(f"🗜️  Estimated compressed data size: {results['total_compressed_size_gb']:.2f} GB")
        else:
            print(f"Total folders processed: {results['processed']}")
            print(f"Folders compressed: {results['compressed']}")
            print(f"Folders skipped (too large): {results['skipped']}")
            print(f"Folders failed: {results['failed']}")
            print(f"Folders already processed: {results['already_processed']}")
            print(f"Total original data size: {results['total_size_gb']:.2f} GB")
            print(f"Total compressed data size: {results['total_compressed_size_gb']:.2f} GB")
        
        # Calculate overall compression ratio
        if results['total_compressed_size_gb'] > 0 and results['total_size_gb'] > 0:
            # Only calculate ratio for compressed folders
            compressed_original_size = sum(
                detail['size_gb'] for detail in results['details'] 
                if detail['compressed']
            )
            if compressed_original_size > 0:
                overall_ratio = (results['total_compressed_size_gb'] / compressed_original_size) * 100
                space_saved = compressed_original_size - results['total_compressed_size_gb']
                if results.get('plan_mode'):
                    print(f"📈 Estimated compression ratio: {overall_ratio:.1f}%")
                    print(f"💾 Estimated space saved: {space_saved:.2f} GB")
                else:
                    print(f"Overall compression ratio: {overall_ratio:.1f}%")
                    print(f"Space saved: {space_saved:.2f} GB")
        
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 