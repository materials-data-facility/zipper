# UNIX/Linux Specific Tests for MDF Zipper

## Overview

This document describes the comprehensive UNIX/Linux specific test suite added to MDF Zipper to ensure absolute safety for high-value datasets on UNIX and Linux systems. These tests address platform-specific filesystem features, signal handling, special file types, and edge cases that are unique to UNIX-like operating systems.

## Test Categories

### 1. TestUnixFilePermissions

Tests for UNIX file permission scenarios that could affect high-value datasets.

#### test_setuid_setgid_files
- **Purpose**: Test handling of setuid/setgid files without compromising security
- **Coverage**: Files with special execution bits (setuid, setgid)
- **Safety Check**: Verifies permissions aren't altered during compression
- **High-Value Dataset Protection**: Ensures sensitive executable files remain unchanged

#### test_files_with_no_permissions  
- **Purpose**: Test handling of files with no permissions (000)
- **Coverage**: Files that are completely inaccessible
- **Safety Check**: Graceful handling without corruption
- **High-Value Dataset Protection**: Protects restricted files from modification

#### test_sticky_bit_directories
- **Purpose**: Test handling of directories with sticky bit set
- **Coverage**: Directories with special permission bits (like /tmp)
- **Safety Check**: Directory permissions remain unchanged
- **High-Value Dataset Protection**: Preserves special directory security settings

#### test_different_owner_files
- **Purpose**: Test handling of files owned by different users
- **Coverage**: Multi-user environments, files owned by root/other users
- **Safety Check**: Files remain intact regardless of ownership
- **High-Value Dataset Protection**: Handles shared datasets safely

### 2. TestUnixSpecialFiles

Tests for UNIX special file types and filesystem features.

#### test_named_pipes_fifos
- **Purpose**: Test handling of named pipes (FIFOs)
- **Coverage**: Inter-process communication files
- **Safety Check**: FIFOs remain functional and unchanged
- **High-Value Dataset Protection**: Preserves special communication channels

#### test_device_files
- **Purpose**: Test handling of device files
- **Coverage**: Character and block device files, symlinks to /dev files
- **Safety Check**: Device files and regular files coexist safely
- **High-Value Dataset Protection**: Prevents device file corruption

#### test_hard_links
- **Purpose**: Test handling of hard links
- **Coverage**: Multiple filesystem entries pointing to same inode
- **Safety Check**: All hard link instances remain valid
- **High-Value Dataset Protection**: Preserves complex file relationships

#### test_sparse_files
- **Purpose**: Test handling of sparse files
- **Coverage**: Files with holes (common in databases, disk images)
- **Safety Check**: File size and sparseness preserved
- **High-Value Dataset Protection**: Maintains storage efficiency of large files

### 3. TestUnixSignalHandling

Tests for UNIX signal handling during compression.

#### test_sigterm_handling
- **Purpose**: Test graceful handling of SIGTERM signal
- **Coverage**: Process termination requests
- **Safety Check**: All files remain intact after signal interruption
- **High-Value Dataset Protection**: Prevents data loss during forced termination

#### test_sighup_handling
- **Purpose**: Test handling of SIGHUP signal (hangup)
- **Coverage**: Terminal disconnection scenarios
- **Safety Check**: Process continues or terminates cleanly
- **High-Value Dataset Protection**: Handles remote session disconnections safely

### 4. TestUnixFilesystemFeatures

Tests for UNIX filesystem-specific features.

#### test_case_sensitive_filesystem_edge_cases
- **Purpose**: Test edge cases on case-sensitive filesystems
- **Coverage**: Files with various case combinations
- **Safety Check**: All case variations preserved correctly
- **High-Value Dataset Protection**: Prevents case-related file conflicts

#### test_extended_attributes_xattr
- **Purpose**: Test handling of extended attributes
- **Coverage**: Filesystem metadata beyond standard attributes
- **Safety Check**: File content integrity maintained
- **High-Value Dataset Protection**: Preserves additional file metadata

#### test_mount_point_boundaries
- **Purpose**: Test behavior across filesystem mount point boundaries
- **Coverage**: Multiple filesystems, network mounts
- **Safety Check**: Files on all filesystems processed correctly
- **High-Value Dataset Protection**: Handles complex storage configurations

#### test_very_long_paths
- **Purpose**: Test handling of very long paths (approaching PATH_MAX)
- **Coverage**: Deeply nested directory structures
- **Safety Check**: Deep files processed correctly
- **High-Value Dataset Protection**: Handles complex academic/research directory structures

### 5. TestUnixNetworkFilesystems

Tests for network filesystem considerations.

#### test_nfs_like_latency_simulation
- **Purpose**: Test behavior with simulated network filesystem latency
- **Coverage**: Network attached storage, high-latency filesystems
- **Safety Check**: File integrity despite network delays
- **High-Value Dataset Protection**: Handles remote storage safely

#### test_stale_nfs_handle_simulation
- **Purpose**: Test handling of stale NFS handle errors
- **Coverage**: Network filesystem error conditions
- **Safety Check**: Graceful error handling
- **High-Value Dataset Protection**: Prevents corruption during network issues

### 6. TestUnixResourceLimits

Tests for UNIX resource limit scenarios.

#### test_file_descriptor_limits
- **Purpose**: Test behavior when approaching file descriptor limits
- **Coverage**: Large numbers of files, system resource constraints
- **Safety Check**: All files processed within resource limits
- **High-Value Dataset Protection**: Handles large datasets efficiently

#### test_ulimit_simulation
- **Purpose**: Test behavior under various ulimit constraints
- **Coverage**: CPU time limits, memory limits, other resource constraints
- **Safety Check**: Completes within resource limits
- **High-Value Dataset Protection**: Respects system resource policies

## Running UNIX/Linux Specific Tests

### Command Line Usage

```bash
# Run all UNIX/Linux specific tests
python run_tests.py --unix-linux

# Run with verbose output
python run_tests.py --unix-linux --verbose

# Run in parallel
python run_tests.py --unix-linux --parallel

# Run with coverage
python run_tests.py --unix-linux --coverage
```

### Test Results

The test runner provides specific feedback for UNIX/Linux platform verification:

```
🐧 UNIX/LINUX PLATFORM VERIFICATION:
   ✅ File permission handling
   ✅ Special file types (FIFOs, device files, hard links)
   ✅ Signal handling (SIGTERM, SIGHUP)
   ✅ Filesystem features (case sensitivity, extended attributes)
   ✅ Network filesystem simulation
   ✅ Resource limits and constraints
   ✅ UNIX-specific edge cases
```

### Platform Detection

The tests automatically skip on non-UNIX platforms:

```python
pytestmark = pytest.mark.skipif(
    sys.platform == "win32", 
    reason="UNIX/Linux specific tests not applicable on Windows"
)
```

## Critical Safety Guarantees for UNIX/Linux

### File System Integrity
- **Special File Types**: FIFOs, device files, and hard links are preserved
- **Permissions**: setuid, setgid, sticky bit, and other special permissions maintained
- **Extended Attributes**: Additional metadata preserved where possible
- **Sparse Files**: Storage efficiency maintained for large files with holes

### Signal Safety
- **SIGTERM**: Graceful handling of termination requests
- **SIGHUP**: Resilient to terminal disconnections
- **Process Interruption**: Clean recovery from any signal interruption

### Network Filesystem Support
- **NFS Compatibility**: Handles network filesystem latency and errors
- **Stale Handles**: Robust error recovery for network issues
- **Mount Points**: Correct behavior across filesystem boundaries

### Resource Management
- **File Descriptors**: Efficient handling of large file counts
- **Resource Limits**: Respects system ulimits and constraints
- **Memory Efficiency**: Handles large datasets within available resources

## Recommendations for UNIX/Linux Production Use

### Pre-Deployment Testing

1. **Run platform-specific tests**:
   ```bash
   python run_tests.py --unix-linux --verbose
   ```

2. **Test on target filesystem**:
   - NFS, CIFS, or other network filesystems
   - Local filesystems (ext4, xfs, btrfs, etc.)
   - Special mount options (nodev, nosuid, etc.)

3. **Verify with actual data patterns**:
   - Test with representative file types
   - Include sparse files if relevant
   - Test with actual permission structures

### Production Configuration

1. **Signal Handling**:
   ```bash
   # Install signal handlers for clean shutdown
   trap 'echo "Received SIGTERM, shutting down gracefully"' TERM
   ```

2. **Resource Monitoring**:
   ```bash
   # Monitor file descriptor usage
   ulimit -n
   
   # Monitor CPU time limits
   ulimit -t
   ```

3. **Network Filesystem Considerations**:
   - Use appropriate timeout settings
   - Monitor for stale handle errors
   - Consider local staging for network filesystems

## Summary

The UNIX/Linux specific test suite provides comprehensive coverage of platform-specific features that could affect high-value datasets. These tests ensure that MDF Zipper operates safely across the full spectrum of UNIX/Linux filesystem features, special file types, signal handling scenarios, and resource constraints.

**Total UNIX/Linux Specific Tests**: 18 tests across 6 test classes

All tests focus on maintaining the core safety guarantee: **original files are never modified, moved, or corrupted under any circumstances**, while properly handling the unique aspects of UNIX/Linux systems that could impact data processing operations. 