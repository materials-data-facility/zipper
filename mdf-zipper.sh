#!/bin/bash
# MDF Zipper Shell Wrapper
# Provides an easy way to run the MDF Zipper tool

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the Python script with all passed arguments
python3 "$SCRIPT_DIR/mdf_zipper.py" "$@" 