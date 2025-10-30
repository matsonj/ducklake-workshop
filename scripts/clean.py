#!/usr/bin/env python3
"""Remove all generated data and catalog"""
import shutil
from pathlib import Path


def main():
    print("Removing all generated data and catalog...")
    
    paths_to_remove = [
        "data/tpch",
        "data/lake",
        "catalog/ducklake.ducklake",
        "catalog/ducklake.ducklake.files"
    ]
    
    for path_str in paths_to_remove:
        path = Path(path_str)
        if path.exists():
            if path.is_file():
                path.unlink()
                print(f"Removed: {path_str}")
            else:
                shutil.rmtree(path)
                print(f"Removed: {path_str}")
    
    print("Cleanup completed!")


if __name__ == "__main__":
    main()

