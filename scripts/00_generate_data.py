#!/usr/bin/env python3
"""Generate TPCH data using tpchgen-cli"""
import os
import subprocess
import sys
from pathlib import Path

import yaml


def get_config():
    """Load configuration from YAML file with environment variable overrides"""
    config_path = Path("config/tpch.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    config["scale"] = int(os.getenv("TPCH_SCALE", config["scale"]))
    config["parts"] = int(os.getenv("TPCH_PARTS", config["parts"]))
    tables_env = os.getenv("TPCH_TABLES")
    config["tables"] = [t.strip() for t in tables_env.split(",")] if tables_env else config["tables"]
    return config


def main():
    part_num = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "--part" and len(sys.argv) > 2:
            part_num = sys.argv[2]
        elif sys.argv[1].startswith("--part="):
            part_num = sys.argv[1].split("=")[1]
    
    config = get_config()
    
    print("TPCH Configuration:")
    print(f"  Scale Factor: {config['scale']}")
    print(f"  Parts: {config['parts']}")
    print(f"  Tables: {', '.join(config['tables'])}")
    print(f"  Output Directory: {config['output_dir']}")
    print(f"  Row Group Size: {config['parquet']['row_group_bytes']} bytes\n")
    
    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
    
    cmd = ["uv", "run", "tpchgen-cli", "-s", str(config["scale"]), "--tables", ",".join(config["tables"]),
           "--format", "parquet", "--parts", str(config["parts"]),
           "--parquet-row-group-bytes", str(config["parquet"]["row_group_bytes"]), "--output-dir", config["output_dir"]]
    
    if part_num:
        cmd.extend(["--part", part_num])
    
    subprocess.run(cmd, check=True)
    print(f"Generated TPCH ({', '.join(config['tables'])}) scale={config['scale']} parts={config['parts']} → {config['output_dir']}")


if __name__ == "__main__":
    main()

