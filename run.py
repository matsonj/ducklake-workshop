#!/usr/bin/env python3
"""
DuckLake TPCH Demo - Simple Python CLI
Replaces Makefile with cross-platform Python scripts
"""
import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import yaml


def print_info(msg):
    """Simple print function for user messages"""
    print(msg)


def check_command(cmd):
    """Check if a command exists in PATH"""
    return shutil.which(cmd) is not None


def get_config():
    """Load configuration from YAML file with environment variable overrides"""
    config_path = Path("config/tpch.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Override with environment variables if present
    config["scale"] = int(os.getenv("TPCH_SCALE", config["scale"]))
    config["parts"] = int(os.getenv("TPCH_PARTS", config["parts"]))
    tables_env = os.getenv("TPCH_TABLES")
    if tables_env:
        config["tables"] = [t.strip() for t in tables_env.split(",")]
    else:
        config["tables"] = config["tables"]
    
    return config


def execute_sql_file(sql_file, variables=None, conn=None):
    """Execute a SQL file using DuckDB"""
    sql_path = Path(f"scripts/{sql_file}")
    if not sql_path.exists():
        print_info(f"Error: SQL file not found: {sql_path}")
        sys.exit(1)
    
    with open(sql_path) as f:
        sql = f.read()
    
    # Replace variables if provided
    if variables:
        for key, value in variables.items():
            # Replace __KEY__ placeholders
            # Special handling for TABLE - add lake. prefix if it's a table name
            if key == "TABLE":
                sql = sql.replace(f"__{key}__", f"lake.{value}")
            else:
                sql = sql.replace(f"__{key}__", str(value))
    
    # Use provided connection or create a new one
    # Track whether connection was provided (vs created) to know if we should skip setup statements
    conn_was_provided = conn is not None
    should_close = False
    if conn is None:
        conn = duckdb.connect()
        should_close = True
    
    try:
        # First, remove comment lines and inline comments
        # Split by newline, remove lines that are comments
        cleaned_lines = []
        for line in sql.split('\n'):
            stripped = line.strip()
            # Skip empty lines and lines that are entirely comments
            if not stripped or stripped.startswith('--'):
                continue
            # Handle inline comments - remove everything after --
            if '--' in line:
                # Find the -- that's not in a string
                comment_pos = line.find('--')
                cleaned_line = line[:comment_pos].rstrip()
                if cleaned_line.strip():
                    cleaned_lines.append(cleaned_line)
            else:
                cleaned_lines.append(line)
        
        # Now join back and split by semicolon
        cleaned_sql = '\n'.join(cleaned_lines)
        raw_statements = cleaned_sql.split(';')
        
        # Only skip INSTALL/LOAD/ATTACH/USE statements if connection was PROVIDED
        # (meaning it was already set up). If we just created it, we need to run these.
        if conn_was_provided:
            filtered_statements = []
            for stmt in raw_statements:
                stmt_upper = stmt.strip().upper()
                # Skip INSTALL, LOAD, ATTACH, and USE statements if connection already provided
                if (stmt_upper.startswith('INSTALL') or 
                    stmt_upper.startswith('LOAD') or 
                    stmt_upper.startswith('ATTACH') or
                    stmt_upper.startswith('USE')):
                    continue
                filtered_statements.append(stmt)
            raw_statements = filtered_statements
        
        for raw_stmt in raw_statements:
            statement = raw_stmt.strip()
            
            # Skip empty statements
            if not statement:
                continue
            
            try:
                result = conn.execute(statement)
                # Try to get results (for SELECT statements)
                try:
                    results = result.fetchall()
                    if results:
                        # Check if this is an info/header statement (single column named 'info')
                        try:
                            description = result.description()
                            columns = [col[0] for col in description] if description else []
                            
                            if columns and len(columns) == 1 and columns[0].lower() == 'info':
                                info_text = str(results[0][0])
                                if info_text.startswith('==='):
                                    print_info("")
                                    print_info(info_text)
                                    print_info("")
                                else:
                                    print_info(info_text)
                            elif columns and len(columns) == 1 and columns[0].lower() == 'status':
                                status_text = str(results[0][0])
                                print_info(status_text)
                                print_info("")
                            else:
                                if columns:
                                    print_info(" | ".join(str(col) for col in columns))
                                    print_info("-" * min(80, max(len(" | ".join(str(col) for col in columns)), 40)))
                                for row in results:
                                    print_info(" | ".join(str(cell) for cell in row))
                                print_info("")
                        except Exception as e:
                            # Fallback: print results without headers
                            for row in results:
                                print_info(" | ".join(str(cell) for cell in row))
                            print_info("")
                except Exception as fetch_error:
                    # Check if this is a statement that doesn't return results
                    error_msg = str(fetch_error)
                    if "does not return a result set" not in error_msg.lower():
                        pass  # Silent - this is expected for non-SELECT statements
                    pass
            except Exception as e:
                # Some statements might fail (e.g., CREATE TABLE IF NOT EXISTS when table exists)
                error_msg = str(e)
                # Don't suppress ATTACH errors - these are critical
                if "failed to attach" in error_msg.lower() and "no such file or directory" in error_msg.lower():
                    # This is likely because the directory doesn't exist - but we create it in cmd_catalog
                    # So this shouldn't happen, but if it does, raise it
                    print_info(f"Error executing SQL: {error_msg}")
                    raise
                # Check if it's a "database not found" error - might need to re-execute USE
                if "failed to find attached database" in error_msg.lower():
                    # Try to re-execute USE if we're in a context where lake should be available
                    if "lake" in error_msg.lower():
                        try:
                            conn.execute("USE lake;")
                            # Retry the statement
                            result = conn.execute(statement)
                            try:
                                results = result.fetchall()
                                if results:
                                    try:
                                        description = result.description()
                                        columns = [col[0] for col in description] if description else []
                                        if columns and len(columns) > 1 or (columns and columns[0].lower() not in ['info', 'status']):
                                            if columns:
                                                print_info(" | ".join(str(col) for col in columns))
                                                print_info("-" * min(80, max(len(" | ".join(str(col) for col in columns)), 40)))
                                            for row in results:
                                                print_info(" | ".join(str(cell) for cell in row))
                                            print_info("")
                                    except:
                                        pass
                            except:
                                pass
                        except:
                            pass
                
                if "already exists" not in error_msg.lower() and "does not exist" not in error_msg.lower():
                    if "failed to find attached database" not in error_msg.lower():
                        print_info(f"Error executing SQL: {error_msg}")
                        raise
    finally:
        if should_close:
            conn.close()


def cmd_setup(args):
    """Install dependencies and verify environment"""
    print_info("Setting up DuckLake TPCH demo...")
    
    # Check for uv
    if not check_command("uv"):
        print_info("Error: uv not found. Please install uv:")
        print_info("  curl -LsSf https://astral.sh/uv/install.sh | sh")
        sys.exit(1)
    
    # Check for duckdb
    if not check_command("duckdb"):
        print_info("Error: duckdb not found. Please install DuckDB and ensure it's in your PATH.")
        sys.exit(1)
    
    # Set up Python virtual environment
    print_info("Setting up Python virtual environment...")
    if not Path(".venv").exists():
        subprocess.run(["uv", "venv"], check=True)
    
    subprocess.run(["uv", "sync"], check=True)
    
    # Verify Python dependencies
    print_info("Checking tpchgen-cli...")
    try:
        subprocess.run(["uv", "run", "tpchgen-cli", "--version"], 
                      check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print_info("Error: tpchgen-cli not available via uv run")
        print_info("Try running: uv sync")
        sys.exit(1)
    
    print_info("Checking yq...")
    try:
        subprocess.run(["uv", "run", "yq", "--version"], 
                      check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print_info("Error: yq not available via uv run")
        print_info("Try running: uv sync")
        sys.exit(1)
    
    # Verify DuckLake extension
    print_info("Checking DuckLake extension...")
    try:
        conn = duckdb.connect()
        conn.execute("INSTALL ducklake;")
        conn.execute("LOAD ducklake;")
        conn.execute("SELECT 'DuckLake extension loaded successfully';")
        conn.close()
    except Exception as e:
        print_info(f"Warning: Could not load DuckLake extension: {e}")
        print_info("Make sure DuckDB 1.3.0+ is installed and in your PATH.")
        sys.exit(1)
    
    # Check disk space (basic check)
    stat = shutil.disk_usage(".")
    free_gb = stat.free / (1024**3)
    if free_gb < 2:
        print_info(f"Error: Not enough disk space (~2GB required). Available: {free_gb:.1f}GB")
        sys.exit(1)
    
    print_info("Preflight checks passed.")


def cmd_tpch(args):
    """Generate TPCH data"""
    config = get_config()
    
    print_info("TPCH Configuration:")
    print_info(f"  Scale Factor: {config['scale']}")
    print_info(f"  Parts: {config['parts']}")
    print_info(f"  Tables: {', '.join(config['tables'])}")
    print_info(f"  Output Directory: {config['output_dir']}")
    print_info(f"  Row Group Size: {config['parquet']['row_group_bytes']} bytes")
    print_info("")
    
    # Create output directory
    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
    
    # Build tpchgen-cli command
    cmd = [
        "uv", "run", "tpchgen-cli",
        "-s", str(config["scale"]),
        "--tables", ",".join(config["tables"]),
        "--format", "parquet",
        "--parts", str(config["parts"]),
        "--parquet-row-group-bytes", str(config["parquet"]["row_group_bytes"]),
        "--output-dir", config["output_dir"]
    ]
    
    if args.part:
        cmd.extend(["--part", str(args.part)])
    
    subprocess.run(cmd, check=True)
    
    print_info(f"Generated TPCH ({', '.join(config['tables'])}) scale={config['scale']} parts={config['parts']} → {config['output_dir']}")


def cmd_catalog(args):
    """Initialize DuckLake catalog"""
    print_info("Initializing DuckLake catalog...")
    Path("catalog").mkdir(exist_ok=True)
    print_info("Creating catalog directory and tables...")
    execute_sql_file("bootstrap_catalog.sql")
    print_info("Catalog initialized successfully!")


def cmd_repartition(args):
    """Repartition orders table"""
    print_info("Repartitioning orders table...")
    print_info("Copying data from orders_raw to partitioned orders table...")
    execute_sql_file("repartition_orders.sql")
    print_info("Repartitioning completed successfully!")


def cmd_load_small_files(args):
    """Load Parquet files one at a time to create small files"""
    table = args.table or "lineitem"
    data_dir = Path(f"data/tpch/{table}")
    catalog_db = "catalog/ducklake.ducklake"
    
    print_info(f"Loading small files for table: {table}")
    print_info(f"Reading from: {data_dir}")
    
    if not data_dir.exists():
        print_info(f"Error: Directory {data_dir} does not exist.")
        print_info("Please generate TPCH data first with: uv run python run.py tpch")
        sys.exit(1)
    
    if not Path(catalog_db).exists():
        print_info(f"Error: DuckLake catalog not found at {catalog_db}")
        print_info("Please initialize catalog first with: uv run python run.py catalog")
        sys.exit(1)
    
    # Get list of parquet files
    parquet_files = sorted(data_dir.glob("*.parquet"), key=lambda p: int(p.stem.split(".")[-1]) if p.stem.split(".")[-1].isdigit() else 0)
    
    if not parquet_files:
        print_info(f"Error: No Parquet files found in {data_dir}")
        sys.exit(1)
    
    file_count = len(parquet_files)
    print_info(f"Found {file_count} Parquet files to load")
    
    # Initialize DuckLake connection
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake;")
    conn.execute("LOAD ducklake;")
    conn.execute(f"ATTACH 'ducklake:{catalog_db}' AS lake (DATA_PATH 'data/lake/');")
    conn.execute("USE lake;")
    
    # Check if table exists (it should, from bootstrap_catalog.sql)
    # Try querying the table directly - if it fails, table doesn't exist
    try:
        conn.execute(f"SELECT COUNT(*) FROM lake.{table} LIMIT 1;")
        table_exists = True
    except:
        table_exists = False
    
    if not table_exists:
        print_info(f"Error: Table {table} does not exist.")
        print_info("Please initialize catalog first with: uv run python run.py catalog")
        conn.close()
        sys.exit(1)
    
    print_info(f"Inserting into existing table: {table}")
    
    # Determine partition column extraction based on table
    # For lineitem: extract year from l_shipdate
    # For orders: extract year, month, day from o_orderdate
    if table == "lineitem":
        partition_select = "year(l_shipdate) AS year"
    elif table == "orders":
        partition_select = "year(o_orderdate) AS year, month(o_orderdate) AS month, day(o_orderdate) AS day"
    else:
        # Default: try to extract year from common date columns
        partition_select = "year(l_shipdate) AS year"
    
    # Load each file individually
    for idx, file_path in enumerate(parquet_files, 1):
        print_info(f"[{idx}/{file_count}] Loading {file_path.name}...")
        conn.execute(f"""
            INSERT INTO lake.{table}
            SELECT 
                *,
                {partition_select}
            FROM read_parquet('{str(file_path)}');
        """)
    
    conn.close()
    
    print_info("")
    print_info(f"Successfully loaded {file_count} files into {table} table")
    print_info("Each file created a separate DuckLake file, ready for compaction!")
    print_info("")
    print_info("Next steps:")
    print_info("  1. Create a snapshot: uv run python run.py manifest")
    print_info("  2. Compact files: uv run python run.py compact")


def cmd_manifest(args):
    """Create DuckLake snapshot"""
    print_info("Creating DuckLake snapshot...")
    execute_sql_file("make_manifest.sql")
    print_info("Snapshot created. Query DuckLake metadata directly - no external manifest files needed!")


def cmd_time_travel(args):
    """Demonstrate time travel queries"""
    print_info("Demonstrating DuckLake time travel capabilities...")
    execute_sql_file("time_travel.sql")


def cmd_compact(args):
    """Compact small files"""
    print_info("Compacting small files to improve query performance...")
    table_name = args.table or "lineitem"
    print_info(f"Table: {table_name}")
    
    # Read the SQL file and replace the hardcoded table name
    sql_path = Path("scripts/compaction.sql")
    with open(sql_path) as f:
        sql = f.read()
    
    # Replace the default table name 'lineitem' with the provided one, or keep default
    sql = sql.replace("SELECT 'lineitem' AS table_name", f"SELECT '{table_name}' AS table_name")
    sql = sql.replace("CALL ducklake_merge_adjacent_files('lake','lineitem')", f"CALL ducklake_merge_adjacent_files('lake','{table_name}')")
    
    print_info("Executing compaction SQL...")
    # Execute the modified SQL
    conn = duckdb.connect()
    try:
        # Split and execute statements to show all results
        raw_statements = sql.split(';')
        
        for raw_stmt in raw_statements:
            statement = raw_stmt.strip()
            if not statement:
                continue
            
            # Remove comment lines
            lines = []
            for line in statement.split('\n'):
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    lines.append(line)
            
            if not lines:
                continue
            
            statement = '\n'.join(lines).strip()
            if not statement:
                continue
            
            try:
                result = conn.execute(statement)
                try:
                    results = result.fetchall()
                    if results:
                        try:
                            columns = result.columns()
                            if columns:
                                print_info(" | ".join(str(col) for col in columns))
                                print_info("-" * min(80, len(" | ".join(str(col) for col in columns)) * 2))
                        except:
                            pass
                        for row in results:
                            print_info(" | ".join(str(cell) for cell in row))
                        print_info("")
                except:
                    pass
            except Exception as e:
                error_msg = str(e)
                if "does not return a result set" not in error_msg.lower():
                    if "already exists" not in error_msg.lower() and "does not exist" not in error_msg.lower():
                        print_info(f"Note: {error_msg}")
    finally:
        conn.close()
    
    print_info("Compaction completed!")


def cmd_expire_snapshots(args):
    """Expire old snapshots"""
    print_info("=" * 80)
    print_info("SNAPSHOT EXPIRATION")
    print_info("=" * 80)
    older_than = args.older_than or "1 minute"
    print_info(f"Retention period: {older_than}")
    
    if args.dry_run:
        print_info("Mode: DRY RUN (no snapshots will be expired)")
    else:
        print_info("Mode: LIVE (snapshots will be expired)")
    
    print_info("")
    print_info("PRE-STATE: Current snapshots and storage")
    print_info("-" * 80)
    
    # Read the SQL file and replace hardcoded values
    sql_path = Path("scripts/expire_snapshots.sql")
    with open(sql_path) as f:
        sql = f.read()
    
    # Replace older_than interval (default: '1 minute')
    sql = sql.replace("INTERVAL '1 minute'", f"INTERVAL '{older_than}'")
    sql = sql.replace("older than 1 minute", f"older than {older_than}")
    
    print_info("")
    # Execute the modified SQL
    conn = duckdb.connect()
    try:
        # Split and execute statements to show all results
        cleaned_lines = []
        for line in sql.split('\n'):
            stripped = line.strip()
            if not stripped or stripped.startswith('--'):
                continue
            if '--' in line:
                comment_pos = line.find('--')
                cleaned_line = line[:comment_pos].rstrip()
                if cleaned_line.strip():
                    cleaned_lines.append(cleaned_line)
            else:
                cleaned_lines.append(line)
        
        cleaned_sql = '\n'.join(cleaned_lines)
        raw_statements = cleaned_sql.split(';')
        
        for raw_stmt in raw_statements:
            statement = raw_stmt.strip()
            if not statement:
                continue
            
            try:
                result = conn.execute(statement)
                try:
                    results = result.fetchall()
                    if results:
                        try:
                            description = result.description()
                            columns = [col[0] for col in description] if description else []
                            
                            if columns and len(columns) == 1 and columns[0].lower() == 'info':
                                info_text = str(results[0][0])
                                if info_text.startswith('==='):
                                    print_info("")
                                    print_info(info_text)
                                    print_info("")
                                else:
                                    print_info(info_text)
                            elif columns and len(columns) == 1 and columns[0].lower() == 'status':
                                status_text = str(results[0][0])
                                print_info(status_text)
                                print_info("")
                            else:
                                if columns:
                                    print_info(" | ".join(str(col) for col in columns))
                                    print_info("-" * min(80, max(len(" | ".join(str(col) for col in columns)), 40)))
                                for row in results:
                                    print_info(" | ".join(str(cell) for cell in row))
                                print_info("")
                        except Exception as e:
                            # Fallback: print results without headers
                            for row in results:
                                print_info(" | ".join(str(cell) for cell in row))
                            print_info("")
                except Exception as fetch_error:
                    # Check if this is a statement that doesn't return results
                    error_msg = str(fetch_error)
                    if "does not return a result set" not in error_msg.lower():
                        pass  # Silent - this is expected for non-SELECT statements
                    pass
            except Exception as e:
                error_msg = str(e)
                if "does not return a result set" not in error_msg.lower():
                    if "already exists" not in error_msg.lower() and "does not exist" not in error_msg.lower():
                        print_info(f"Note: {error_msg}")
    finally:
        conn.close()
    
    print_info("")
    print_info("╔" + "═" * 78 + "╗")
    print_info("║ POST-STATE: Remaining snapshots and storage" + " " * 40 + "║")
    print_info("╚" + "═" * 78 + "╝")
    print_info("")
    print_info("Expire snapshots completed!")
    print_info("=" * 80)


def cmd_change_feed(args):
    """Show row-level changes between snapshots"""
    print_info("=" * 80)
    print_info("CHANGE FEED ANALYSIS")
    print_info("=" * 80)
    table = args.table or "orders"
    print_info(f"Table: {table}")
    
    # Connect to DuckLake to get snapshot IDs
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake;")
    conn.execute("LOAD ducklake;")
    conn.execute("ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');")
    conn.execute("USE lake;")
    
    # Get list of snapshots
    snapshots = conn.execute("""
        SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot 
        ORDER BY snapshot_id DESC LIMIT 10;
    """).fetchall()
    
    # Find two valid snapshots where the table exists
    to_version = None
    from_version = None
    
    for snapshot_row in snapshots:
        snapshot_id = snapshot_row[0]
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM {table} AT (VERSION => {snapshot_id}) LIMIT 1;
            """).fetchone()
            if result and result[0] >= 0:
                if to_version is None:
                    to_version = snapshot_id
                elif from_version is None:
                    from_version = snapshot_id
                    break
        except:
            continue
    
    # Fallback to latest two snapshots
    if to_version is None or from_version is None:
        if snapshots:
            to_version = snapshots[0][0] if to_version is None else to_version
            if len(snapshots) > 1:
                from_version = snapshots[1][0] if from_version is None else from_version
    
    if to_version is None or from_version is None:
        print_info("Error: Need at least two snapshots to compare")
        conn.close()
        sys.exit(1)
    
    print_info(f"\nPRE-STATE: Snapshot {from_version}")
    print_info(f"POST-STATE: Snapshot {to_version}")
    print_info("")
    print_info("Executing change feed analysis...")
    print_info("")
    
    # Execute change feed SQL with snapshot IDs using the same connection
    execute_sql_file("change_feed.sql", {
        "FROM_VERSION": from_version,
        "TO_VERSION": to_version,
        "TABLE": table
    }, conn=conn)
    
    conn.close()
    
    print_info("=" * 80)
    print_info("Change feed analysis completed!")
    print_info("=" * 80)


def cmd_verify(args):
    """Show row counts"""
    print_info("Verifying row counts across raw files, raw tables, and partitioned tables...")
    print_info("")
    execute_sql_file("verify_counts.sql")
    print_info("")
    print_info("Verification completed!")


def cmd_clean(args):
    """Remove all generated data and catalog"""
    print_info("Removing all generated data and catalog...")
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
            else:
                shutil.rmtree(path)
            print_info(f"Removed: {path_str}")


def main():
    parser = argparse.ArgumentParser(
        description="DuckLake TPCH Demo - Simple Python CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python run.py setup                    # Install dependencies
  uv run python run.py tpch                     # Generate all TPCH parts
  uv run python run.py tpch --part 1            # Generate only part 1
  uv run python run.py catalog                  # Initialize DuckLake catalog
  uv run python run.py repartition              # Repartition orders table
  uv run python run.py verify                   # Verify row counts
  uv run python run.py manifest                 # Create snapshot
  uv run python run.py time-travel              # Demonstrate time travel queries
  uv run python run.py change-feed              # Show changes between snapshots
  uv run python run.py compact --table orders   # Compact files
  uv run python run.py clean                    # Remove all data
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Setup
    subparsers.add_parser("setup", help="Install Python dependencies and verify environment")
    
    # Data Generation
    tpch_parser = subparsers.add_parser("tpch", help="Generate TPCH data")
    tpch_parser.add_argument("--part", type=int, help="Generate only part N")
    
    # DuckLake Operations
    subparsers.add_parser("catalog", help="Initialize DuckLake catalog")
    subparsers.add_parser("repartition", help="Repartition orders table")
    
    load_parser = subparsers.add_parser("load-small-files", help="Load Parquet files one at a time")
    load_parser.add_argument("--table", default="lineitem", help="Table name (default: lineitem)")
    
    subparsers.add_parser("manifest", help="Create DuckLake snapshot")
    
    # Time Travel
    subparsers.add_parser("time-travel", help="Demonstrate time travel queries")
    
    # File Management
    compact_parser = subparsers.add_parser("compact", help="Compact small files")
    compact_parser.add_argument("--table", help="Table name")
    compact_parser.add_argument("--target-size", help="Target size in bytes")
    compact_parser.add_argument("--partition-filter", help="Partition filter (e.g., year=1992)")
    
    expire_parser = subparsers.add_parser("expire-snapshots", help="Expire old snapshots")
    expire_parser.add_argument("--older-than", help="Expire snapshots older than (e.g., '7 days')")
    expire_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    # Change Feed
    change_feed_parser = subparsers.add_parser("change-feed", help="Show row-level changes between snapshots")
    change_feed_parser.add_argument("--table", default="orders", help="Table name (default: orders)")
    change_feed_parser.add_argument("--from-version", type=int, help="From snapshot version")
    change_feed_parser.add_argument("--to-version", type=int, help="To snapshot version")
    
    # Verification
    subparsers.add_parser("verify", help="Show row counts")
    
    # Cleanup
    subparsers.add_parser("clean", help="Remove all generated data and catalog")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Map commands to functions
    commands = {
        "setup": cmd_setup,
        "tpch": cmd_tpch,
        "catalog": cmd_catalog,
        "repartition": cmd_repartition,
        "load-small-files": cmd_load_small_files,
        "manifest": cmd_manifest,
        "time-travel": cmd_time_travel,
        "compact": cmd_compact,
        "expire-snapshots": cmd_expire_snapshots,
        "change-feed": cmd_change_feed,
        "verify": cmd_verify,
        "clean": cmd_clean,
    }
    
    try:
        commands[args.command](args)
    except KeyboardInterrupt:
        print_info("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print_info(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

