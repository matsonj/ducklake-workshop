# Code Cleanup and Readability Proposal

## Overview
This document proposes improvements to make the codebase cleaner and more readable without changing any functionality.

## Key Findings

### 1. SQL Scripts - Repetitive Boilerplate
**Issue**: All SQL scripts repeat the same DuckLake initialization code:
```sql
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;
```

**Recommendation**: 
- Add clear header comments explaining this pattern
- Standardize the format across all scripts
- Consider documenting this pattern in README for users

### 2. SQL Scripts - Inconsistent Formatting
**Issues**:
- `repartition_orders.sql` has commented-out code (line 36: `--ORDER BY o_orderdate;`)
- Inconsistent spacing and indentation
- Some scripts have better headers than others

**Recommendations**:
- Remove commented-out code
- Standardize SQL formatting (alignment, spacing)
- Add consistent file headers with purpose and usage

### 3. Shell Scripts - Variable Naming
**Issues**:
- `gen_tpch.sh` uses inconsistent variable naming (TABLES_CSV vs TABLES)
- Some error messages could be clearer
- Missing function documentation

**Recommendations**:
- Use consistent naming conventions
- Improve error messages with context
- Add brief function documentation where helpful

### 4. Configuration Files
**Issues**:
- `tpch.yaml` lacks inline comments explaining defaults
- Values could use brief explanations

**Recommendations**:
- Add inline comments for non-obvious settings
- Document environment variable overrides

### 5. Makefile
**Issues**:
- Limited comments explaining complex targets
- Could benefit from grouping related targets

**Recommendations**:
- Add section comments grouping related targets
- Clarify dependencies between targets

## Specific Proposed Changes

### SQL Scripts

#### `repartition_orders.sql`
- Remove commented-out ORDER BY line
- Add clear section headers
- Improve formatting consistency

#### `bootstrap_catalog.sql`
- Enhance comments explaining idempotency
- Clarify the zero-copy file registration concept

#### `verify_counts.sql`
- Add comments explaining the verification purpose
- Improve output formatting readability

#### `make_manifest.sql`
- Better organize query sections
- Add comments explaining metadata table structure

#### `fix_schema.sql`
- Add comment explaining when this script is needed
- Clarify the relationship to bootstrap_catalog.sql

### Shell Scripts

#### `gen_tpch.sh`
- Standardize variable naming (consider `TABLES` instead of `TABLES_CSV`)
- Add comments explaining part vs all logic
- Improve error messages

#### `preflight.sh`
- Enhance error messages with actionable guidance
- Add comments explaining why each check is needed
- Document the space check threshold

### Configuration

#### `tpch.yaml`
- Add inline comments explaining default values
- Document environment variable overrides
- Clarify compression and row group settings

#### `Makefile`
- Add section comments (Setup, Data Generation, DuckLake Operations, etc.)
- Enhance target descriptions
- Document target dependencies

## Implementation Priority

1. **High Priority** (Improves readability significantly):
   - Remove commented-out code
   - Standardize SQL formatting
   - Add consistent file headers
   - Improve error messages

2. **Medium Priority** (Enhances maintainability):
   - Add section comments in SQL scripts
   - Improve variable naming consistency
   - Add configuration file comments

3. **Low Priority** (Nice to have):
   - Group Makefile targets with comments
   - Enhance function documentation

## Principles

- **No functionality changes** - All behavior remains identical
- **Consistency** - Standardize formatting and naming across similar files
- **Clarity** - Add comments that explain "why" not just "what"
- **Remove dead code** - Delete commented-out lines
- **Improve discoverability** - Make it easier for new users to understand the code

