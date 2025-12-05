# Trademark Data Pipeline

Production-ready pipeline for processing trademark data from Excel files, transforming to Markify-like format, and exporting to JSON/CSV.

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Usage

**Basic usage (default data.xls):**
```bash
python main.py
```

**Custom Excel file:**
```bash
python main.py --data-file mydata.xls
```

**Limit number of records:**
```bash
python main.py --data-file data.xls --max-results 1000
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_TRADEMARKS` | Maximum trademarks to process | `100` |
| `OUTPUT_DIR` | Output directory | `output` |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | `INFO` |

### Command Line Arguments

- `--data-file`: Path to Excel file (default: `data.xls`)
- `--max-results`: Maximum number of trademarks to process (default: from config)

## Output

- **JSON** (`output/trademarks_cleaned.json`): Full nested structure with all fields
- **CSV** (`output/trademarks_cleaned.csv`): Flattened structure for analysis

## Project Structure

```
├── main.py                 # Main pipeline script
├── config.py               # Configuration (supports .env)
├── static_data_loader.py   # Excel file loader
├── data_transformer.py     # Data transformation to Markify format
├── data_cleaner.py         # Data cleaning and normalization
├── requirements.txt        # Dependencies
└── output/                 # Output directory
```

## Data Format

The pipeline expects Excel files (.xls or .xlsx) with the following columns:
- `Application Id` - Maps to `serial_number`
- `Application Number` - Maps to `registration_number`
- `Application Date` - Maps to `filing_date` (normalized to YYYY-MM-DD)
- `Country` - Maps to `owner.country`
- `Title` - Maps to `mark_text`
- `I P C` - Maps to `classes` (IPC codes parsed)

## Logging

Logs are written to console and file (configurable via `LOG_FILE` environment variable).

## Requirements

- Python 3.8+
- See `requirements.txt` for dependencies
