"""
Trademark Data Pipeline
Processes trademark data from Excel files, transforms to Markify-like format, and exports to JSON/CSV
"""

import json
import sys
import logging
from pathlib import Path
from typing import List, Optional
import pandas as pd

from config import (
    OUTPUT_DIR, 
    OUTPUT_JSON_FILE, 
    OUTPUT_CSV_FILE, 
    MAX_TRADEMARKS,
    LOG_LEVEL,
    LOG_FILE
)
from static_data_loader import StaticDataLoader
from data_transformer import DataTransformer
from data_cleaner import DataCleaner

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def ensure_output_dir() -> Path:
    """Create output directory if it doesn't exist"""
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def save_json(data: List[dict], filename: str, output_dir: Path) -> str:
    """Save data as JSON file"""
    filepath = output_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved JSON to: {filepath}")
    return str(filepath)


def save_csv(data: List[dict], filename: str, output_dir: Path) -> Optional[str]:
    """Save data as CSV file with flattened structure"""
    if not data:
        logger.warning("No data to save to CSV")
        return None
    
    try:
        # Flatten records for CSV
        flattened_data = [DataCleaner.flatten_record(record) for record in data]
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_data)
        
        # Save to CSV
        filepath = output_dir / filename
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"Saved CSV to: {filepath}")
        logger.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        return str(filepath)
    except Exception as e:
        logger.error(f"Error saving CSV: {e}")
        return None


def main(data_file: Optional[str] = None, max_results: Optional[int] = None):
    """
    Main execution function
    
    Args:
        data_file: Optional path to Excel file (default: data.xls)
        max_results: Optional maximum number of trademarks to process. Defaults to MAX_TRADEMARKS.
    """
    max_results = max_results or MAX_TRADEMARKS
    
    logger.info("=" * 60)
    logger.info("Trademark Data Pipeline")
    logger.info("=" * 60)
    
    try:
        data_file = data_file or "data.xls"
        logger.info(f"Loading data from: {data_file}")
        
        try:
            raw_trademarks = StaticDataLoader.load_from_excel(
                filepath=data_file,
                max_records=max_results
            )
            
            if not raw_trademarks:
                logger.error(f"No trademarks loaded from {data_file}. Check file exists and has data.")
                sys.exit(1)
            
            logger.info(f"Loaded {len(raw_trademarks)} trademark records")
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            sys.exit(1)
        
        logger.info("Transforming data to Markify-like format...")
        try:
            transformed_trademarks = DataTransformer.transform_batch(raw_trademarks)
            logger.info(f"Transformed {len(transformed_trademarks)} trademarks")
        except Exception as e:
            logger.error(f"Error transforming data: {e}", exc_info=True)
            sys.exit(1)
        
        logger.info("Cleaning and normalizing data...")
        try:
            cleaned_trademarks = DataCleaner.clean_batch(transformed_trademarks)
            logger.info(f"Cleaned {len(cleaned_trademarks)} trademarks")
        except Exception as e:
            logger.error(f"Error cleaning data: {e}", exc_info=True)
            sys.exit(1)
        
        logger.info("Saving outputs...")
        output_dir = ensure_output_dir()
        
        try:
            json_path = save_json(cleaned_trademarks, OUTPUT_JSON_FILE, output_dir)
            csv_path = save_csv(cleaned_trademarks, OUTPUT_CSV_FILE, output_dir)
            
            logger.info("=" * 60)
            logger.info("PIPELINE SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total trademarks processed: {len(cleaned_trademarks)}")
            logger.info(f"Output directory: {output_dir}/")
            logger.info(f"  - JSON: {OUTPUT_JSON_FILE}")
            if csv_path:
                logger.info(f"  - CSV: {OUTPUT_CSV_FILE}")
            logger.info("Pipeline completed successfully")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error saving outputs: {e}", exc_info=True)
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Trademark Data Pipeline')
    parser.add_argument(
        '--data-file',
        type=str,
        default='data.xls',
        help='Path to Excel file with trademark data (default: data.xls)'
    )
    parser.add_argument(
        '--max-results',
        type=int,
        default=MAX_TRADEMARKS,
        help=f'Maximum number of trademarks to process (default: {MAX_TRADEMARKS})'
    )
    
    args = parser.parse_args()
    
    main(data_file=args.data_file, max_results=args.max_results)
