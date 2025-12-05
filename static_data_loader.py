"""
Static Data Loader - Loads trademark data from Excel files
"""

import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
import xlrd

logger = logging.getLogger(__name__)


class StaticDataLoader:
    """Load trademark data from static Excel file"""
    
    @staticmethod
    def load_from_excel(filepath: str = "data.xls", max_records: Optional[int] = None) -> List[Dict]:
        """
        Load trademark data from Excel file
        
        Args:
            filepath: Path to Excel file (default: data.xls)
            max_records: Optional maximum number of records to load
            
        Returns:
            List of trademark records as dictionaries
        """
        file_path = Path(filepath)
        
        if not file_path.exists():
            logger.error(f"Excel file not found: {filepath}")
            return []
        
        logger.info(f"Loading static data from: {filepath}")
        
        try:
            file_ext = file_path.suffix.lower()
            
            if file_ext == '.xlsx':
                try:
                    df = pd.read_excel(filepath, engine='openpyxl')
                except Exception as e:
                    logger.warning(f"Failed to read with openpyxl: {e}")
                    df = pd.read_excel(filepath)
            elif file_ext == '.xls':
                logger.info("Reading .xls file using xlrd...")
                records = StaticDataLoader._read_xls_with_xlrd(filepath)
                if records:
                    if max_records:
                        records = records[:max_records]
                        logger.info(f"Limited to {max_records} records")
                    
                    formatted_records = [StaticDataLoader._format_record(r) for r in records]
                    logger.info(f"Successfully loaded {len(formatted_records)} records")
                    return formatted_records
                else:
                    logger.error("No records read from .xls file")
                    return []
            else:
                df = pd.read_excel(filepath)
            
            if df is None or df.empty:
                logger.warning("Excel file is empty or could not be read")
                return []
            
            logger.info(f"Loaded {len(df)} rows from Excel file")
            logger.info(f"Columns: {list(df.columns)}")
            
            if max_records:
                df = df.head(max_records)
                logger.info(f"Limited to {max_records} records")
            
            records = df.where(pd.notnull(df), None).to_dict('records')
            formatted_records = [StaticDataLoader._format_record(r) for r in records]
            
            logger.info(f"Successfully loaded {len(formatted_records)} records")
            return formatted_records
            
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _format_record(record: Dict) -> Dict:
        """Format a single record from Excel to match expected structure"""
        formatted = {}
        for key, value in record.items():
            if pd.isna(value):
                formatted[key] = None
            else:
                normalized_key = str(key).strip().lower().replace(' ', '_').replace('-', '_')
                formatted[normalized_key] = value
        
        formatted['_raw_excel_data'] = record
        return formatted
    
    @staticmethod
    def _read_xls_with_xlrd(filepath: str) -> List[Dict]:
        """Read .xls file using xlrd library directly"""
        try:
            workbook = xlrd.open_workbook(filepath)
            sheet = workbook.sheet_by_index(0)
            
            header_row_idx = None
            for row_idx in range(min(10, sheet.nrows)):
                non_empty_count = sum(1 for col_idx in range(sheet.ncols) 
                                    if sheet.cell_value(row_idx, col_idx) and 
                                    str(sheet.cell_value(row_idx, col_idx)).strip())
                if non_empty_count >= 3:
                    header_row_idx = row_idx
                    break
            
            if header_row_idx is None:
                header_row_idx = 0
                logger.warning("Could not find header row, using first row")
            else:
                logger.info(f"Found header row at index {header_row_idx}")
            
            headers = []
            for col_idx in range(sheet.ncols):
                cell_value = sheet.cell_value(header_row_idx, col_idx)
                header_name = str(cell_value).strip() if cell_value else f"Column_{col_idx}"
                if not header_name:
                    header_name = f"Column_{col_idx}"
                headers.append(header_name)
            
            logger.info(f"Found {len(headers)} columns: {headers[:10]}..." if len(headers) > 10 else f"Found {len(headers)} columns: {headers}")
            
            records = []
            for row_idx in range(header_row_idx + 1, sheet.nrows):
                non_empty_count = sum(1 for col_idx in range(sheet.ncols) 
                                    if sheet.cell_value(row_idx, col_idx) and 
                                    str(sheet.cell_value(row_idx, col_idx)).strip())
                if non_empty_count == 0:
                    continue
                
                record = {}
                for col_idx in range(sheet.ncols):
                    cell_value = sheet.cell_value(row_idx, col_idx)
                    if isinstance(cell_value, float) and cell_value == int(cell_value):
                        cell_value = int(cell_value)
                    elif cell_value == '':
                        cell_value = None
                    record[headers[col_idx]] = cell_value
                records.append(record)
            
            logger.info(f"Read {len(records)} data rows from Excel file")
            return records
            
        except Exception as e:
            logger.error(f"Error reading .xls file: {e}", exc_info=True)
            return []

