"""
Data transformation module
Converts trademark data to Markify-like format
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import re


class DataTransformer:
    """Transform trademark data to Markify-like structure"""
    
    @staticmethod
    def transform_trademark(record: Dict) -> Dict:
        """
        Transform a single trademark record to Markify-like format
        
        Args:
            record: Raw trademark record (from Excel or API)
            
        Returns:
            Transformed trademark record in Markify-like format
        """
        raw_data = record.get("raw_data", {}) or {}
        excel_data = raw_data.get("_raw_excel_data", {}) or {}
        all_sources = [record, raw_data, excel_data]
        
        transformed = {
            # Trademark Info
            # Map application_number to registration_number, application_id to serial_number
            "registration_number": DataTransformer._extract_field_multisource(
                all_sources,
                ["registrationNumber", "regNumber", "registration_number", "regNum",
                 "application_number", "Application Number"]
            ),
            "serial_number": DataTransformer._extract_field_multisource(
                all_sources,
                ["serialNumber", "serial_number", "serialNum", "applicationNumber",
                 "application_id", "Application Id"]
            ),
            "registration_date": DataTransformer._normalize_date(
                DataTransformer._extract_field_multisource(
                    all_sources,
                    ["registrationDate", "registration_date", "regDate", "registeredDate"]
                )
            ),
            "expiry_date": DataTransformer._normalize_date(
                DataTransformer._extract_field_multisource(
                    all_sources,
                    ["expiryDate", "expiry_date", "expirationDate", "expDate"]
                )
            ),
            "status": DataTransformer._extract_field_multisource(
                all_sources,
                ["status", "markStatus", "currentStatus", "statusCode"]
            ),
            # Map title to mark_text
            "mark_text": DataTransformer._extract_field_multisource(
                all_sources,
                ["markText", "mark_text", "wordMark", "markDescription",
                 "title", "Title"]
            ),
            "mark_drawing_code": DataTransformer._extract_field_multisource(
                all_sources,
                ["markDrawingCode", "mark_drawing_code", "drawingCode"]
            ),
            
            "classes": DataTransformer._extract_classes(record),
            "owner": DataTransformer._extract_owner(record),
            "representative": DataTransformer._extract_representative(record),
            "events": DataTransformer._extract_events(record),
            "filing_date": DataTransformer._normalize_date(
                DataTransformer._extract_field_multisource(
                    all_sources,
                    ["filingDate", "filing_date", "applicationDate", "appDate",
                     "application_date", "Application Date"]
                )
            ),
            "published_date": DataTransformer._normalize_date(
                DataTransformer._extract_field_multisource(
                    all_sources,
                    ["publishedDate", "publicationDate", "pubDate"]
                )
            ),
            "goods_services": DataTransformer._extract_field_multisource(
                all_sources,
                ["goodsServices", "goods_services", "description", "goodsAndServices"]
            ),
            
            "raw_data": record
        }
        
        return transformed
    
    @staticmethod
    def _extract_field(record: Dict, possible_keys: List[str], default: Any = None) -> Any:
        """Extract field from record using multiple possible key names"""
        for key in possible_keys:
            if "." in key:
                keys = key.split(".")
                value = record
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        value = None
                        break
                if value is not None:
                    return value
            elif key in record:
                value = record[key]
                if value is not None and value != "":
                    return value
        
        return default
    
    @staticmethod
    def _extract_field_multisource(sources: List[Dict], possible_keys: List[str], default: Any = None) -> Any:
        """Extract field from multiple source dictionaries"""
        for source in sources:
            if not isinstance(source, dict):
                continue
            value = DataTransformer._extract_field(source, possible_keys)
            if value is not None and value != "":
                return value
        return default
    
    @staticmethod
    def _normalize_date(date_value: Any) -> Optional[str]:
        """Normalize date to YYYY-MM-DD format"""
        if date_value is None:
            return None
        
        if isinstance(date_value, str):
            # Try to parse various date formats
            date_formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%d.%m.%Y",  # Excel format: DD.MM.YYYY
                "%d-%m-%Y",  # Alternative: DD-MM-YYYY
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
                "%B %d, %Y",
                "%b %d, %Y",
                "%d %B %Y",
                "%d %b %Y"
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            
            # Try to extract date from timestamp string
            try:
                timestamp = float(date_value)
                dt = datetime.fromtimestamp(timestamp)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
        
        elif isinstance(date_value, (int, float)):
            # Handle timestamp
            try:
                dt = datetime.fromtimestamp(date_value)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, OSError):
                return None
        
        elif isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
        
        return None
    
    @staticmethod
    def _extract_classes(record: Dict) -> List[Dict]:
        """Extract trademark class information"""
        classes = []
        raw_data = record.get("raw_data", {}) or {}
        excel_data = raw_data.get("_raw_excel_data", {}) or {}
        all_sources = [record, raw_data, excel_data]
        
        class_fields = [
            "classes", "internationalClasses", "classCodes",
            "goodsServicesClasses", "trademarkClasses",
            "i_p_c", "I P C", "ipc"
        ]
        
        ipc_data = None
        for source in all_sources:
            if not isinstance(source, dict):
                continue
            for field in class_fields:
                if field in source:
                    ipc_data = source[field]
                    break
            if ipc_data:
                break
        
        if ipc_data:
            if isinstance(ipc_data, list):
                for cls in ipc_data:
                    if isinstance(cls, dict):
                        classes.append({
                            "class_number": cls.get("classNumber") or cls.get("class_number") or cls.get("code"),
                            "description": cls.get("description") or cls.get("classDescription")
                        })
                    elif isinstance(cls, (int, str)):
                        classes.append({"class_number": str(cls), "description": None})
            elif isinstance(ipc_data, dict):
                classes.append({
                    "class_number": ipc_data.get("classNumber") or ipc_data.get("class_number"),
                    "description": ipc_data.get("description")
                })
            elif isinstance(ipc_data, str):
                ipc_codes = [code.strip() for code in ipc_data.split(';') if code.strip()]
                classes.extend([{"class_number": code, "description": None} for code in ipc_codes])
        
        if not classes:
            goods_services = DataTransformer._extract_field_multisource(
                all_sources, ["goodsServices", "goods_services", "description"]
            )
            if goods_services:
                class_matches = re.findall(r'[Cc]lass\s+(\d+)', str(goods_services))
                classes.extend([{"class_number": match, "description": None} for match in class_matches])
        
        return classes
    
    @staticmethod
    def _extract_owner(record: Dict) -> Dict:
        """Extract owner/applicant information"""
        owner = {
            "name": None, "address": None, "city": None,
            "state": None, "country": None, "postal_code": None
        }
        
        raw_data = record.get("raw_data", {}) or {}
        excel_data = raw_data.get("_raw_excel_data", {}) or {}
        all_sources = [record, raw_data, excel_data]
        
        owner_fields = ["owner", "applicant", "assignee", "ownerName", "applicantName"]
        owner_data = None
        
        for source in all_sources:
            if not isinstance(source, dict):
                continue
            for field in owner_fields:
                if field in source:
                    owner_data = source[field]
                    break
            if owner_data:
                break
        
        if owner_data:
            if isinstance(owner_data, dict):
                owner["name"] = owner_data.get("name") or owner_data.get("ownerName") or owner_data.get("applicantName")
                for addr_field in ["address", "street", "streetAddress"]:
                    if addr_field in owner_data:
                        owner["address"] = owner_data[addr_field]
                        break
                owner["city"] = owner_data.get("city")
                owner["state"] = owner_data.get("state") or owner_data.get("stateProvince")
                owner["country"] = owner_data.get("country") or owner_data.get("countryCode")
                owner["postal_code"] = owner_data.get("postalCode") or owner_data.get("postal_code") or owner_data.get("zip")
            elif isinstance(owner_data, str):
                owner["name"] = owner_data
        
        if not owner["country"]:
            owner["country"] = DataTransformer._extract_field_multisource(all_sources, ["country", "Country"])
        
        if owner["name"]:
            owner["name"] = DataTransformer._normalize_text(owner["name"])
        
        return owner
    
    @staticmethod
    def _extract_representative(record: Dict) -> Optional[Dict]:
        """Extract representative/attorney information"""
        rep = {"name": None, "firm": None, "address": None, "phone": None, "email": None}
        rep_fields = ["representative", "attorney", "correspondent", "lawyer", "representativeName"]
        
        rep_data = None
        for field in rep_fields:
            if field in record:
                rep_data = record[field]
                break
        
        if rep_data and isinstance(rep_data, dict):
            rep["name"] = rep_data.get("name") or rep_data.get("attorneyName")
            rep["firm"] = rep_data.get("firm") or rep_data.get("lawFirm")
            rep["address"] = rep_data.get("address")
            rep["phone"] = rep_data.get("phone") or rep_data.get("telephone")
            rep["email"] = rep_data.get("email")
        
        return rep if any(rep.values()) else None
    
    @staticmethod
    def _extract_events(record: Dict) -> List[Dict]:
        """Extract event history"""
        events = []
        event_fields = ["events", "history", "prosecutionHistory", "statusHistory", "eventHistory"]
        
        for field in event_fields:
            if field in record:
                event_data = record[field]
                if isinstance(event_data, list):
                    for event in event_data:
                        if isinstance(event, dict):
                            events.append({
                                "date": DataTransformer._normalize_date(event.get("date") or event.get("eventDate")),
                                "type": event.get("type") or event.get("eventType") or event.get("code"),
                                "description": event.get("description") or event.get("eventDescription"),
                                "code": event.get("code") or event.get("eventCode")
                            })
                break
        
        return events
    
    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize text by removing extra whitespace and standardizing"""
        if not text or not isinstance(text, str):
            return text
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
    
    @staticmethod
    def transform_batch(records: List[Dict]) -> List[Dict]:
        """Transform a batch of records"""
        return [DataTransformer.transform_trademark(record) for record in records]

