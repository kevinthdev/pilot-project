"""
Data cleaning and normalization module
"""

from typing import Dict, List
import re


class DataCleaner:
    """Clean and normalize trademark data"""
    
    @staticmethod
    def clean_trademark(record: Dict) -> Dict:
        """Clean a single trademark record"""
        cleaned = record.copy()
        
        if "owner" in cleaned and cleaned["owner"]:
            cleaned["owner"] = DataCleaner._clean_owner(cleaned["owner"])
        
        if "representative" in cleaned and cleaned["representative"]:
            cleaned["representative"] = DataCleaner._clean_representative(cleaned["representative"])
        
        for field in ["mark_text", "status", "goods_services"]:
            if field in cleaned and cleaned[field]:
                cleaned[field] = DataCleaner._normalize_text(cleaned[field])
        
        cleaned = DataCleaner._ensure_required_fields(cleaned)
        
        return cleaned
    
    @staticmethod
    def _clean_owner(owner: Dict) -> Dict:
        """Clean owner information"""
        if not owner:
            return owner
        
        cleaned_owner = owner.copy()
        
        if "name" in cleaned_owner and cleaned_owner["name"]:
            cleaned_owner["name"] = DataCleaner._normalize_text(cleaned_owner["name"])
            cleaned_owner["name"] = re.sub(r'\s+(Inc\.?|LLC|L\.L\.C\.?|Corp\.?|Corporation|Ltd\.?|Limited)$',
                                          '', cleaned_owner["name"], flags=re.IGNORECASE)
        
        if "address" in cleaned_owner and cleaned_owner["address"]:
            cleaned_owner["address"] = DataCleaner._normalize_text(cleaned_owner["address"])
        
        if "city" in cleaned_owner and cleaned_owner["city"]:
            cleaned_owner["city"] = DataCleaner._normalize_text(cleaned_owner["city"])
        
        if "state" in cleaned_owner and cleaned_owner["state"]:
            cleaned_owner["state"] = cleaned_owner["state"].strip().upper()
        
        if "country" in cleaned_owner and cleaned_owner["country"]:
            cleaned_owner["country"] = cleaned_owner["country"].strip().upper()
        
        if "postal_code" in cleaned_owner and cleaned_owner["postal_code"]:
            cleaned_owner["postal_code"] = cleaned_owner["postal_code"].strip().upper()
        
        return cleaned_owner
    
    @staticmethod
    def _clean_representative(rep: Dict) -> Dict:
        """Clean representative information"""
        if not rep:
            return rep
        
        cleaned_rep = rep.copy()
        
        for field in ["name", "firm", "address"]:
            if field in cleaned_rep and cleaned_rep[field]:
                cleaned_rep[field] = DataCleaner._normalize_text(cleaned_rep[field])
        
        if "phone" in cleaned_rep and cleaned_rep["phone"]:
            cleaned_rep["phone"] = re.sub(r'[^\d+]', '', cleaned_rep["phone"])
        
        if "email" in cleaned_rep and cleaned_rep["email"]:
            cleaned_rep["email"] = cleaned_rep["email"].strip().lower()
        
        return cleaned_rep
    
    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize text by removing extra whitespace"""
        if not text or not isinstance(text, str):
            return text
        return re.sub(r'\s+', ' ', text.strip())
    
    @staticmethod
    def _ensure_required_fields(record: Dict) -> Dict:
        """Ensure all required fields exist"""
        required_structure = {
            "registration_number": None,
            "serial_number": None,
            "registration_date": None,
            "expiry_date": None,
            "status": None,
            "mark_text": None,
            "mark_drawing_code": None,
            "classes": [],
            "owner": {
                "name": None,
                "address": None,
                "city": None,
                "state": None,
                "country": None,
                "postal_code": None
            },
            "representative": None,
            "events": [],
            "filing_date": None,
            "published_date": None,
            "goods_services": None
        }
        
        cleaned = required_structure.copy()
        cleaned.update(record)
        
        if "owner" in record and record["owner"]:
            cleaned["owner"] = {**required_structure["owner"], **record["owner"]}
        else:
            cleaned["owner"] = required_structure["owner"].copy()
        
        if "classes" not in cleaned or not isinstance(cleaned["classes"], list):
            cleaned["classes"] = []
        
        if "events" not in cleaned or not isinstance(cleaned["events"], list):
            cleaned["events"] = []
        
        return cleaned
    
    @staticmethod
    def clean_batch(records: List[Dict]) -> List[Dict]:
        """Clean a batch of trademark records"""
        return [DataCleaner.clean_trademark(record) for record in records]
    
    @staticmethod
    def flatten_record(record: Dict) -> Dict:
        """
        Flatten nested JSON structure for CSV export
        Each trademark record becomes fully self-contained
        """
        flattened = record.copy()
        
        # Flatten owner
        if "owner" in flattened and isinstance(flattened["owner"], dict):
            owner = flattened.pop("owner")
            flattened["owner_name"] = owner.get("name")
            flattened["owner_address"] = owner.get("address")
            flattened["owner_city"] = owner.get("city")
            flattened["owner_state"] = owner.get("state")
            flattened["owner_country"] = owner.get("country")
            flattened["owner_postal_code"] = owner.get("postal_code")
        
        # Flatten representative
        if "representative" in flattened and isinstance(flattened["representative"], dict):
            rep = flattened.pop("representative")
            flattened["rep_name"] = rep.get("name")
            flattened["rep_firm"] = rep.get("firm")
            flattened["rep_address"] = rep.get("address")
            flattened["rep_phone"] = rep.get("phone")
            flattened["rep_email"] = rep.get("email")
        
        # Flatten classes (comma-separated)
        if "classes" in flattened and isinstance(flattened["classes"], list):
            classes = flattened.pop("classes")
            class_numbers = [str(c.get("class_number", "")) for c in classes if c.get("class_number")]
            flattened["class_numbers"] = ", ".join(class_numbers) if class_numbers else None
            flattened["class_count"] = len(classes)
        else:
            flattened["class_numbers"] = None
            flattened["class_count"] = 0
        
        # Flatten events (count and latest event)
        if "events" in flattened and isinstance(flattened["events"], list):
            events = flattened.pop("events")
            flattened["event_count"] = len(events)
            if events:
                # Get latest event date
                latest_event = max([e for e in events if e.get("date")], 
                                 key=lambda x: x.get("date", ""), default=None)
                flattened["latest_event_date"] = latest_event.get("date") if latest_event else None
                flattened["latest_event_type"] = latest_event.get("type") if latest_event else None
            else:
                flattened["latest_event_date"] = None
                flattened["latest_event_type"] = None
        else:
            flattened["event_count"] = 0
            flattened["latest_event_date"] = None
            flattened["latest_event_type"] = None
        
        # Remove raw_data for CSV (too large)
        if "raw_data" in flattened:
            del flattened["raw_data"]
        
        return flattened

