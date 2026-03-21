from typing import List, Dict, Any, Optional
def safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

def safe_float(value: Any) -> Optional[float]:
   if value is None:
         return None
   try:
        return float(value)
   except (ValueError, TypeError):
        return None
    
def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return{
        "brewery_id": safe_str(record.get("brewery_id")),
        "name": safe_str(record.get("brewery_name")),
        "brewery_type": safe_str(record.get("brewery_type")),
        "street": safe_str(record.get("brewery_street")),
        "city": safe_str(record.get("brewery_city")),
        "state": safe_str(record.get("brewery_state")),
        "postal_code": safe_str(record.get("brewery_postal_code")),
        "country": safe_str(record.get("brewery_country")),
        "longitude": safe_float(record.get("brewery_longitude")),
        "latitude": safe_float(record.get("brewery_latitude")),
        "phone": safe_str(record.get("brewery_phone")),
        "website_url": safe_str(record.get("brewery_website_url")),
        "state_province": safe_str(record.get("brewery_state_province"))
        
    }
    
def transform_all(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformed = []
    skipped = 0
    for record in raw_data:
        item = transform_record(record)
        if not item ["brewery_id"] or not item["name"]:
            skipped += 1
            continue
    transformed.append(item)
    print(f"[transform] transformed {len(transformed)} records")
    print(f"[transform] transformed records, skipped {skipped}")
    return transformed