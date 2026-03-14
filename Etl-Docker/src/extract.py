import time
import requests
from typing import List, Dict, Any
from config import settings

def fetch_page(page:int, per_page:int) -> List[Dict[str, Any]]:
   url= settings.api_base_url
   params = {"page": page, "per_page": per_page}
   
   for attempt in range(3):
       try:
              response = requests.get(url, params=params, timeout=10)
              response.raise_for_status()
              data= response.json()
              
              if not isinstance(data, list):
                  raise ValueError("API returned invalid data ")
              return data
       except Exception as exc:
           print(f"[extract] attempt{attempt+1}/3 page {page} failed: {exc}")
           time.sleep(2 ** attempt)
           
   raise RuntimeError(f"Failed to fetch page {page} after 3 attempts")
def extract_all() -> List[Dict[str, Any]]:
    all_data = []
    
    for page in range(1, settings.max_pages + 1):
        page_data = fetch_page(page= page, per_page=settings.per_page)
        if not page_data:
            print(f"[extract] no data found for page {page}")
            break
        print(f"[extract] fetched {len(page_data)} records from page {page}")
        all_data.extend(page_data)    
    print(f"[extract] fetched {len(all_data)} records from {settings.max_pages} pages")
    return all_data

   
      
        