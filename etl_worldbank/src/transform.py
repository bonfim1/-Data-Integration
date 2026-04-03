import logging
from datetime import datetime
from config import settings

logger = logging.getLogger(__name__)

def transform_countries(raw_data):
    transformed = []
    for item in raw_data:
        # T1: Filtro de entidade (ISO2 == 2 chars) e Grupos de Renda aceitos [cite: 116]
        iso2 = item.get("id", "")
        income_level = item.get("incomeLevel", {}).get("id", "")
        
        if len(iso2) == 2 and income_level in settings.income_groups:
            # T2: Limpeza de strings e Title Case [cite: 116]
            transformed.append({
                "iso2_code": iso2.strip(),
                "iso3_code": item.get("iso2Code", "").strip(), # A API as vezes inverte campos
                "name": item.get("name", "").strip(),
                "region": item.get("region", {}).get("value", "").strip().title(),
                "income_group": item.get("incomeLevel", {}).get("value", "").strip(),
                "capital": item.get("capitalCity", "").strip() or None,
                "longitude": float(item.get("longitude")) if item.get("longitude") else None, # T3 [cite: 116]
                "latitude": float(item.get("latitude")) if item.get("latitude") else None
            })
    return transformed

def transform_indicators(raw_indicators_dict):
    facts = []
    current_year = datetime.now().year
    
    for code, records in raw_indicators_dict.items():
        for rec in records:
            # T1: Filtro de país real [cite: 116]
            country_id = rec.get("country", {}).get("id", "")
            year_str = rec.get("date", "")
            
            if len(country_id) == 2 and year_str.isdigit():
                year = int(year_str)
                # T4: Filtro temporal (2010 até hoje) [cite: 116]
                if settings.year_min <= year <= current_year:
                    # T3: Conversão segura de valor [cite: 116]
                    try:
                        val = float(rec["value"]) if rec["value"] is not None else None
                    except (ValueError, TypeError):
                        val = None
                        
                    facts.append({
                        "iso2_code": country_id,
                        "indicator_code": code,
                        "year": year,
                        "value": val
                    })
    
    # T5: Deduplicação [cite: 117]
    unique_facts = {}
    dupes = 0
    for f in facts:
        key = (f["iso2_code"], f["indicator_code"], f["year"])
        if key not in unique_facts:
            unique_facts[key] = f
        else:
            dupes += 1
    
    logger.info(f"[transform] {dupes} duplicatas removidas.")
    return list(unique_facts.values())