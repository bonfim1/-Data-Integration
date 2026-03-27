import time
import requests
from typing import List, Dict, Any
from config import settings


def fetch_page(page: int, per_page: int) -> List[Dict[str, Any]]:
    url = settings.api_base_url
    params = {"page": page, "per_page": per_page}

    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("Resposta da API não está no formato esperado (lista).")

            return data
        except Exception as exc:
            print(f"[extract] tentativa {attempt + 1}/3 falhou na página {page}: {exc}")
            time.sleep(2)

    raise RuntimeError(f"Falha ao extrair dados da página {page} após 3 tentativas.")


def extract_all() -> List[Dict[str, Any]]:
    all_data = []

    for page in range(1, settings.max_pages + 1):
        page_data = fetch_page(page=page, per_page=settings.per_page)

        if not page_data:
            print(f"[extract] página {page} vazia. Encerrando extração.")
            break

        print(f"[extract] página {page}: {len(page_data)} registros extraídos.")
        all_data.extend(page_data)

    print(f"[extract] total extraído: {len(all_data)} registros.")
    return all_data
