import time
import logging
from typing import Any, Dict, Generator, List

import requests

from config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilitário de requisição com retry e backoff exponencial
# ---------------------------------------------------------------------------

def _get_with_retry(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Realiza GET com até `settings.retry_attempts` tentativas.
    Backoff: 2s, 4s, 8s (exponencial).
    """
    for attempt in range(1, settings.retry_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=settings.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            wait = 2 ** attempt  # 2, 4, 8 segundos
            logger.warning(
                "[extract] tentativa %d/%d falhou — %s. Aguardando %ds...",
                attempt, settings.retry_attempts, exc, wait,
            )
            if attempt == settings.retry_attempts:
                raise RuntimeError(
                    f"Falha após {settings.retry_attempts} tentativas: {url} | {exc}"
                ) from exc
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Extração de países
# ---------------------------------------------------------------------------

def fetch_countries() -> List[Dict[str, Any]]:
    """
    Retorna TODOS os registros do endpoint /v2/country (incluindo agregados).
    A filtragem de entidades reais (iso2 com 2 chars) ocorre em transform.py.
    """
    url = f"{settings.api_base_url}/country"
    page = 1
    all_records: List[Dict[str, Any]] = []

    while True:
        params = {"format": "json", "per_page": settings.per_page_countries, "page": page}
        data = _get_with_retry(url, params)

        # data[0] = meta, data[1] = lista de países
        meta = data[0]
        records = data[1] or []

        all_records.extend(records)
        logger.info(
            "[extract] countries — página %d/%d | %d registros nesta página",
            page, meta.get("pages", "?"), len(records),
        )

        if page >= meta.get("pages", 1):
            break
        page += 1

    logger.info("[extract] countries — total bruto extraído: %d", len(all_records))
    return all_records


# ---------------------------------------------------------------------------
# Extração de série histórica por indicador
# ---------------------------------------------------------------------------

def fetch_indicator(indicator_code: str) -> List[Dict[str, Any]]:
    """
    Retorna todos os registros históricos de um indicador para todos os países.
    Usa mrv (most recent values) para limitar ao período relevante.
    """
    url = f"{settings.api_base_url}/country/all/indicator/{indicator_code}"
    page = 1
    all_records: List[Dict[str, Any]] = []

    while True:
        params = {
            "format": "json",
            "per_page": settings.per_page_indicators,
            "mrv": settings.mrv,
            "page": page,
        }
        data = _get_with_retry(url, params)

        meta = data[0]
        records = data[1] or []

        all_records.extend(records)
        logger.info(
            "[extract] %s — página %d/%d | %d registros nesta página",
            indicator_code, page, meta.get("pages", "?"), len(records),
        )

        if page >= meta.get("pages", 1):
            break
        page += 1

    nulls = sum(1 for r in all_records if r.get("value") is None)
    logger.info(
        "[extract] %s — total: %d registros | %d nulos (%.1f%%)",
        indicator_code,
        len(all_records),
        nulls,
        100 * nulls / len(all_records) if all_records else 0,
    )
    return all_records


# ---------------------------------------------------------------------------
# Ponto de entrada da extração completa
# ---------------------------------------------------------------------------

def extract_all() -> Dict[str, Any]:
    """
    Executa a extração completa:
      - países (brutos, com agregados)
      - série histórica dos 5 indicadores obrigatórios

    Retorna dict com chaves 'countries' e 'indicators'.
    """
    logger.info("[extract] iniciando extração da World Bank API...")

    raw_countries = fetch_countries()

    raw_indicators: Dict[str, List[Dict[str, Any]]] = {}
    for ind in settings.indicators:
        code = ind["code"]
        logger.info("[extract] extraindo indicador: %s", code)
        raw_indicators[code] = fetch_indicator(code)

    logger.info("[extract] extração concluída.")
    return {"countries": raw_countries, "indicators": raw_indicators}
