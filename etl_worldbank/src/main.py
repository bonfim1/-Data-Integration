import logging
import sys

from extract import extract_all
from transform import transform_countries, transform_indicators
from load import load_all
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def run_etl():
    logger.info("=" * 60)
    logger.info("[main] iniciando pipeline ETL — World Bank API v2")
    logger.info("=" * 60)

    try:
        # 1. Extração
        raw = extract_all()

        # 2. Transformação
        clean_countries = transform_countries(raw["countries"])
        clean_facts = transform_indicators(raw["indicators"])

        # Dimensão de indicadores vem do config (estática)
        formatted_meta = [
            {"indicator_code": i["code"], "indicator_name": i["name"], "unit": i["unit"]}
            for i in settings.indicators
        ]

        # 3. Carga
        load_all(clean_countries, formatted_meta, clean_facts)

        logger.info("=" * 60)
        logger.info("[main] pipeline finalizado com sucesso.")
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("[main] pipeline abortado com erro: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_etl()