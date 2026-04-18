"""
=========================================================
ESPM — Data Integration — Semana 10
DAG: Pipeline ETL — REST Countries API → PostgreSQL
=========================================================

Esta DAG demonstra os conceitos fundamentais do Airflow:
  - @dag e @task (TaskFlow API)
  - Dependências implícitas via retorno/parâmetro
  - XComs automáticos
  - Conexão com banco externo via variáveis de ambiente

API utilizada: https://restcountries.com/v3.1/all (sem autenticação)
"""

from __future__ import annotations


import logging # Para logging estruturado e consistente em todas as tasks
import os
from datetime import datetime

from airflow.decorators import dag, task # Para definir DAGs e tasks usando a TaskFlow API

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Configurações (lidas de variáveis de ambiente)
# -------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("ETL_DB_HOST", "postgres"),
    "port": int(os.getenv("ETL_DB_PORT", "5432")),
    "user": os.getenv("ETL_DB_USER", "etl_user"),
    "password": os.getenv("ETL_DB_PASS", "etl_pass"),
    "database": os.getenv("ETL_DB_NAME", "etl_db"),
}

API_URL = "https://restcountries.com/v3.1/all"

API_FIELDS = (
    "cca2,cca3,name,capital,languages,currencies,latlng,region,population,area"
    )
# Campos adicionais disponíveis: "subregion,flags,demonyms,timezones,continents"
# -------------------------------------------------------
# DAG principal
# -------------------------------------------------------
@dag(
    dag_id="etl_rest_countries",
    description="Pipeline ETL: REST Countries API → PostgreSQL",
    schedule="@daily",
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["etl", "demo", "semana10"],
    default_args={
        "owner": "espm",
        "retries": 2,
    },
)
def etl_rest_countries():

    # ---------------------------------------------------
    # Task 1: EXTRACT — Consumir a API REST Countries
    # ---------------------------------------------------
    @task()
    def extract() -> list[dict]:
        """
        Extrai a lista completa de países da REST Countries API.
        Retorna uma lista de dicionários com os dados brutos.
        """
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        # Retry com backoff exponencial
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503])
        session.mount("https://", HTTPAdapter(max_retries=retry))

        logger.info("Extraindo dados de %s", API_URL)
        response = session.get(API_URL, timeout=30)
        response.raise_for_status()

        raw_data = response.json()
        logger.info("Extraídos %d registros da API", len(raw_data))
        return raw_data

    # ---------------------------------------------------
    # Task 2: TRANSFORM — Limpar e normalizar
    # ---------------------------------------------------
    @task()
    def transform(raw_data: list[dict]) -> list[dict]:
        """
        Aplica transformações nos dados brutos:
        - Filtra campos relevantes
        - Normaliza listas (idiomas, moedas) para strings
        - Trata valores ausentes
        - Descarta registros sem código ISO alpha-2
        """
        transformed = []
        discarded = 0

        for country in raw_data:
            # Filtro: precisa ter código ISO alpha-2 de 2 caracteres
            cca2 = country.get("cca2", "")
            if not cca2 or len(cca2) != 2: # cca2 documentação da api
                discarded += 1
                continue # sai do loop atual e vai para o próximo país

            # Extrair nome comum e oficial
            name_data = country.get("name", {})
            name = name_data.get("common", "Unknown")
            official = name_data.get("official", name)

            # Extrair capital (pode ser lista)
            capitals = country.get("capital", [])
            capital = capitals[0] if capitals else None

            # Idiomas: dict → string separada por vírgula
            languages_dict = country.get("languages", {})
            languages = ", ".join(sorted(languages_dict.values())) if languages_dict else None

            # Moedas: dict → string separada por vírgula
            currencies_dict = country.get("currencies", {})
            currencies = ", ".join(
                f"{v.get('name', k)} ({v.get('symbol', '')})"
                for k, v in currencies_dict.items()
            ) if currencies_dict else None

            # Coordenadas
            latlng = country.get("latlng", [None, None])
            lat = latlng[0] if len(latlng) > 0 else None
            lng = latlng[1] if len(latlng) > 1 else None

            # Converter população e área com tratamento seguro
            def safe_int(val):
                try:
                    return int(val) if val is not None else None
                except (ValueError, TypeError):
                    return None

            def safe_float(val):
                try:
                    return float(val) if val is not None else None
                except (ValueError, TypeError):
                    return None

            record = {
                "iso2_code": cca2.upper().strip(),
                "iso3_code": country.get("cca3", "").upper().strip() or None,
                "name": name.strip(),
                "official_name": official.strip(),
                "region": (country.get("region") or "").strip().title() or None,
                "subregion": (country.get("subregion") or "").strip().title() or None,
                "capital": capital.strip() if capital else None,
                "population": safe_int(country.get("population")),
                "area_km2": safe_float(country.get("area")),
                "languages": languages,
                "currencies": currencies,
                "latitude": safe_float(lat),
                "longitude": safe_float(lng),
            }
            transformed.append(record)

        logger.info(
            "Transformação concluída: %d registros aceitos, %d descartados",
            len(transformed), discarded
        )
        return transformed

    # ---------------------------------------------------
    # Task 3: LOAD — Upsert no PostgreSQL via SQLAlchemy
    # ---------------------------------------------------
    @task()
    def load(data: list[dict]) -> None:
        """
        Carrega os dados transformados no PostgreSQL usando
        SQLAlchemy com estratégia de upsert (ON CONFLICT DO UPDATE).
        """
        from sqlalchemy import create_engine, text
        from sqlalchemy.dialects.postgresql import insert

        # Montar connection string
        conn_str = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(conn_str)

        logger.info("Conectando ao PostgreSQL em %s:%s", DB_CONFIG["host"], DB_CONFIG["port"])

        with engine.begin() as conn:
            # Upsert em lote
            if data:
                stmt = text("""
                    INSERT INTO countries (
                        iso2_code, iso3_code, name, official_name,
                        region, subregion, capital, population, area_km2,
                        languages, currencies, latitude, longitude, loaded_at
                    ) VALUES (
                        :iso2_code, :iso3_code, :name, :official_name,
                        :region, :subregion, :capital, :population, :area_km2,
                        :languages, :currencies, :latitude, :longitude, NOW()
                    )
                    ON CONFLICT (iso2_code) DO UPDATE SET
                        iso3_code = EXCLUDED.iso3_code,
                        name = EXCLUDED.name,
                        official_name = EXCLUDED.official_name,
                        region = EXCLUDED.region,
                        subregion = EXCLUDED.subregion,
                        capital = EXCLUDED.capital,
                        population = EXCLUDED.population,
                        area_km2 = EXCLUDED.area_km2,
                        languages = EXCLUDED.languages,
                        currencies = EXCLUDED.currencies,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        loaded_at = NOW()
                """)
                conn.execute(stmt, data)
                logger.info("Upsert concluído: %d registros carregados", len(data))

            # Validação rápida
            result = conn.execute(text("SELECT COUNT(*) FROM countries"))
            total = result.scalar()
            logger.info("Total de países no banco: %d", total)

    # ---------------------------------------------------
    # Encadeamento das tasks (define a DAG)
    # ---------------------------------------------------
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)


# Instanciar a DAG
etl_rest_countries()
