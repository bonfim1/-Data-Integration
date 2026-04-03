import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Settings:
    # API
    api_base_url: str = os.getenv("API_BASE_URL", "https://api.worldbank.org/v2")
    per_page_countries: int = int(os.getenv("PER_PAGE_COUNTRIES", "300"))
    per_page_indicators: int = int(os.getenv("PER_PAGE_INDICATORS", "100"))
    mrv: int = int(os.getenv("MRV", "10"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "60"))
    retry_attempts: int = int(os.getenv("RETRY_ATTEMPTS", "3"))

    # DB
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "etl_worldbank")
    db_user: str = os.getenv("DB_USER", "etl_user")
    db_password: str = os.getenv("DB_PASSWORD", "etl_pass")

    # Filtros temporais
    year_min: int = int(os.getenv("YEAR_MIN", "2010"))

    # Grupos de renda aceitos
    income_groups: List[str] = field(
        default_factory=lambda: ["LIC", "MIC", "HIC", "UMC", "LMC"]
    )

    # Indicadores obrigatórios
    indicators: List[dict] = field(default_factory=lambda: [
        {"code": "NY.GDP.PCAP.KD", "name": "GDP per capita (constant 2015 US$)",          "unit": "USD"},
        {"code": "SP.POP.TOTL",    "name": "Population, total",                            "unit": "people"},
        {"code": "SH.XPD.CHEX.GD.ZS", "name": "Current health expenditure (% of GDP)",   "unit": "% GDP"},
        {"code": "SE.XPD.TOTL.GD.ZS", "name": "Government expenditure on education (% of GDP)", "unit": "% GDP"},
        {"code": "EG.ELC.ACCS.ZS", "name": "Access to electricity (% of population)",    "unit": "%"},
    ])

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()
