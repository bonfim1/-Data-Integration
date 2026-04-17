-- =========================================================
-- ESPM - Data Integration - Semana 10
-- DDL: Tabela de países (destino do pipeline)
-- =========================================================

CREATE TABLE IF NOT EXISTS countries (
    iso2_code     CHAR(2) PRIMARY KEY,
    iso3_code     CHAR(3),
    name          VARCHAR(200) NOT NULL,
    official_name VARCHAR(300),
    region        VARCHAR(80),
    subregion     VARCHAR(80),
    capital       VARCHAR(150),
    population    BIGINT,
    area_km2      NUMERIC(12,2),
    languages     TEXT,            -- idiomas separados por vírgula
    currencies    TEXT,            -- moedas separadas por vírgula
    latitude      NUMERIC(9,4),
    longitude     NUMERIC(9,4),
    loaded_at     TIMESTAMP DEFAULT NOW()
);

-- Índice para consultas por região
CREATE INDEX IF NOT EXISTS idx_countries_region ON countries(region);
