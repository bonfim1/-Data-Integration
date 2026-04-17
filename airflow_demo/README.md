# Airflow Demo — REST Countries ETL

**ESPM — Data Integration — Semana 10**

Pipeline ETL orquestrado com Apache Airflow que extrai dados da [REST Countries API](https://restcountries.com/), transforma e carrega no PostgreSQL.

## Pré-requisitos

- Docker e Docker Compose instalados

## Como executar

```bash
# 1. Subir o ambiente
docker compose up --build

# 2. Acessar a UI do Airflow
# http://localhost:8080
# Usuário: admin | Senha: admin

# 3. A DAG "etl_rest_countries" aparecerá na lista
# Ative-a e dispare manualmente (botão Play)

# 4. Validar dados no PostgreSQL
docker exec -it etl_postgres psql -U etl_user -d etl_db

# Consultas de validação:
# SELECT COUNT(*) FROM countries;
# SELECT name, region, population FROM countries ORDER BY population DESC LIMIT 10;
# SELECT region, COUNT(*) FROM countries GROUP BY region ORDER BY 2 DESC;
```

## Estrutura do projeto

```
airflow_demo/
├── docker-compose.yml    # Serviços: Airflow + PostgreSQL
├── requirements.txt      # Dependências Python
├── .env.example          # Variáveis de ambiente
├── db/
│   └── init.sql          # DDL da tabela countries
├── dags/
│   └── dag_rest_countries.py  # DAG principal
└── src/
    └── __init__.py
```

## Modelo de dados

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| iso2_code | CHAR(2) PK | Código ISO alpha-2 |
| iso3_code | CHAR(3) | Código ISO alpha-3 |
| name | VARCHAR(200) | Nome comum |
| official_name | VARCHAR(300) | Nome oficial |
| region | VARCHAR(80) | Região (ex: Americas) |
| subregion | VARCHAR(80) | Sub-região |
| capital | VARCHAR(150) | Capital |
| population | BIGINT | População |
| area_km2 | NUMERIC | Área em km² |
| languages | TEXT | Idiomas (vírgula) |
| currencies | TEXT | Moedas (vírgula) |
| latitude | NUMERIC | Latitude |
| longitude | NUMERIC | Longitude |
| loaded_at | TIMESTAMP | Data/hora da carga |
