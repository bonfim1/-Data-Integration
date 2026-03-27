# Projeto ETL com Docker + Python + PostgreSQL

## Objetivo
Consumir dados da API pública Open Brewery DB, transformar os dados e carregar em PostgreSQL.

## Stack
- Python 3.11
- Docker
- Docker Compose
- PostgreSQL

## Estrutura
- `src/extract.py`: extração da API
- `src/transform.py`: limpeza e transformação
- `src/load.py`: carga no banco
- `src/main.py`: orquestração do ETL
- `lecture/lecture.tex`: material teórico em LaTeX

## Como executar

### 1. Subir containers
```bash
docker compose up --build
```

### 2. Ver logs
```bash
docker compose logs -f etl_app
```

### 3. Acessar o banco
```bash
docker exec -it etl_postgres psql -U etl_user -d etl_db
```

### 4. Consultar dados
```sql
SELECT COUNT(*) FROM breweries;
SELECT * FROM breweries LIMIT 10;
```

## Melhorias possíveis
- incluir tabela staging
- incluir logs estruturados
- adicionar testes unitários
- agendar execução com cron ou Airflow
- persistir dados brutos em JSON
