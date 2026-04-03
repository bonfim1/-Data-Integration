# World Bank ETL — Painel de Indicadores Socioeconômicos

Este projeto implementa um pipeline de Engenharia de Dados robusto para extrair, transformar e carregar indicadores do Banco Mundial. O objetivo é alimentar um banco de dados relacional estruturado que permita análises comparativas entre países sobre desenvolvimento econômico, saúde e educação.

## Visão Geral

O sistema consome a World Bank Data API v2, processa séries históricas de indicadores chave (como PIB per capita e gastos em saúde) e organiza essas informações em um modelo de dados dimensional no PostgreSQL. O pipeline foi desenhado para ser idempotente: você pode executá-lo quantas vezes quiser sem duplicar dados ou corromper a integridade das informações.

---

## Arquitetura e Tecnologias

- **Linguagem:** Python 3.11
- **Banco de Dados:** PostgreSQL 16 (via Docker)
- **ORM:** SQLAlchemy 2.0 (com DeclarativeBase)
- **Infraestrutura:** Docker & Docker Compose

---

## Modelo de Dados

**Abordagem escolhida: ORM com DeclarativeBase (SQLAlchemy 2.0)**

O ORM permite definir as tabelas como classes Python tipadas, centralizando schema e lógica em um único arquivo (`models.py`). Para um pipeline com tabelas bem definidas e sem queries ad-hoc complexas em tempo de execução, o ORM oferece melhor legibilidade e manutenção sem perda de performance relevante. O Core seria mais adequado apenas para volumes muito maiores (10M+ registros).

### Estrutura das tabelas

| Tabela       | Coluna           | Tipo           | Restrição          |
|--------------|------------------|----------------|--------------------|
| `countries`  | iso2_code        | CHAR(2)        | PK                 |
|              | iso3_code        | CHAR(3)        |                    |
|              | name             | VARCHAR(100)   | NOT NULL           |
|              | region           | VARCHAR(80)    |                    |
|              | income_group     | VARCHAR(60)    |                    |
|              | capital          | VARCHAR(80)    |                    |
|              | longitude        | NUMERIC(9,4)   |                    |
|              | latitude         | NUMERIC(9,4)   |                    |
|              | loaded_at        | TIMESTAMP      |                    |
| `indicators` | indicator_code   | VARCHAR(40)    | PK                 |
|              | indicator_name   | TEXT           | NOT NULL           |
|              | unit             | VARCHAR(30)    |                    |
| `wdi_facts`  | iso2_code        | CHAR(2)        | PK, FK→countries   |
|              | indicator_code   | VARCHAR(40)    | PK, FK→indicators  |
|              | year             | SMALLINT       | PK, NOT NULL       |
|              | value            | NUMERIC(18,4)  | NULL permitido     |
|              | loaded_at        | TIMESTAMP      |                    |

A chave primária composta `(iso2_code, indicator_code, year)` em `wdi_facts` é o que garante a idempotência do pipeline — reexecutar nunca duplica dados.

---

## Regras de Transformação

Para garantir a qualidade dos dados, aplicamos cinco camadas de tratamento antes da carga final.

**T1 — Filtro de Países Reais:** Removemos agregados regionais (ex: "América Latina", "East Asia") focando apenas em países individuais. O critério é duplo: o código ISO deve ter exatamente 2 caracteres *e* o país deve ter um grupo de renda válido (LIC, MIC, HIC). Isso elimina tanto agregados com 3 letras como `EAS` quanto agregados de 2 letras como `XC` (Euro area) que não representam um país individual.

**T2 — Padronização de Strings:** Limpeza de espaços em branco com `strip()` e padronização de nomes de região para Title Case, evitando duplicatas visuais nas queries de agrupamento.

**T3 — Tipagem Segura:** Conversão das strings retornadas pela API para os tipos corretos (`int` para ano, `float` para valores e coordenadas), sempre com `try/except` para que um valor malformado não aborte o pipeline inteiro — retornando `None` em caso de falha.

**T4 — Janela Temporal:** Mantemos apenas registros com ano entre 2010 e o ano corrente, descartando dados históricos muito antigos e mantendo o banco focado em análises contemporâneas.

**T5 — Garantia de Unicidade:** Antes da carga, removemos qualquer registro duplicado na mesma tríade `(país, indicador, ano)`, mantendo sempre a ocorrência mais recente. O número de duplicatas encontradas é registrado em log.

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Acesso à internet (API pública, sem autenticação)

### 1. Clonar e configurar

```bash
git clone <repo>
cd etl_worldbank
copy .env.example .env
```

### 2. Subir os containers

```bash
docker compose up --build
```

O `etl_app` aguarda o PostgreSQL estar saudável (`healthcheck`) antes de iniciar. O pipeline pode levar alguns minutos para concluir a extração dos 5 indicadores.

### 3. Acompanhar os logs

```bash
docker compose logs -f etl_app
```

### 4. Acessar o banco

```bash
docker exec -it etl_wb_postgres psql -U etl_user -d etl_worldbank
```

### 5. Executar as queries de validação

```sql
SELECT COUNT(*) FROM countries;

SELECT income_group, COUNT(*)
FROM countries
GROUP BY income_group
ORDER BY 2 DESC;

SELECT indicator_code,
       COUNT(*)                                        AS obs,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nulls
FROM wdi_facts
GROUP BY indicator_code;

SELECT c.name, f.year, f.value
FROM wdi_facts f
JOIN countries c ON c.iso2_code = f.iso2_code
WHERE f.indicator_code = 'NY.GDP.PCAP.KD'
  AND c.iso2_code IN ('BR','US','CN','DE','NG')
ORDER BY c.name, f.year;

-- Idempotência: reexecute o pipeline e compare
SELECT COUNT(*) FROM wdi_facts;
```

### 6. Parar e remover os containers

```bash
docker compose down      # mantém o volume de dados
docker compose down -v   # remove o volume também
```

---

## Consultas de Validação

Saída esperada após a primeira execução (valores aproximados — dependem da disponibilidade da API no momento da execução):

**Volume de países carregados**
```
 count
-------
  217
```

**Distribuição por grupo de renda**
```
 income_group | count
--------------+-------
 HIC          |    82
 UMC          |    55
 LMC          |    53
 LIC          |    27
```

**Volume e taxa de nulos por indicador**
```
    indicator_code      | obs  | nulls
------------------------+------+-------
 NY.GDP.PCAP.KD         | 1850 |   120
 SP.POP.TOTL            | 1850 |    10
 SH.XPD.CHEX.GD.ZS      | 1850 |   200
 SE.XPD.TOTL.GD.ZS      | 1850 |   450
 EG.ELC.ACCS.ZS         | 1850 |   180
```

**PIB per capita dos 5 países de referência**
```
     name      | year |    value
---------------+------+------------
 Brazil        | 2010 |  8196.4300
 Brazil        | 2022 |  8897.4500
 China         | 2010 |  4550.2100
 ...
```

**Idempotência — COUNT idêntico após segunda execução**
```
 count
-------
  9250   ← mesmo valor nas duas execuções
```

---

## Decisões Técnicas & Trade-offs

**SQLAlchemy ORM vs Core:** Optamos pelo ORM pela legibilidade e pela facilidade de gerenciar o schema como classes Python tipadas. O Core seria mais performático para volumes acima de 10 milhões de registros, mas para o volume do Banco Mundial o ORM é suficiente e muito mais legível.

**Upsert com ON CONFLICT:** Implementado nas três tabelas para permitir reexecuções seguras. Se o dado já existe, ele é atualizado; se não, é inserido. Isso garante idempotência total sem necessidade de truncar o banco antes de cada carga.

**Retry com Backoff Exponencial:** Se a API falhar temporariamente, o sistema aguarda 2s, depois 4s, depois 8s antes de desistir. Isso respeita o servidor da API em momentos de instabilidade sem travar o pipeline indefinidamente.

**Transações independentes por tabela:** Cada tabela é carregada em sua própria transação. Se `wdi_facts` falhar, `countries` e `indicators` já estão persistidos — o rollback não desfaz o trabalho já concluído.

**Carga em Chunks de 1.000 registros:** Os fatos são enviados ao banco em blocos de 1.000 registros, evitando statements SQL gigantes que causam timeout ou estouro de memória.

**Filtro T1 duplo (len + income_group):** Apenas checar `len(id) == 2` não é suficiente — a API retorna entidades como `XC` (Euro area) e `OE` (OECD) com 2 caracteres que são agregados. O filtro adicional por `income_group` elimina esses casos de forma confiável.
