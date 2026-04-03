import logging
from typing import Any, Dict, List

from sqlalchemy import create_engine, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

from config import settings
from models import Base, Country, Indicator, WdiFact

logger = logging.getLogger(__name__)

# Configuração do Engine com timeout para evitar travamentos
engine = create_engine(
    settings.db_url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 10},
)
# Fábrica de sessões
Session = sessionmaker(bind=engine)

# ---------------------------------------------------------------------------
# Upsert genérico — garante a idempotência exigida
# ---------------------------------------------------------------------------

def upsert_data(session, model, data, index_elements):
    """
    Realiza o Upsert (Insert ou Update) em lote.
    Garante que reexecuções não dupliquem dados.
    """
    if not data:
        logger.warning("[load] %s — nenhum registro para carregar.", model.__tablename__)
        return

    # Cria a instrução de insert para PostgreSQL
    stmt = pg_insert(model).values(data)

    # Define quais colunas devem ser atualizadas em caso de conflito (exclui PKs)
    update_dict = {
        c.name: stmt.excluded[c.name]
        for c in model.__table__.columns
        if not c.primary_key and c.name != "loaded_at"
    }

    # Atualiza o timestamp de carga no banco
    if "loaded_at" in model.__table__.columns:
        update_dict["loaded_at"] = func.now()

    # Monta a cláusula ON CONFLICT
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=index_elements,
        set_=update_dict,
    )
    
    # Executa dentro da sessão atual
    session.execute(upsert_stmt)


# ---------------------------------------------------------------------------
# Ponto de entrada da carga — Com commits independentes
# ---------------------------------------------------------------------------

def load_all(countries, indicators_meta, facts):
    """
    Carrega os dados respeitando a integridade referencial (FKs).
    Cada bloco tem seu próprio 'with session.begin()', garantindo que 
    o que for carregado com sucesso permaneça no banco mesmo em caso de erro posterior.
    """

    # 1. Carregar Países (Dimensão)
    try:
        with Session() as session:
            with session.begin():
                logger.info("[load] carregando países (%d registros)...", len(countries))
                upsert_data(session, Country, countries, ["iso2_code"])
        logger.info("[load] countries — Persistido com sucesso.")
    except Exception as e:
        logger.error(f"[load] erro ao carregar países: {e}")
        # Não interrompe se quiser tentar carregar o resto, mas aqui é base
        raise

    # 2. Carregar Metadados dos Indicadores (Dimensão)
    try:
        with Session() as session:
            with session.begin():
                logger.info("[load] carregando indicadores (%d registros)...", len(indicators_meta))
                upsert_data(session, Indicator, indicators_meta, ["indicator_code"])
        logger.info("[load] indicators — Persistido com sucesso.")
    except Exception as e:
        logger.error(f"[load] erro ao carregar indicadores: {e}")
        raise

    # 3. Carregar Fatos (Série Histórica) em chunks de 1000
    total = len(facts)
    if total > 0:
        logger.info("[load] carregando fatos (%d registros em chunks)...", total)
        for i in range(0, total, 1000):
            chunk = facts[i : i + 1000]
            try:
                with Session() as session:
                    with session.begin():
                        upsert_data(session, WdiFact, chunk, ["iso2_code", "indicator_code", "year"])
                logger.info("[load] wdi_facts — bloco %d/%d processado.", min(i + 1000, total), total)
            except Exception as e:
                logger.error(f"[load] erro no bloco de fatos: {e}")
                # Aqui você decide se para tudo ou continua os próximos blocos
                continue 

    logger.info("[load] processo de carga finalizado.")