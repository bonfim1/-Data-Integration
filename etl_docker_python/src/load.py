from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from config import settings


def get_connection():
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password
    )


def load_data(records: List[Dict[str, Any]]) -> None:
    if not records:
        print("[load] nenhum registro para carregar.")
        return

    sql = """
    INSERT INTO breweries (
        brewery_id,
        name,
        brewery_type,
        street,
        city,
        state,
        postal_code,
        country,
        longitude,
        latitude,
        phone,
        website_url,
        state_province,
        updated_at
    ) VALUES (
        %(brewery_id)s,
        %(name)s,
        %(brewery_type)s,
        %(street)s,
        %(city)s,
        %(state)s,
        %(postal_code)s,
        %(country)s,
        %(longitude)s,
        %(latitude)s,
        %(phone)s,
        %(website_url)s,
        %(state_province)s,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (brewery_id)
    DO UPDATE SET
        name = EXCLUDED.name,
        brewery_type = EXCLUDED.brewery_type,
        street = EXCLUDED.street,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        postal_code = EXCLUDED.postal_code,
        country = EXCLUDED.country,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        phone = EXCLUDED.phone,
        website_url = EXCLUDED.website_url,
        state_province = EXCLUDED.state_province,
        updated_at = CURRENT_TIMESTAMP;
    """

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            execute_batch(cursor, sql, records, page_size=100)
        conn.commit()
        print(f"[load] {len(records)} registros carregados com sucesso.")
    except Exception as exc:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Erro ao carregar dados no banco: {exc}") from exc
    finally:
        if conn:
            conn.close()
