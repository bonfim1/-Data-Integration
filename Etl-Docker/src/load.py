from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch


def get_connection():
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password
    )

def load_data(records: List[Dict[str, Any]]) -> None:
    if not records:
        print("[load] no records to load")
        return
    sql= """
    """
    