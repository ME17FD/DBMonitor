from utils.db_connection import get_connection

def get_replication_delay():
    query = """
    SELECT client_addr, state, write_lag, flush_lag, replay_lag
    FROM pg_stat_replication;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
