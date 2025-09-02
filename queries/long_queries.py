from utils.db_connection import get_connection

def get_long_queries(threshold_ms=600, limit=10):
    """
    Get queries exceeding a threshold (ms) with full query text and proper time formatting.
    """
    query = f"""
    SELECT 
        query,
        total_exec_time,
        mean_exec_time,
        calls,
        CASE 
            WHEN total_exec_time >= 60000 THEN ROUND((total_exec_time/60000)::numeric, 2) || 'm'
            WHEN total_exec_time >= 1000 THEN ROUND((total_exec_time/1000)::numeric, 2) || 's'
            ELSE ROUND(total_exec_time::numeric, 2) || 'ms'
        END as total_time_formatted,
        CASE 
            WHEN mean_exec_time >= 60000 THEN ROUND((mean_exec_time/60000)::numeric, 2) || 'm'
            WHEN mean_exec_time >= 1000 THEN ROUND((mean_exec_time/1000)::numeric, 2) || 's'
            ELSE ROUND(mean_exec_time::numeric, 2) || 'ms'
        END as mean_time_formatted
    FROM pg_stat_statements
    WHERE mean_exec_time > {threshold_ms}
    ORDER BY mean_exec_time DESC
    LIMIT {limit};
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
