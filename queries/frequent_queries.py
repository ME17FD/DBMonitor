from utils.db_connection import get_connection

def get_frequent_queries(limit=10):
    """
    Top most frequently called queries with proper time formatting.
    """
    query = f"""
    SELECT 
        query, 
        calls, 
        total_exec_time,
        CASE 
            WHEN total_exec_time >= 60000 THEN ROUND((total_exec_time/60000)::numeric, 2) || 'm'
            WHEN total_exec_time >= 1000 THEN ROUND((total_exec_time/1000)::numeric, 2) || 's'
            ELSE ROUND(total_exec_time::numeric, 2) || 'ms'
        END as total_time_formatted,
        CASE 
            WHEN calls > 0 AND (total_exec_time/calls) >= 60000 THEN ROUND(((total_exec_time/calls)/60000)::numeric, 2) || 'm'
            WHEN calls > 0 AND (total_exec_time/calls) >= 1000 THEN ROUND(((total_exec_time/calls)/1000)::numeric, 2) || 's'
            WHEN calls > 0 THEN ROUND((total_exec_time/calls)::numeric, 2) || 'ms'
            ELSE '0ms'
        END as avg_time_per_call
    FROM pg_stat_statements
    ORDER BY calls DESC
    LIMIT {limit};
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
