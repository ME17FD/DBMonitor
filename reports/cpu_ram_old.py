import psutil
from utils.db_connection import get_connection

def get_cpu_ram_usage():
    """Get both system and PostgreSQL server metrics"""
    system_metrics = {
        "system_cpu_percent": psutil.cpu_percent(interval=1),
        "system_ram_percent": psutil.virtual_memory().percent,
        "system_ram_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "system_ram_used_gb": round(psutil.virtual_memory().used / (1024**3), 2),
        "system_ram_available_gb": round(psutil.virtual_memory().available / (1024**3), 2)
    }
    
    # Get PostgreSQL server metrics
    postgres_metrics = get_postgres_server_metrics()
    
    return {**system_metrics, **postgres_metrics}

def get_postgres_server_metrics():
    """Get PostgreSQL server-specific CPU and memory metrics"""
    try:
        with get_connection() as conn, conn.cursor() as cur:
            # Get PostgreSQL process memory usage
            cur.execute("""
                SELECT 
                    pg_size_pretty(pg_database_size(current_database())) as db_size,
                    (SELECT setting::int FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers_kb,
                    (SELECT setting::int FROM pg_settings WHERE name = 'work_mem') as work_mem_kb,
                    (SELECT setting::int FROM pg_settings WHERE name = 'maintenance_work_mem') as maintenance_work_mem_kb
            """)
            result = cur.fetchone()
            
            # Get active connections and their memory usage
            cur.execute("""
                SELECT 
                    count(*) as active_connections,
                    sum(case when state = 'active' then 1 else 0 end) as active_queries
                FROM pg_stat_activity 
                WHERE state IS NOT NULL
            """)
            conn_result = cur.fetchone()
            
            return {
                "postgres_db_size": result[0] if result else "N/A",
                "postgres_shared_buffers_mb": round(result[1] / 1024, 2) if result and result[1] else 0,
                "postgres_work_mem_mb": round(result[2] / 1024, 2) if result and result[2] else 0,
                "postgres_maintenance_work_mem_mb": round(result[3] / 1024, 2) if result and result[3] else 0,
                "postgres_active_connections": conn_result[0] if conn_result else 0,
                "postgres_active_queries": conn_result[1] if conn_result else 0
            }
    except Exception as e:
        print(f"Warning: Could not get PostgreSQL metrics: {e}")
        return {
            "postgres_db_size": "N/A",
            "postgres_shared_buffers_mb": 0,
            "postgres_work_mem_mb": 0,
            "postgres_maintenance_work_mem_mb": 0,
            "postgres_active_connections": 0,
            "postgres_active_queries": 0
        }
