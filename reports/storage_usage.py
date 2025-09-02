from utils.db_connection import get_connection

def get_storage_usage():
    """Get comprehensive storage usage including databases, tables, and indexes"""
    storage_data = {}
    
    # Get per-database storage
    storage_data['databases'] = get_database_storage()
    
    # Get per-table storage
    storage_data['tables'] = get_table_storage()
    
    # Get per-index storage
    storage_data['indexes'] = get_index_storage()
    
    # Get index usage statistics (scans, tuples read/fetched)
    storage_data['index_usage'] = get_index_usage()
    
    return storage_data

def get_database_storage():
    """Get storage usage per database"""
    query = """
    SELECT 
        datname, 
        pg_size_pretty(pg_database_size(datname)) AS size_pretty,
        pg_database_size(datname) AS size_bytes
    FROM pg_database
    WHERE datistemplate = false
    ORDER BY pg_database_size(datname) DESC;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def get_table_storage():
    """Get storage usage per table"""
    query = """
    SELECT 
        t.schemaname,
        t.tablename,
        pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename)) AS total_size_pretty,
        pg_size_pretty(pg_relation_size(t.schemaname||'.'||t.tablename)) AS table_size_pretty,
        pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename) - pg_relation_size(t.schemaname||'.'||t.tablename)) AS index_size_pretty,
        pg_total_relation_size(t.schemaname||'.'||t.tablename) AS total_size_bytes,
        pg_relation_size(t.schemaname||'.'||t.tablename) AS table_size_bytes,
        COALESCE(NULLIF(s.n_live_tup, 0), NULLIF(c.reltuples::bigint, 0), 0) AS row_count
    FROM pg_tables t
    LEFT JOIN pg_stat_all_tables s
      ON t.schemaname = s.schemaname AND t.tablename = s.relname
    LEFT JOIN pg_namespace n
      ON n.nspname = t.schemaname
    LEFT JOIN pg_class c
      ON c.relname = t.tablename AND c.relnamespace = n.oid AND c.relkind IN ('r','p','m','t')
    WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY pg_total_relation_size(t.schemaname||'.'||t.tablename) DESC
    LIMIT 50;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def get_index_storage():
    """Get storage usage per index"""
    query = """
    SELECT 
        schemaname,
        indexname,
        tablename,
        pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) AS size_pretty,
        pg_relation_size(schemaname||'.'||indexname) AS size_bytes
    FROM pg_indexes 
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY pg_relation_size(schemaname||'.'||indexname) DESC
    LIMIT 20;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def get_index_usage():
    """Get index usage statistics (scans, tuples read/fetched) with sizes."""
    query = """
    SELECT 
        n.nspname                            AS schemaname,
        c.relname                            AS tablename,
        i.relname                            AS indexname,
        pg_size_pretty(pg_relation_size(i.oid)) AS size_pretty,
        pg_relation_size(i.oid)              AS size_bytes,
        COALESCE(s.idx_scan, 0)              AS idx_scan,
        COALESCE(s.idx_tup_read, 0)          AS idx_tup_read,
        COALESCE(s.idx_tup_fetch, 0)         AS idx_tup_fetch
    FROM pg_class c
    JOIN pg_index ix        ON ix.indrelid = c.oid
    JOIN pg_class i         ON i.oid = ix.indexrelid
    JOIN pg_namespace n     ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.relkind IN ('r','p')
    ORDER BY COALESCE(s.idx_scan, 0) DESC, pg_relation_size(i.oid) DESC
    LIMIT 50;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
