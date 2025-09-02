from utils.db_connection import get_connection

def get_cache_hit_ratio():
    """Get comprehensive cache hit ratios including total, per-table, and index vs heap ratios"""
    cache_data = {}
    
    # Get total cache hit ratio
    cache_data['total'] = get_total_cache_hit_ratio()
    
    # Get per-table cache hit ratios
    cache_data['per_table'] = get_per_table_cache_hit_ratio()
    
    # Get index vs heap ratio
    cache_data['index_heap_ratio'] = get_index_heap_ratio()
    
    return cache_data

def get_total_cache_hit_ratio():
    """Get overall cache hit ratio"""
    query = """
    SELECT 
        sum(blks_hit) AS hits, 
        sum(blks_read) AS reads,
        (sum(blks_hit) / nullif(sum(blks_hit) + sum(blks_read), 0)) as ratio
    FROM pg_stat_database;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()

def get_per_table_cache_hit_ratio():
    """Get cache hit ratio per table"""
    query = """
    SELECT 
        schemaname,
        relname as tablename,
        heap_blks_hit,
        heap_blks_read,
        CASE 
            WHEN (heap_blks_hit + heap_blks_read) > 0 
            THEN ROUND((heap_blks_hit::numeric / (heap_blks_hit + heap_blks_read)) * 100, 2)
            ELSE 0 
        END as hit_ratio_percent
    FROM pg_statio_user_tables
    WHERE (heap_blks_hit + heap_blks_read) > 0
    ORDER BY hit_ratio_percent ASC
    LIMIT 20;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def get_index_heap_ratio():
    """Get index vs heap usage ratio"""
    query = """
    SELECT 
        schemaname,
        relname as tablename,
        heap_blks_hit + heap_blks_read as heap_blocks,
        idx_blks_hit + idx_blks_read as index_blocks,
        CASE 
            WHEN (heap_blks_hit + heap_blks_read + idx_blks_hit + idx_blks_read) > 0 
            THEN ROUND(((idx_blks_hit + idx_blks_read)::numeric / (heap_blks_hit + heap_blks_read + idx_blks_hit + idx_blks_read)) * 100, 2)
            ELSE 0 
        END as index_heap_ratio_percent
    FROM pg_statio_user_tables
    WHERE (heap_blks_hit + heap_blks_read) > 0
    ORDER BY index_heap_ratio_percent DESC
    LIMIT 20;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
