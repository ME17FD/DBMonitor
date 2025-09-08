from pathlib import Path
import re
import psutil
import yaml
from utils.db_connection import get_connection
import  paramiko

def get_cpu_ram_usage():
    """Get both system and PostgreSQL server metrics"""
    
    # Check if we're connecting to localhost
    if _is_localhost_connection():
        # Use local psutil for localhost connections
        system_metrics = {
            "system_cpu_percent": psutil.cpu_percent(interval=1),
            "system_ram_percent": psutil.virtual_memory().percent,
            "system_ram_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "system_ram_used_gb": round(psutil.virtual_memory().used / (1024**3), 2),
            "system_ram_available_gb": round(psutil.virtual_memory().available / (1024**3), 2)
        }
    else:
        # Get system metrics from remote PostgreSQL server
        system_metrics = _get_remote_system_metrics()
    
    # Get PostgreSQL server metrics
    postgres_metrics = get_postgres_server_metrics()
    
    return {**system_metrics, **postgres_metrics}

def _is_localhost_connection():
    """Check if the database connection is to localhost"""
    config_file = Path(__file__).parent.parent / "config" / "db_config.yaml"
    with open(config_file, "r") as f:
        db= yaml.safe_load(f)["database"]
        localhost_hosts = ['localhost', '127.0.0.1', '0.0.0.1', '::1']

        return db['host'] in localhost_hosts
    


def _get_remote_system_metrics():
    """Get system metrics from the remote PostgreSQL server via SSH (Linux commands)"""
    try:
        # Load DB config to get SSH connection details
        config_file = Path(__file__).parent.parent / "config" / "db_config.yaml"
        with open(config_file, "r") as f:
            db = yaml.safe_load(f)["database"]

        ssh_host = db["host"]
        ssh_user = db.get("ssh_user", "postgres")   # default user, can override in config
        ssh_pass = db.get("ssh_password")           # or use key
        ssh_key  = db.get("ssh_key")                # optional private key path
        ssh_port = db.get("ssh_port")
        # Setup SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if ssh_key:
            ssh.connect(ssh_host, username=ssh_user, key_filename=ssh_key, timeout=5,port=ssh_port)
        else:
            ssh.connect(ssh_host, username=ssh_user, password=ssh_pass, timeout=5,port=ssh_port)

        # Run commands to get CPU and memory info
        # CPU usage (instant snapshot)
        stdin, stdout, stderr = ssh.exec_command("top -bn1 | grep 'Cpu(s)'")
        cpu_line = stdout.read().decode()
        cpu_match = re.search(r"(\d+\.\d+)\s*id", cpu_line)
        if cpu_match:
            idle = float(cpu_match.group(1))
            cpu_percent = round(100 - idle, 2)
        else:
            cpu_percent = 0.0

        # Memory usage
        stdin, stdout, stderr = ssh.exec_command("free -m")
        mem_lines = stdout.read().decode().splitlines()
        mem_info = mem_lines[1].split()
        total_mb, used_mb, free_mb = map(int, mem_info[1:4])
        ram_percent = round((used_mb / total_mb) * 100, 2)

        ssh.close()

        return {
            "system_cpu_percent": cpu_percent,
            "system_ram_percent": ram_percent,
            "system_ram_total_gb": round(total_mb / 1024, 2),
            "system_ram_used_gb": round(used_mb / 1024, 2),
            "system_ram_available_gb": round(free_mb / 1024, 2),
        }

    except Exception as e:
        print(f"Warning: Could not get remote system metrics via SSH: {e}")
        return {
            "system_cpu_percent": 0.0,
            "system_ram_percent": 0.0,
            "system_ram_total_gb": 0.0,
            "system_ram_used_gb": 0.0,
            "system_ram_available_gb": 0.0
        }

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