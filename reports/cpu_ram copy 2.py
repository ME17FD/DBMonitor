import psutil
import paramiko
import json
import socket
from utils.db_connection import get_connection

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
        # Get exact system metrics from remote server
        system_metrics = _get_remote_system_metrics()
    
    # Get PostgreSQL server metrics
    postgres_metrics = get_postgres_server_metrics()
    
    return {**system_metrics, **postgres_metrics}

def _is_localhost_connection():
    """Check if the database connection is to localhost"""
    try:
        with get_connection() as conn:
            # Get the connection info
            info = conn.get_dsn_parameters()
            host = info.get('host', 'localhost').lower()
            
            # Check for localhost variations
            localhost_hosts = ['localhost', '127.0.0.1', '0.0.0.1', '::1']
            return host in localhost_hosts
    except Exception:
        # If we can't determine, assume remote
        return False

def _get_remote_system_metrics():
    """Get exact system metrics from remote server via SSH"""
    try:
        # Get database connection info to determine remote host
        with get_connection() as conn:
            info = conn.get_dsn_parameters()
            remote_host = info.get('host')
            
        if not remote_host:
            raise Exception("Could not determine remote host")
            
        # Execute remote psutil script via SSH
        system_metrics = _execute_remote_psutil(remote_host)
        return system_metrics
        
    except Exception as e:
        print(f"Warning: Could not get remote system metrics: {e}")
        return {
            "system_cpu_percent": 0.0,
            "system_ram_percent": 0.0,
            "system_ram_total_gb": 0.0,
            "system_ram_used_gb": 0.0,
            "system_ram_available_gb": 0.0
        }

def _get_docker_container_metrics(db_config):
    """Get exact system metrics from Docker container"""
    
    # Remote Python script to get exact system metrics
    remote_script = '''
import psutil
import json

try:
    # Get CPU usage with 1 second interval for accuracy
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Get memory information
    memory = psutil.virtual_memory()
    
    metrics = {
        "system_cpu_percent": cpu_percent,
        "system_ram_percent": memory.percent,
        "system_ram_total_gb": round(memory.total / (1024**3), 2),
        "system_ram_used_gb": round(memory.used / (1024**3), 2),
        "system_ram_available_gb": round(memory.available / (1024**3), 2)
    }
    
    print(json.dumps(metrics))
    
except Exception as e:
    error_metrics = {
        "system_cpu_percent": 0.0,
        "system_ram_percent": 0.0,
        "system_ram_total_gb": 0.0,
        "system_ram_used_gb": 0.0,
        "system_ram_available_gb": 0.0,
        "error": str(e)
    }
    print(json.dumps(error_metrics))
'''

    try:
        # Method 1: Try Docker API (if Docker daemon is accessible)
        try:
            client = docker.from_env()
            
            # Find the PostgreSQL container by port mapping or name
            container = None
            db_port = db_config.get('port', 5432)
            
            # Try to find container by port mapping
            for c in client.containers.list():
                if c.status == 'running':
                    port_mappings = c.attrs['NetworkSettings']['Ports']
                    for container_port, host_bindings in port_mappings.items():
                        if host_bindings:
                            for binding in host_bindings:
                                if binding['HostPort'] == str(db_port):
                                    container = c
                                    break
                    if container:
                        break
            
            # If not found by port, try by container name patterns
            if not container:
                common_names = ['postgres', 'postgresql', 'db', 'database']
                for c in client.containers.list():
                    if c.status == 'running':
                        container_name = c.name.lower()
                        if any(name in container_name for name in common_names):
                            container = c
                            break
            
            if not container:
                raise Exception("Could not find PostgreSQL Docker container")
            
            # Execute the psutil script inside the container
            exec_result = container.exec_run(['python3', '-c', remote_script])
            
            if exec_result.exit_code != 0:
                raise Exception(f"Container exec failed with code {exec_result.exit_code}: {exec_result.output.decode()}")
            
            # Parse the JSON output
            output = exec_result.output.decode().strip()
            metrics = json.loads(output)
            
            # Remove error field if present and no actual error occurred
            if 'error' in metrics and exec_result.exit_code == 0:
                del metrics['error']
            
            return metrics
            
        except docker.errors.DockerException as docker_err:
            raise Exception(f"Docker API error: {docker_err}")
            
    except Exception as e:
        # Method 2: Fallback to docker exec command via subprocess
        try:
            import subprocess
            
            # Try to find container using docker ps
            ps_result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\t{{.Ports}}'], 
                                     capture_output=True, text=True, timeout=10)
            
            if ps_result.returncode != 0:
                raise Exception("Docker ps command failed")
            
            # Parse output to find PostgreSQL container
            container_name = None
            db_port = db_config.get('port', 5432)
            
            for line in ps_result.stdout.split('\n')[1:]:  # Skip header
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        name, ports = parts[0], parts[1]
                        if str(db_port) in ports or any(keyword in name.lower() for keyword in ['postgres', 'db']):
                            container_name = name
                            break
            
            if not container_name:
                raise Exception("Could not find PostgreSQL container via docker ps")
            
            # Execute the script in the container
            exec_result = subprocess.run(
                ['docker', 'exec', container_name, 'python3', '-c', remote_script],
                capture_output=True, text=True, timeout=30
            )
            
            if exec_result.returncode != 0:
                raise Exception(f"Docker exec failed: {exec_result.stderr}")
            
            # Parse the JSON output
            metrics = json.loads(exec_result.stdout.strip())
            
            return metrics
            
        except subprocess.TimeoutExpired:
            raise Exception("Docker exec command timed out")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Docker exec subprocess error: {e}")
        except Exception as subprocess_err:
            raise Exception(f"Subprocess fallback failed: {subprocess_err}")

def _execute_remote_psutil_ssh(host):
    """Fallback: Execute psutil commands on remote server via SSH"""
    
    # Remote Python script to get exact system metrics
    remote_script = '''
import psutil
import json

try:
    # Get CPU usage with 1 second interval for accuracy
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Get memory information
    memory = psutil.virtual_memory()
    
    metrics = {
        "system_cpu_percent": cpu_percent,
        "system_ram_percent": memory.percent,
        "system_ram_total_gb": round(memory.total / (1024**3), 2),
        "system_ram_used_gb": round(memory.used / (1024**3), 2),
        "system_ram_available_gb": round(memory.available / (1024**3), 2)
    }
    
    print(json.dumps(metrics))
    
except Exception as e:
    error_metrics = {
        "system_cpu_percent": 0.0,
        "system_ram_percent": 0.0,
        "system_ram_total_gb": 0.0,
        "system_ram_used_gb": 0.0,
        "system_ram_available_gb": 0.0,
        "error": str(e)
    }
    print(json.dumps(error_metrics))
'''

    try:
        # Try SSH connection with key-based authentication first
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Try different authentication methods
        connected = False
        
        # Method 1: Try with SSH key (most secure)
        try:
            ssh.connect(host, timeout=10)
            connected = True
        except:
            pass
            
        # Method 2: Try with SSH agent
        if not connected:
            try:
                ssh.connect(host, timeout=10, allow_agent=True)
                connected = True
            except:
                pass
                
        # Method 3: Try with username from environment or config
        # You may need to customize this based on your setup
        if not connected:
            import os
            username = os.getenv('REMOTE_SSH_USER', 'postgres')  # Default to postgres user
            try:
                ssh.connect(host, username=username, timeout=10)
                connected = True
            except:
                pass
        
        if not connected:
            raise Exception(f"Could not establish SSH connection to {host}")
            
        # Execute the remote script
        stdin, stdout, stderr = ssh.exec_command(f'python3 -c "{remote_script}"')
        
        # Get the output
        output = stdout.read().decode().strip()
        error_output = stderr.read().decode().strip()
        
        ssh.close()
        
        if error_output:
            print(f"Remote script stderr: {error_output}")
            
        # Parse JSON response
        metrics = json.loads(output)
        
        # Remove error field if present and no actual error occurred
        if 'error' in metrics and not error_output:
            del metrics['error']
            
        return metrics
        
    except Exception as e:
        print(f"SSH execution failed: {e}")
        
        # Fallback: Try using PostgreSQL's COPY FROM PROGRAM (if available and permitted)
        return _try_postgres_copy_method(remote_script)

def _try_postgres_copy_method(script):
    """Fallback method using PostgreSQL's COPY FROM PROGRAM"""
    try:
        with get_connection() as conn, conn.cursor() as cur:
            # Create temporary table for results
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_metrics (
                    metrics_json TEXT
                )
            """)
            
            # Try to execute system command via PostgreSQL (requires superuser privileges)
            bash_command = f"bash -c '{script}'"
            
            cur.execute(f"""
                COPY temp_metrics (metrics_json) 
                FROM PROGRAM '{bash_command}'
            """)
            
            # Retrieve the result
            cur.execute("SELECT metrics_json FROM temp_metrics LIMIT 1")
            result = cur.fetchone()
            
            if result:
                metrics = json.loads(result[0])
                return metrics
            else:
                raise Exception("No metrics returned from COPY FROM PROGRAM")
                
    except Exception as e:
        print(f"PostgreSQL COPY FROM PROGRAM method failed: {e}")
        # Return zero metrics as final fallback
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