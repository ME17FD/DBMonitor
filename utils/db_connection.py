import psycopg2
import yaml
from pathlib import Path

def load_config():
    config_file = Path(__file__).parent.parent / "config" / "db_config.yaml"
    with open(config_file, "r") as f:
        return yaml.safe_load(f)["database"]

def get_connection():
    cfg = load_config()
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["dbname"]
    )
