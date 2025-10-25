
from __future__ import annotations
import os, yaml, datetime as dt

def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def today_str() -> str:
    return dt.date.today().isoformat()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
