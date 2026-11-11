import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))
# Las apps FastAPI se importan con un broker en memoria (sin red) en las pruebas.
os.environ.setdefault("BROKER_URL", "memoria://")
os.environ.setdefault("LLM_LATENCIA_MS", "0")
os.environ.setdefault("AGENDA_LATENCIA_ESCRITURA_MS", "0")
# Backends inexistentes: el gateway debe degradar sin romperse.
os.environ.setdefault("TRIAJE_URLS", "http://127.0.0.1:9,http://127.0.0.1:10")
os.environ.setdefault("AGENDA_URL", "http://127.0.0.1:11")
os.environ.setdefault("RETRY_BASE_MS", "1")
