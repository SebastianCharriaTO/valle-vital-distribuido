"""Utilidades comunes a todos los nodos: configuracion por entorno, logs con
X-Correlation-ID y tamano del pool de hilos de FastAPI/AnyIO.
"""
from __future__ import annotations

import logging
import os
import sys
import uuid
from typing import Optional

CABECERA_CID = "X-Correlation-ID"


def env_int(nombre: str, defecto: int) -> int:
    return int(os.getenv(nombre, str(defecto)))


def env_float(nombre: str, defecto: float) -> float:
    return float(os.getenv(nombre, str(defecto)))


def env_bool(nombre: str, defecto: bool) -> bool:
    return os.getenv(nombre, "1" if defecto else "0").strip().lower() in ("1", "true", "si", "yes")


def nombre_nodo(defecto: str) -> str:
    return os.getenv("NODO_NOMBRE", defecto)


def nuevo_cid(cid: Optional[str] = None) -> str:
    return cid or uuid.uuid4().hex[:10]


def logger(nodo: str) -> logging.Logger:
    log = logging.getLogger("vv." + nodo)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s nodo=" + nodo + " hilo=%(threadName)s %(levelname)s %(message)s"))
        log.addHandler(h)
        log.setLevel(os.getenv("LOG_NIVEL", "INFO"))
        log.propagate = False
    return log


def configurar_pool_hilos() -> int:
    """FastAPI ejecuta cada endpoint `def` en el pool de hilos de AnyIO (40 por defecto).
    HILOS_POR_NODO lo fija explicitamente: es la capacidad de cada nodo (ver E1)."""
    import anyio.to_thread

    n = env_int("HILOS_POR_NODO", 40)
    anyio.to_thread.current_default_thread_limiter().total_tokens = n
    return n
