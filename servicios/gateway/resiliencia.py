"""Tacticas de resiliencia del gateway: Retry con backoff exponencial y jitter (Microsoft,
2025) y Bulkhead por backend (Microsoft, 2026). El Circuit Breaker vive en common/."""
from __future__ import annotations

import random
import threading
import time
from typing import Callable, TypeVar

R = TypeVar("R")


class BulkheadLleno(Exception):
    """El compartimento del backend esta lleno: se rechaza en vez de agotar hilos."""


class Bulkhead:
    """Limita las llamadas simultaneas a UN backend. Si triaje se pone lento, solo se ocupan
    sus `capacidad` hilos y el resto del pool del gateway sigue libre para agenda."""

    def __init__(self, nombre: str, capacidad: int, espera_s: float = 0.5) -> None:
        self.nombre, self.capacidad, self.espera_s = nombre, capacidad, espera_s
        self._sem = threading.BoundedSemaphore(capacidad)
        self._lock = threading.Lock()
        self.en_uso = 0
        self.rechazos = 0

    def ejecutar(self, fn: Callable[[], R]) -> R:
        if not self._sem.acquire(timeout=self.espera_s):
            with self._lock:
                self.rechazos += 1
            raise BulkheadLleno("bulkhead {} lleno ({} llamadas)".format(self.nombre, self.capacidad))
        with self._lock:
            self.en_uso += 1
        try:
            return fn()
        finally:
            with self._lock:
                self.en_uso -= 1
            self._sem.release()


def espera_backoff(intento: int, base_ms: float, max_ms: float, rnd: random.Random = random) -> float:
    """Backoff exponencial con 'full jitter': uniforme entre 0 y min(max, base*2^intento)."""
    techo = min(max_ms, base_ms * (2 ** intento))
    return rnd.uniform(0, techo) / 1000.0


def reintentar(fn: Callable[[int], R], intentos: int, base_ms: float, max_ms: float,
               es_reintentable: Callable[[BaseException], bool],
               dormir: Callable[[float], None] = time.sleep) -> R:
    """Ejecuta fn(intento). Reintenta solo errores transitorios (es_reintentable)."""
    ultimo = None  # type: BaseException
    for i in range(intentos + 1):
        try:
            return fn(i)
        except Exception as exc:  # noqa: BLE001
            ultimo = exc
            if i >= intentos or not es_reintentable(exc):
                raise
            dormir(espera_backoff(i, base_ms, max_ms))
    raise ultimo  # pragma: no cover
