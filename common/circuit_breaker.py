"""Circuit Breaker (Microsoft, 2025; Fowler, 2014): CERRADO -> ABIERTO -> SEMIABIERTO.

Heredado de la Act. 2 con un AJUSTE de la Act. 4: ahora es seguro entre hilos.
En la Act. 2 `self._fallos += 1` no era atomico; como FastAPI ejecuta los `def` en un
pool de hilos, dos hilos podian perder un incremento o abrir el circuito dos veces.
Todo cambio de estado se hace bajo un threading.Lock.
"""
from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable, Optional, Tuple, Type, TypeVar

R = TypeVar("R")


class EstadoCircuito(str, Enum):
    CERRADO = "cerrado"
    ABIERTO = "abierto"
    SEMIABIERTO = "semiabierto"


class CircuitoAbierto(Exception):
    """El circuito esta abierto: la llamada ni siquiera se intenta (falla rapida)."""


class CircuitBreaker:
    def __init__(self, umbral_fallos: int = 3, segundos_recuperacion: float = 30.0,
                 reloj: Callable[[], float] = time.monotonic,
                 excepciones: Tuple[Type[BaseException], ...] = (Exception,),
                 al_cambiar: Optional[Callable[[EstadoCircuito, EstadoCircuito], None]] = None) -> None:
        self.umbral = umbral_fallos
        self.recuperacion = segundos_recuperacion
        self._reloj = reloj
        self._excepciones = excepciones
        self._al_cambiar = al_cambiar
        self._lock = threading.Lock()
        self._fallos = 0
        self._abierto_desde = None  # type: Optional[float]
        self._estado = EstadoCircuito.CERRADO
        self._sonda_en_curso = False

    # -- lectura -------------------------------------------------------------
    @property
    def estado(self) -> EstadoCircuito:
        with self._lock:
            return self._estado_actual()

    @property
    def fallos(self) -> int:
        with self._lock:
            return self._fallos

    def _estado_actual(self) -> EstadoCircuito:
        if self._estado is EstadoCircuito.ABIERTO and self._abierto_desde is not None \
                and self._reloj() - self._abierto_desde >= self.recuperacion:
            self._cambiar(EstadoCircuito.SEMIABIERTO)
        return self._estado

    def _cambiar(self, nuevo: EstadoCircuito) -> None:
        viejo = self._estado
        self._estado = nuevo
        if viejo is not nuevo and self._al_cambiar is not None:
            try:
                self._al_cambiar(viejo, nuevo)
            except Exception:  # un observador no debe romper el breaker
                pass

    # -- uso -----------------------------------------------------------------
    def permitir(self) -> bool:
        """True si se puede intentar la llamada. En SEMIABIERTO deja pasar una sola sonda."""
        with self._lock:
            estado = self._estado_actual()
            if estado is EstadoCircuito.CERRADO:
                return True
            if estado is EstadoCircuito.SEMIABIERTO and not self._sonda_en_curso:
                self._sonda_en_curso = True
                return True
            return False

    def llamar(self, fn: Callable[[], R]) -> R:
        if not self.permitir():
            raise CircuitoAbierto("circuito abierto")
        try:
            resultado = fn()
        except self._excepciones:
            self.registrar_fallo()
            raise
        self.registrar_exito()
        return resultado

    def registrar_fallo(self) -> None:
        with self._lock:
            self._fallos += 1
            self._sonda_en_curso = False
            if self._estado is EstadoCircuito.SEMIABIERTO or self._fallos >= self.umbral:
                self._abierto_desde = self._reloj()
                self._cambiar(EstadoCircuito.ABIERTO)

    def registrar_exito(self) -> None:
        with self._lock:
            self._fallos = 0
            self._abierto_desde = None
            self._sonda_en_curso = False
            self._cambiar(EstadoCircuito.CERRADO)
