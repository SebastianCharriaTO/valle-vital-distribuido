"""Proveedor de IA SIMULADO (Strategy 2, heredado de la Act. 2) con latencia y fallas
configurables, para que la carga del nodo triaje sea de E/S (como un LLM remoto real).

No llama a ningun servicio externo ni usa claves.
"""
from __future__ import annotations

import random
import threading
import time

from common.dominio import ResultadoTriage, SolicitudTriage
from common.reglas import MotorReglas


class ProveedorIAError(Exception):
    """Falla del proveedor externo (red, cuota, tiempo de espera...)."""


class MotorIASimulado:
    nombre = "llm_simulado"

    def __init__(self, latencia_ms: float = 100.0, tasa_fallo: float = 0.0,
                 timeout_ms: float = 1000.0, semilla: int = 0) -> None:
        self.latencia_ms = latencia_ms
        self.tasa_fallo = tasa_fallo
        self.timeout_ms = timeout_ms
        self._base = MotorReglas()
        self._rnd = random.Random(semilla or None)
        self._lock = threading.Lock()

    def _azar(self) -> float:
        with self._lock:
            return self._rnd.random()

    def evaluar(self, solicitud: SolicitudTriage) -> ResultadoTriage:
        if self.tasa_fallo > 0 and self._azar() < self.tasa_fallo:
            time.sleep(self.timeout_ms / 1000.0)  # el cliente espera hasta agotar el timeout
            raise ProveedorIAError("Proveedor de IA no disponible (simulado): tiempo de espera agotado")
        if self.latencia_ms > 0:
            time.sleep(self.latencia_ms * (0.8 + 0.4 * self._azar()) / 1000.0)
        r = self._base.evaluar(solicitud)
        return r.model_copy(update={
            "motor": self.nombre,
            "explicacion": "Analisis del asistente: " + r.explicacion
                           + " Se recomienda valoracion presencial segun la prioridad sugerida.",
        })
