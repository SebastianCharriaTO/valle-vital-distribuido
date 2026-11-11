"""Servicio de triaje: estrategia primaria (IA simulada) + Circuit Breaker + fallback a reglas.
Sin estado de negocio: por eso puede tener N replicas (escalado horizontal, E1)."""
from __future__ import annotations

from typing import Optional

from common.circuit_breaker import CircuitBreaker, CircuitoAbierto
from common.dominio import ResultadoTriage, SolicitudTriage
from common.reglas import MotorReglas
from servicios.triaje.motor_ia import MotorIASimulado, ProveedorIAError


class TriajeService:
    def __init__(self, primario: MotorIASimulado, fallback: MotorReglas,
                 breaker: Optional[CircuitBreaker]) -> None:
        self.primario, self.fallback, self.breaker = primario, fallback, breaker

    def evaluar(self, solicitud: SolicitudTriage) -> ResultadoTriage:
        """Nunca propaga la falla del proveedor: degrada a reglas locales."""
        try:
            if self.breaker is None:  # configuracion SIN Circuit Breaker (experimento E3b)
                return self.primario.evaluar(solicitud)
            return self.breaker.llamar(lambda: self.primario.evaluar(solicitud))
        except (ProveedorIAError, CircuitoAbierto) as exc:
            r = self.fallback.evaluar(solicitud)
            return r.model_copy(update={
                "explicacion": "[Modo degradado: reglas locales; {}] ".format(type(exc).__name__)
                               + r.explicacion})
