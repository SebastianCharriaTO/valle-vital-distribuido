"""Proxies remotos (GoF Proxy) hacia los nodos triaje y agenda.

TriajeProxy ofrece la misma operacion `evaluar(solicitud)` que el motor de reglas local
(Strategy de la Act. 2): quien lo usa no sabe si el triaje es local o remoto (transparencia
de distribucion). Por dentro hace: balanceo round-robin entre replicas, Circuit Breaker POR
REPLICA, Retry hacia la siguiente replica y Bulkhead por backend.
"""
from __future__ import annotations

import itertools
import threading
from typing import Callable, List, Optional, Tuple

import httpx

from common.circuit_breaker import CircuitBreaker, CircuitoAbierto, EstadoCircuito
from common.dominio import Cita, PeticionAsignacion, ResultadoTriage, SolicitudTriage
from common.nodo import CABECERA_CID
from servicios.gateway.resiliencia import Bulkhead, BulkheadLleno, reintentar


class ServicioNoDisponible(Exception):
    """Ninguna replica del backend respondio (o todas tienen el circuito abierto)."""


class SinCupo(Exception):
    """La agenda respondio 409: no hay franjas."""


class ErrorBackend(Exception):
    def __init__(self, status: int, detalle: str) -> None:
        super().__init__("HTTP {}: {}".format(status, detalle))
        self.status = status


ERRORES_TRANSITORIOS = (httpx.TransportError, CircuitoAbierto, BulkheadLleno)


def _transitorio(exc: BaseException) -> bool:
    return isinstance(exc, ERRORES_TRANSITORIOS) or (isinstance(exc, ErrorBackend) and exc.status >= 500)


class ClienteHTTP:
    """Envuelve httpx. compartido=True reutiliza un pool de conexiones (AJUSTE medido en E6);
    compartido=False crea un cliente (y una conexion TCP) por peticion, como en la version inicial."""

    def __init__(self, compartido: bool, timeout_s: float, max_conexiones: int) -> None:
        self.compartido = compartido
        self._timeout = httpx.Timeout(timeout_s, connect=min(1.0, timeout_s))
        self._limits = httpx.Limits(max_connections=max_conexiones, max_keepalive_connections=max_conexiones)
        self._c = httpx.Client(timeout=self._timeout, limits=self._limits) if compartido else None

    def post(self, url: str, json: dict, headers: dict) -> httpx.Response:
        if self._c is not None:
            return self._c.post(url, json=json, headers=headers)
        with httpx.Client(timeout=self._timeout) as c:
            return c.post(url, json=json, headers=headers)

    def get(self, url: str, timeout_s: float = 1.0) -> httpx.Response:
        if self._c is not None:
            return self._c.get(url, timeout=timeout_s)
        with httpx.Client(timeout=timeout_s) as c:
            return c.get(url)


class Replica:
    def __init__(self, nombre: str, url: str, breaker: CircuitBreaker) -> None:
        self.nombre, self.url, self.breaker = nombre, url.rstrip("/"), breaker
        self.ok = 0
        self.fallos = 0


class TriajeProxy:
    def __init__(self, urls: List[str], cliente: ClienteHTTP, resiliencia: bool, reintentos: int,
                 base_ms: float, max_ms: float, cb_umbral: int, cb_recuperacion_s: float,
                 bulkhead: Bulkhead,
                 al_cambiar_circuito: Optional[Callable[[str, EstadoCircuito, EstadoCircuito], None]] = None) -> None:
        def cb(nombre: str) -> CircuitBreaker:
            aviso = (lambda a, b: al_cambiar_circuito(nombre, a, b)) if al_cambiar_circuito else None
            return CircuitBreaker(cb_umbral, cb_recuperacion_s, excepciones=(Exception,), al_cambiar=aviso)

        self.replicas = [Replica("triaje-{}".format(i + 1), u, cb("triaje-{}".format(i + 1)))
                         for i, u in enumerate(urls)]
        self._cliente, self.resiliencia = cliente, resiliencia
        self._reintentos, self._base_ms, self._max_ms = reintentos, base_ms, max_ms
        self.bulkhead = bulkhead
        self._rr = itertools.count()
        self._lock = threading.Lock()

    def _siguiente(self) -> int:
        with self._lock:
            return next(self._rr)

    def _llamar(self, rep: Replica, solicitud: SolicitudTriage, cid: str) -> Tuple[ResultadoTriage, str]:
        r = self._cliente.post(rep.url + "/triaje", json=solicitud.model_dump(mode="json"),
                               headers={CABECERA_CID: cid})
        if r.status_code >= 400:
            raise ErrorBackend(r.status_code, r.text[:200])
        return ResultadoTriage(**r.json()), r.headers.get("X-Nodo", rep.nombre)

    def evaluar(self, solicitud: SolicitudTriage, cid: str = "-") -> Tuple[ResultadoTriage, str]:
        inicio = self._siguiente()
        n = len(self.replicas)
        if not self.resiliencia:  # version inicial: un intento, sin breaker ni reintentos
            rep = self.replicas[inicio % n]
            try:
                res = self._llamar(rep, solicitud, cid)
                rep.ok += 1
                return res
            except Exception:
                rep.fallos += 1
                raise

        def intento(i: int) -> Tuple[ResultadoTriage, str]:
            # cada reintento va a la SIGUIENTE replica cuyo circuito permita llamar
            for salto in range(n):
                rep = self.replicas[(inicio + i + salto) % n]
                if rep.breaker.permitir():
                    break
            else:
                raise CircuitoAbierto("todas las replicas de triaje tienen el circuito abierto")
            try:
                res = self.bulkhead.ejecutar(lambda: self._llamar(rep, solicitud, cid))
            except Exception as exc:
                if _transitorio(exc) and not isinstance(exc, BulkheadLleno):
                    rep.breaker.registrar_fallo()
                    rep.fallos += 1
                raise
            rep.breaker.registrar_exito()
            rep.ok += 1
            return res

        try:
            return reintentar(intento, self._reintentos, self._base_ms, self._max_ms, _transitorio)
        except Exception as exc:
            if _transitorio(exc):
                raise ServicioNoDisponible(str(exc))
            raise

    def estado(self) -> List[dict]:
        return [{"nodo": r.nombre, "url": r.url, "circuito": r.breaker.estado.value,
                 "ok": r.ok, "fallos": r.fallos} for r in self.replicas]


class AgendaProxy:
    """POST /citas NO es idempotente: solo se reintenta si la conexion no llego a abrirse
    (httpx.ConnectError), porque en ese caso la agenda nunca recibio la peticion."""

    def __init__(self, url: str, cliente: ClienteHTTP, resiliencia: bool, reintentos: int, base_ms: float,
                 max_ms: float, breaker: CircuitBreaker, bulkhead: Bulkhead) -> None:
        self.url, self._cliente, self.resiliencia = url.rstrip("/"), cliente, resiliencia
        self._reintentos, self._base_ms, self._max_ms = reintentos, base_ms, max_ms
        self.breaker, self.bulkhead = breaker, bulkhead

    def _llamar(self, p: PeticionAsignacion, cid: str) -> Cita:
        r = self._cliente.post(self.url + "/citas", json=p.model_dump(mode="json"), headers={CABECERA_CID: cid})
        if r.status_code == 409:
            raise SinCupo(r.json().get("detail", "sin cupo"))
        if r.status_code >= 400:
            raise ErrorBackend(r.status_code, r.text[:200])
        return Cita(**r.json())

    def asignar(self, p: PeticionAsignacion, cid: str = "-") -> Cita:
        if not self.resiliencia:
            return self._llamar(p, cid)

        def intento(_: int) -> Cita:
            return self.breaker.llamar(lambda: self.bulkhead.ejecutar(lambda: self._llamar(p, cid)))

        try:
            return reintentar(intento, self._reintentos, self._base_ms, self._max_ms,
                              lambda e: isinstance(e, httpx.ConnectError))
        except (httpx.TransportError, CircuitoAbierto, BulkheadLleno) as exc:
            raise ServicioNoDisponible(str(exc))
