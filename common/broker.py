"""Puerto del broker de mensajes y sus dos adaptadores (puertos y adaptadores, Cockburn).

- Cola de TAREAS (Competing Consumers / Queue-Based Load Leveling): cada tarea la procesa
  UN solo consumidor. Con Redis se usan Streams + grupo de consumidores + XACK, que dan
  entrega *at-least-once* (Redis, s. f.). Con el broker local (Plan B, solo biblioteca
  estandar) la entrega es *at-most-once*: si un worker muere con una tarea en mano, se pierde.
- TOPICOS de eventos (Observer distribuido / Publish-Subscribe): cada evento llega a TODOS
  los suscriptores vivos. Con Redis Pub/Sub la entrega es *at-most-once*.

Los servicios solo conocen BrokerPort; `crear_broker(url)` (raiz de composicion) elige el
adaptador por la variable BROKER_URL: redis://host:6379/0  o  local://host:50000
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

Tarea = Dict[str, Any]
Evento = Dict[str, Any]

COLA_TAREAS = "vv:tareas"
GRUPO_WORKERS = "workers"
PREFIJO_EVENTOS = "vv.eventos."


class Suscripcion(ABC):
    @abstractmethod
    def recibir(self, timeout_s: float = 1.0) -> Optional[Evento]: ...

    def cerrar(self) -> None:  # pragma: no cover - opcional
        pass


class BrokerPort(ABC):
    tipo = "abstracto"
    entrega_tareas = "?"

    # --- cola de tareas -----------------------------------------------------
    @abstractmethod
    def encolar(self, tarea: Tarea, cola: str = COLA_TAREAS) -> str: ...

    @abstractmethod
    def tomar(self, consumidor: str, timeout_s: float = 1.0,
              cola: str = COLA_TAREAS) -> Optional[Tuple[str, Tarea]]: ...

    @abstractmethod
    def confirmar(self, msg_id: str, cola: str = COLA_TAREAS) -> None: ...

    def reclamar_pendientes(self, consumidor: str, inactivo_s: float = 5.0,
                            cola: str = COLA_TAREAS) -> List[Tuple[str, Tarea]]:
        """Tareas entregadas a un consumidor que murio sin confirmarlas (solo at-least-once)."""
        return []

    @abstractmethod
    def longitud(self, cola: str = COLA_TAREAS) -> int:
        """Tareas en espera (aun no entregadas)."""

    @abstractmethod
    def marcar_hecha(self, tarea_id: str, cola: str = COLA_TAREAS) -> bool:
        """Registra la tarea como procesada. False si ya lo estaba (consumidor idempotente)."""

    @abstractmethod
    def hechas(self, cola: str = COLA_TAREAS) -> int: ...

    # --- eventos ------------------------------------------------------------
    @abstractmethod
    def publicar(self, topico: str, evento: Evento) -> None: ...

    @abstractmethod
    def suscribir(self, patron: str = PREFIJO_EVENTOS + "*") -> Suscripcion: ...

    # --- operacion ----------------------------------------------------------
    @abstractmethod
    def ping(self) -> bool: ...

    @abstractmethod
    def reiniciar(self) -> None:
        """Vacia colas y contadores (solo para pruebas y mediciones)."""


def nueva_tarea(tipo: str, datos: Optional[Dict[str, Any]] = None, cid: str = "-") -> Tarea:
    return {"id": uuid.uuid4().hex[:12], "tipo": tipo, "datos": datos or {}, "cid": cid,
            "creada": time.time()}


def nuevo_evento(tipo: str, origen: str, cid: str = "-", **datos: Any) -> Evento:
    return {"tipo": tipo, "origen": origen, "cid": cid, "momento": time.time(), "datos": datos}


# =============================================================================
# Adaptador 1: Redis (Plan A, Docker Compose; tambien sirve con procesos locales)
# =============================================================================
class _SuscripcionRedis(Suscripcion):
    def __init__(self, cliente: Any, patron: str) -> None:
        self._ps = cliente.pubsub(ignore_subscribe_messages=True)
        self._ps.psubscribe(patron)

    def recibir(self, timeout_s: float = 1.0) -> Optional[Evento]:
        msg = self._ps.get_message(timeout=timeout_s)
        if msg is None or msg.get("type") != "pmessage":
            return None
        return json.loads(msg["data"])

    def cerrar(self) -> None:
        self._ps.close()


class BrokerRedis(BrokerPort):
    tipo = "redis"
    entrega_tareas = "at-least-once (Streams + XACK)"

    def __init__(self, url: str) -> None:
        import redis  # import diferido: el Plan B no necesita Redis

        self._r = redis.Redis.from_url(url, decode_responses=True, socket_timeout=10,
                                       health_check_interval=30)
        self._grupos_listos = set()  # type: set
        self._lock = threading.Lock()

    def _grupo(self, cola: str) -> None:
        if cola in self._grupos_listos:
            return
        with self._lock:
            try:
                self._r.xgroup_create(cola, GRUPO_WORKERS, id="0", mkstream=True)
            except Exception as exc:  # BUSYGROUP: ya existe
                if "BUSYGROUP" not in str(exc):
                    raise
            self._grupos_listos.add(cola)

    def encolar(self, tarea: Tarea, cola: str = COLA_TAREAS) -> str:
        self._grupo(cola)
        self._r.xadd(cola, {"t": json.dumps(tarea)})
        return tarea["id"]

    def tomar(self, consumidor: str, timeout_s: float = 1.0,
              cola: str = COLA_TAREAS) -> Optional[Tuple[str, Tarea]]:
        self._grupo(cola)
        res = self._r.xreadgroup(GRUPO_WORKERS, consumidor, {cola: ">"}, count=1,
                                 block=max(1, int(timeout_s * 1000)))
        if not res:
            return None
        _, mensajes = res[0]
        msg_id, campos = mensajes[0]
        return msg_id, json.loads(campos["t"])

    def confirmar(self, msg_id: str, cola: str = COLA_TAREAS) -> None:
        self._r.xack(cola, GRUPO_WORKERS, msg_id)

    def reclamar_pendientes(self, consumidor: str, inactivo_s: float = 5.0,
                            cola: str = COLA_TAREAS) -> List[Tuple[str, Tarea]]:
        self._grupo(cola)
        res = self._r.xautoclaim(cola, GRUPO_WORKERS, consumidor, int(inactivo_s * 1000),
                                 start_id="0-0", count=50)
        mensajes = res[1] if res else []
        return [(mid, json.loads(c["t"])) for mid, c in mensajes if c]

    def longitud(self, cola: str = COLA_TAREAS) -> int:
        self._grupo(cola)
        for g in self._r.xinfo_groups(cola):
            if g.get("name") == GRUPO_WORKERS:
                lag = g.get("lag")
                if lag is not None:
                    return int(lag)
        return int(self._r.xlen(cola))

    def marcar_hecha(self, tarea_id: str, cola: str = COLA_TAREAS) -> bool:
        return bool(self._r.sadd(cola + ":hechas", tarea_id))

    def hechas(self, cola: str = COLA_TAREAS) -> int:
        return int(self._r.scard(cola + ":hechas"))

    def publicar(self, topico: str, evento: Evento) -> None:
        self._r.publish(topico, json.dumps(evento))

    def suscribir(self, patron: str = PREFIJO_EVENTOS + "*") -> Suscripcion:
        return _SuscripcionRedis(self._r, patron)

    def ping(self) -> bool:
        try:
            return bool(self._r.ping())
        except Exception:
            return False

    def reiniciar(self) -> None:
        for k in self._r.scan_iter("vv:*"):
            self._r.delete(k)
        self._grupos_listos.clear()


# =============================================================================
# Adaptador 2: broker local por TCP (Plan B, solo biblioteca estandar)
# =============================================================================
class _SuscripcionLocal(Suscripcion):
    def __init__(self, proxy: Any, sid: str) -> None:
        self._p, self._sid = proxy, sid

    def recibir(self, timeout_s: float = 1.0) -> Optional[Evento]:
        return self._p.recibir(self._sid, timeout_s)

    def cerrar(self) -> None:
        try:
            self._p.cancelar(self._sid)
        except Exception:
            pass


class BrokerLocal(BrokerPort):
    """Cliente del servidor broker_local/servidor.py (multiprocessing.managers por TCP).
    Los proxies de multiprocessing usan una conexion por hilo, asi que el cliente es seguro
    entre hilos."""
    tipo = "local"
    entrega_tareas = "at-most-once (queue.Queue en memoria)"

    def __init__(self, host: str, puerto: int, clave: bytes) -> None:
        from broker_local.servidor import conectar

        self._p = conectar(host, puerto, clave)

    def encolar(self, tarea: Tarea, cola: str = COLA_TAREAS) -> str:
        self._p.encolar(cola, tarea)
        return tarea["id"]

    def tomar(self, consumidor: str, timeout_s: float = 1.0,
              cola: str = COLA_TAREAS) -> Optional[Tuple[str, Tarea]]:
        t = self._p.tomar(cola, timeout_s)
        return None if t is None else (t["id"], t)

    def confirmar(self, msg_id: str, cola: str = COLA_TAREAS) -> None:
        return None  # at-most-once: la tarea salio de la cola al tomarla

    def longitud(self, cola: str = COLA_TAREAS) -> int:
        return self._p.longitud(cola)

    def marcar_hecha(self, tarea_id: str, cola: str = COLA_TAREAS) -> bool:
        return self._p.marcar_hecha(cola, tarea_id)

    def hechas(self, cola: str = COLA_TAREAS) -> int:
        return self._p.hechas(cola)

    def publicar(self, topico: str, evento: Evento) -> None:
        self._p.publicar(topico, evento)

    def suscribir(self, patron: str = PREFIJO_EVENTOS + "*") -> Suscripcion:
        return _SuscripcionLocal(self._p, self._p.crear_suscripcion(patron))

    def ping(self) -> bool:
        try:
            return self._p.ping() == "pong"
        except Exception:
            return False

    def reiniciar(self) -> None:
        self._p.reiniciar()


# =============================================================================
# Adaptador 3: en memoria (solo pruebas unitarias)
# =============================================================================
class BrokerMemoria(BrokerPort):
    tipo = "memoria"
    entrega_tareas = "at-most-once (pruebas)"

    def __init__(self) -> None:
        from broker_local.servidor import NucleoBroker

        self._n = NucleoBroker()

    def encolar(self, tarea: Tarea, cola: str = COLA_TAREAS) -> str:
        self._n.encolar(cola, tarea)
        return tarea["id"]

    def tomar(self, consumidor: str, timeout_s: float = 1.0,
              cola: str = COLA_TAREAS) -> Optional[Tuple[str, Tarea]]:
        t = self._n.tomar(cola, timeout_s)
        return None if t is None else (t["id"], t)

    def confirmar(self, msg_id: str, cola: str = COLA_TAREAS) -> None:
        return None

    def longitud(self, cola: str = COLA_TAREAS) -> int:
        return self._n.longitud(cola)

    def marcar_hecha(self, tarea_id: str, cola: str = COLA_TAREAS) -> bool:
        return self._n.marcar_hecha(cola, tarea_id)

    def hechas(self, cola: str = COLA_TAREAS) -> int:
        return self._n.hechas(cola)

    def publicar(self, topico: str, evento: Evento) -> None:
        self._n.publicar(topico, evento)

    def suscribir(self, patron: str = PREFIJO_EVENTOS + "*") -> Suscripcion:
        n = self._n
        sid = n.crear_suscripcion(patron)

        class _S(Suscripcion):
            def recibir(self, timeout_s: float = 1.0) -> Optional[Evento]:
                return n.recibir(sid, timeout_s)

        return _S()

    def ping(self) -> bool:
        return True

    def reiniciar(self) -> None:
        self._n.reiniciar()


def crear_broker(url: Optional[str] = None, intentos: int = 30) -> BrokerPort:
    """Raiz de composicion del broker. Reintenta mientras el broker arranca."""
    url = url or os.getenv("BROKER_URL", "local://127.0.0.1:50000")
    u = urlparse(url)
    ultimo = None  # type: Optional[BaseException]
    for _ in range(intentos):
        try:
            if u.scheme == "redis":
                b = BrokerRedis(url)  # type: BrokerPort
            elif u.scheme == "local":
                clave = os.getenv("BROKER_CLAVE", "valle-vital").encode()
                b = BrokerLocal(u.hostname or "127.0.0.1", u.port or 50000, clave)
            elif u.scheme == "memoria":
                return BrokerMemoria()
            else:
                raise ValueError("BROKER_URL no soportada: " + url)
            if b.ping():
                return b
        except (ConnectionError, OSError, socket.error, EOFError) as exc:
            ultimo = exc
        time.sleep(0.5)
    raise ConnectionError("No se pudo conectar al broker {}: {}".format(url, ultimo))
