"""Broker local del Plan B: un proceso con colas y topicos en memoria expuesto por TCP con
multiprocessing.managers (solo biblioteca estandar, Python 3.8+).

Uso:  python -m broker_local.servidor --puerto 50000
Limites (declarados en el informe): todo vive en memoria del proceso; la cola de tareas es
at-most-once; no hay persistencia ni replicacion. Es un sustituto didactico de Redis.
"""
from __future__ import annotations

import argparse
import fnmatch
import itertools
import os
import queue
import threading
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Optional, Set


class NucleoBroker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._colas = {}  # type: Dict[str, queue.Queue]
        self._hechas = {}  # type: Dict[str, Set[str]]
        self._subs = {}  # type: Dict[str, Any]
        self._seq = itertools.count(1)

    def _cola(self, nombre: str) -> "queue.Queue":
        with self._lock:
            if nombre not in self._colas:
                self._colas[nombre] = queue.Queue()
            return self._colas[nombre]

    def ping(self) -> str:
        return "pong"

    def encolar(self, cola: str, tarea: Dict[str, Any]) -> None:
        self._cola(cola).put(tarea)

    def tomar(self, cola: str, timeout_s: float = 1.0) -> Optional[Dict[str, Any]]:
        try:
            return self._cola(cola).get(timeout=timeout_s)
        except queue.Empty:
            return None

    def longitud(self, cola: str) -> int:
        return self._cola(cola).qsize()

    def marcar_hecha(self, cola: str, tarea_id: str) -> bool:
        with self._lock:
            s = self._hechas.setdefault(cola, set())
            if tarea_id in s:
                return False
            s.add(tarea_id)
            return True

    def hechas(self, cola: str) -> int:
        with self._lock:
            return len(self._hechas.get(cola, ()))

    def crear_suscripcion(self, patron: str) -> str:
        sid = "sub-{}".format(next(self._seq))
        with self._lock:
            self._subs[sid] = (patron, queue.Queue(maxsize=10000))
        return sid

    def cancelar(self, sid: str) -> None:
        with self._lock:
            self._subs.pop(sid, None)

    def publicar(self, topico: str, evento: Dict[str, Any]) -> None:
        with self._lock:
            destinos = [q for (p, q) in self._subs.values() if fnmatch.fnmatch(topico, p)]
        for q in destinos:
            try:
                q.put_nowait(evento)
            except queue.Full:  # suscriptor lento: se descarta (at-most-once)
                pass

    def recibir(self, sid: str, timeout_s: float = 1.0) -> Optional[Dict[str, Any]]:
        with self._lock:
            par = self._subs.get(sid)
        if par is None:
            return None
        try:
            return par[1].get(timeout=timeout_s)
        except queue.Empty:
            return None

    def reiniciar(self) -> None:
        with self._lock:
            self._colas.clear()
            self._hechas.clear()


_NUCLEO = NucleoBroker()


class _Gestor(BaseManager):
    pass


def _obtener() -> NucleoBroker:
    return _NUCLEO


_Gestor.register("broker", callable=_obtener)


def conectar(host: str, puerto: int, clave: bytes) -> Any:
    # multiprocessing guarda por direccion la conexion de cada hilo; si el broker se reinicio,
    # esa conexion vieja quedaria muerta para siempre. Se descarta antes de reconectar.
    from multiprocessing.managers import BaseProxy
    getattr(BaseProxy, "_address_to_local", {}).pop((host, puerto), None)

    class _Cliente(BaseManager):
        pass

    _Cliente.register("broker")
    m = _Cliente(address=(host, puerto), authkey=clave)
    m.connect()
    return m.broker()  # type: ignore[attr-defined]


def main() -> None:
    ap = argparse.ArgumentParser(description="Broker local (Plan B) de Valle Vital")
    ap.add_argument("--host", default=os.getenv("BROKER_HOST", "127.0.0.1"))
    ap.add_argument("--puerto", type=int, default=int(os.getenv("BROKER_PUERTO", "50000")))
    a = ap.parse_args()
    clave = os.getenv("BROKER_CLAVE", "valle-vital").encode()
    gestor = _Gestor(address=(a.host, a.puerto), authkey=clave)
    print("broker_local escuchando en {}:{}".format(a.host, a.puerto), flush=True)
    gestor.get_server().serve_forever()


if __name__ == "__main__":
    main()
