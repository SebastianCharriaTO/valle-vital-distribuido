"""Nodo WORKER: pool de W hilos (ThreadPoolExecutor) que compiten por la cola de tareas
(Competing Consumers). Varios procesos worker pueden correr a la vez.

Uso: python -m servicios.worker.worker            (W = WORKER_HILOS, por defecto 8)
"""
from __future__ import annotations

import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.broker import BrokerPort, crear_broker
from common.nodo import env_float, env_int, logger, nombre_nodo
from servicios.worker.tareas import MANEJADORES

NODO = nombre_nodo("worker-" + socket.gethostname()[:12])
log = logger(NODO)


class Worker:
    def __init__(self, broker: BrokerPort, hilos: int, latencia_ms: float, nodo: str = NODO) -> None:
        self.broker, self.hilos, self.latencia_ms, self.nodo = broker, hilos, latencia_ms, nodo
        self.parar = threading.Event()
        self._lock = threading.Lock()
        self.ocupados = 0
        self.procesadas = 0
        self.duplicadas = 0

    def _procesar(self, msg_id: str, tarea: dict) -> None:
        with self._lock:
            self.ocupados += 1
        try:
            MANEJADORES.get(tarea.get("tipo"), MANEJADORES["simulada"])(tarea, self.broker, self.nodo, self.latencia_ms)
            nueva = self.broker.marcar_hecha(tarea["id"])  # consumidor idempotente (S08)
            with self._lock:
                if nueva:
                    self.procesadas += 1
                else:
                    self.duplicadas += 1
            self.broker.confirmar(msg_id)
            if tarea.get("tipo") in ("recordatorio", "farmacia"):
                log.info("cid=%s tarea=%s tipo=%s hecha", tarea.get("cid"), tarea["id"], tarea.get("tipo"))
        except Exception as exc:  # la tarea queda sin ACK: Redis la reentregara
            log.error("tarea=%s fallo: %s", tarea.get("id"), exc)
        finally:
            with self._lock:
                self.ocupados -= 1

    def _bucle(self, i: int) -> None:
        consumidor = "{}-h{}".format(self.nodo, i)
        while not self.parar.is_set():
            try:
                m = self.broker.tomar(consumidor, timeout_s=1.0)
            except Exception as exc:
                log.error("broker no disponible: %s", exc)
                time.sleep(1.0)
                continue
            if m is not None:
                self._procesar(*m)

    def _reclamador(self) -> None:
        """Recupera tareas de workers muertos (solo Redis Streams; at-least-once)."""
        while not self.parar.wait(2.0):
            try:
                for msg_id, tarea in self.broker.reclamar_pendientes(self.nodo + "-reclamo", inactivo_s=5.0):
                    log.warning("reclamada tarea=%s de un consumidor caido", tarea.get("id"))
                    self._procesar(msg_id, tarea)
            except Exception:
                pass

    def _reporte(self) -> None:
        while not self.parar.wait(5.0):
            try:
                cola = self.broker.longitud()
            except Exception:
                cola = -1
            log.info("saturacion ocupados=%s/%s cola=%s procesadas=%s", self.ocupados, self.hilos, cola,
                     self.procesadas)

    def ejecutar(self) -> None:
        log.info("arranque hilos=%s broker=%s entrega=%s", self.hilos, self.broker.tipo, self.broker.entrega_tareas)
        threading.Thread(target=self._reclamador, name="reclamador", daemon=True).start()
        threading.Thread(target=self._reporte, name="reporte", daemon=True).start()
        with ThreadPoolExecutor(max_workers=self.hilos, thread_name_prefix=self.nodo) as pool:
            for i in range(self.hilos):
                pool.submit(self._bucle, i)
            while not self.parar.wait(0.5):
                pass
        log.info("detenido procesadas=%s", self.procesadas)


def main() -> None:
    w = Worker(crear_broker(), env_int("WORKER_HILOS", 8), env_float("TAREA_LATENCIA_MS", 80))
    signal.signal(signal.SIGTERM, lambda *_: w.parar.set())
    signal.signal(signal.SIGINT, lambda *_: w.parar.set())
    w.ejecutar()


if __name__ == "__main__":
    main()
