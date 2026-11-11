"""Manejadores de tareas del worker. Todas son de E/S simulada salvo 'cpu' (experimento GIL)."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Dict

from common.broker import BrokerPort, nuevo_evento, PREFIJO_EVENTOS, Tarea
from common.dominio import InventarioSede
from servicios.worker.farmacia import sugerencias

SEED = Path(__file__).resolve().parents[2] / "data" / "seed.json"
_INVENTARIO = [InventarioSede(id="{}:{}".format(f["sede_id"], f["medicamento_id"]), **f)
               for f in json.loads(SEED.read_text(encoding="utf-8"))["inventario"]]


def _recordatorio(t: Tarea, broker: BrokerPort, nodo: str, latencia_ms: float) -> None:
    time.sleep(latencia_ms / 1000.0)  # envio simulado de SMS/correo (pasarela externa)
    broker.publicar(PREFIJO_EVENTOS + "recordatorio.enviado",
                    nuevo_evento("recordatorio.enviado", nodo, t.get("cid", "-"), **t.get("datos", {})))


def _farmacia(t: Tarea, broker: BrokerPort, nodo: str, latencia_ms: float) -> None:
    time.sleep(latencia_ms / 1000.0)  # lectura simulada del inventario de las sedes
    for s in sugerencias(_INVENTARIO):
        broker.publicar(PREFIJO_EVENTOS + "stock.bajo_minimo", nuevo_evento(
            "stock.bajo_minimo", nodo, t.get("cid", "-"), medicamento=s.medicamento_id,
            sede=s.sede_destino_id, sede_origen=s.sede_origen_id or "central", cantidad=s.cantidad))


def _simulada(t: Tarea, broker: BrokerPort, nodo: str, latencia_ms: float) -> None:
    time.sleep(float(t.get("datos", {}).get("ms", 100)) / 1000.0)


def _cpu(t: Tarea, broker: BrokerPort, nodo: str, latencia_ms: float) -> None:
    n = int(t.get("datos", {}).get("n", 200000))
    acc = 0
    for i in range(n):  # trabajo de CPU puro en Python: el GIL impide paralelizarlo con hilos
        acc = (acc + i * i) % 1000003


MANEJADORES = {
    "recordatorio": _recordatorio,
    "farmacia": _farmacia,
    "simulada": _simulada,
    "cpu": _cpu,
}  # type: Dict[str, Callable[[Tarea, BrokerPort, str, float], Any]]
