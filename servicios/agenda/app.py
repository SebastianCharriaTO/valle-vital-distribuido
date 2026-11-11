"""Nodo AGENDA (FastAPI). Una sola replica: es el unico escritor de franjas y citas."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel

from common.broker import BrokerPort, crear_broker, nueva_tarea, nuevo_evento, PREFIJO_EVENTOS
from common.dominio import Cita, PeticionAsignacion, Profesional
from common.nodo import (CABECERA_CID, configurar_pool_hilos, env_bool, env_float, env_int,
                         logger, nombre_nodo)
from servicios.agenda.servicio import AgendaService, SinDisponibilidad

NODO = nombre_nodo("agenda")
log = logger(NODO)
SEED = Path(__file__).resolve().parents[2] / "data" / "seed.json"
_profs = [Profesional(**p) for p in json.loads(SEED.read_text(encoding="utf-8"))["profesionales"]]
servicio = AgendaService(_profs, dias=env_int("AGENDA_DIAS", 7), usar_lock=env_bool("AGENDA_LOCK", True),
                         latencia_escritura_ms=env_float("AGENDA_LATENCIA_ESCRITURA_MS", 5))
broker = None  # type: BrokerPort
fallos_publicacion = 0

app = FastAPI(title="Valle Vital - nodo agenda", version="1.0.0")


@app.on_event("startup")
def _arranque() -> None:
    global broker
    hilos = configurar_pool_hilos()
    broker = crear_broker()
    log.info("arranque hilos=%s lock=%s broker=%s", hilos, servicio.usar_lock, broker.tipo)


@app.post("/citas", response_model=Cita, status_code=201)
def asignar(p: PeticionAsignacion, response: Response, x_correlation_id: str = Header(default="-")) -> Cita:
    global fallos_publicacion
    try:
        cita = servicio.asignar(p.paciente_id, p.sede_id, p.especialidad, p.prioridad)
    except SinDisponibilidad as exc:
        raise HTTPException(409, str(exc))
    response.headers["X-Nodo"] = NODO
    response.headers[CABECERA_CID] = x_correlation_id
    log.info("cid=%s cita=%s franja=%s prioridad=%s", x_correlation_id, cita.id, cita.franja_id,
             cita.prioridad.value)
    # Lado asincrono: evento (Observer distribuido) + tarea (cola). El cliente no los espera.
    try:
        broker.publicar(PREFIJO_EVENTOS + "cita.asignada", nuevo_evento(
            "cita.asignada", NODO, x_correlation_id, cita=cita.id, paciente=cita.paciente_id,
            prioridad=cita.prioridad.value, inicio=cita.inicio.isoformat()))
        broker.encolar(nueva_tarea("recordatorio", {"cita": cita.id, "paciente": cita.paciente_id,
                                                    "inicio": cita.inicio.isoformat()}, x_correlation_id))
    except Exception as exc:  # el broker caido no impide asignar la cita
        fallos_publicacion += 1
        log.error("cid=%s no se pudo publicar en el broker: %s", x_correlation_id, exc)
    return cita


@app.get("/citas", response_model=List[Cita])
def listar() -> List[Cita]:
    return servicio.citas()


@app.get("/resumen")
def resumen() -> dict:
    d = servicio.resumen()
    d["fallos_publicacion"] = fallos_publicacion
    return d


class CambioLock(BaseModel):
    activo: bool


@app.post("/admin/lock")
def cambiar_lock(c: CambioLock) -> dict:
    servicio.usar_lock = c.activo
    return {"lock": servicio.usar_lock}


@app.post("/admin/reiniciar")
def reiniciar() -> dict:
    servicio.reiniciar()
    return servicio.resumen()


@app.get("/health")
def health() -> dict:
    return {"estado": "ok", "nodo": NODO}


@app.get("/health/ready")
def ready() -> dict:
    ok = broker is not None and broker.ping()
    if not ok:
        raise HTTPException(503, "broker no disponible")
    return {"estado": "listo", "nodo": NODO, "broker": broker.tipo}
