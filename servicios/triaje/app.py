"""Nodo TRIAJE (FastAPI). Replicable: triaje-1, triaje-2, ... (sin estado)."""
from __future__ import annotations

import time

from fastapi import FastAPI, Header, Response
from pydantic import BaseModel

from common.circuit_breaker import CircuitBreaker
from common.dominio import ResultadoTriage, SolicitudTriage
from common.nodo import (CABECERA_CID, configurar_pool_hilos, env_bool, env_float, env_int,
                         logger, nombre_nodo)
from common.reglas import MotorReglas
from servicios.triaje.motor_ia import MotorIASimulado, ProveedorIAError
from servicios.triaje.servicio import TriajeService

NODO = nombre_nodo("triaje")
log = logger(NODO)

motor = MotorIASimulado(latencia_ms=env_float("LLM_LATENCIA_MS", 100),
                        tasa_fallo=env_float("LLM_TASA_FALLO", 0.0),
                        timeout_ms=env_float("LLM_TIMEOUT_MS", 1000))
breaker = CircuitBreaker(env_int("CB_UMBRAL_FALLOS", 3), env_float("CB_SEGUNDOS_RECUPERACION", 30),
                         excepciones=(ProveedorIAError,)) if env_bool("CB_HABILITADO", True) else None
servicio = TriajeService(motor, MotorReglas(), breaker)
INICIO = time.time()

app = FastAPI(title="Valle Vital - nodo triaje", version="1.0.0")


@app.on_event("startup")
def _arranque() -> None:
    hilos = configurar_pool_hilos()
    log.info("arranque hilos=%s llm_latencia_ms=%s tasa_fallo=%s cb=%s", hilos, motor.latencia_ms,
             motor.tasa_fallo, breaker is not None)


@app.post("/triaje", response_model=ResultadoTriage)
def triaje(solicitud: SolicitudTriage, response: Response,
           x_correlation_id: str = Header(default="-")) -> ResultadoTriage:
    r = servicio.evaluar(solicitud)
    response.headers["X-Nodo"] = NODO
    response.headers[CABECERA_CID] = x_correlation_id
    log.info("cid=%s triaje prioridad=%s motor=%s", x_correlation_id, r.prioridad.value, r.motor)
    return r


class CambioProveedor(BaseModel):
    tasa_fallo: float


@app.post("/admin/proveedor-ia")
def cambiar_proveedor(c: CambioProveedor) -> dict:
    """Interruptor de demostracion: 1.0 = proveedor de IA caido; 0.0 = sano."""
    motor.tasa_fallo = max(0.0, min(1.0, c.tasa_fallo))
    log.warning("proveedor IA tasa_fallo=%s", motor.tasa_fallo)
    return {"nodo": NODO, "tasa_fallo": motor.tasa_fallo}


@app.get("/health")
def health() -> dict:
    return {"estado": "ok", "nodo": NODO}


@app.get("/estado")
def estado() -> dict:
    return {"nodo": NODO, "uptime_s": round(time.time() - INICIO, 1),
            "circuito_ia": breaker.estado.value if breaker else "deshabilitado",
            "llm_latencia_ms": motor.latencia_ms, "llm_tasa_fallo": motor.tasa_fallo}
