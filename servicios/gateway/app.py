"""Nodo GATEWAY (API Gateway / Gateway Routing): punto unico de entrada de Valle Vital.

Raiz de composicion del gateway: aqui se arman los proxies, el mediador, los breakers,
los bulkheads y el broker segun variables de entorno.
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common.broker import BrokerPort, crear_broker, nueva_tarea, nuevo_evento, PREFIJO_EVENTOS
from common.circuit_breaker import CircuitBreaker, EstadoCircuito
from common.dominio import SolicitudCita, SolicitudTriage
from common.nodo import (CABECERA_CID, configurar_pool_hilos, env_bool, env_float, env_int,
                         logger, nombre_nodo, nuevo_cid)
from common.reglas import MotorReglas
from servicios.gateway.clientes import (AgendaProxy, ClienteHTTP, ErrorBackend, ServicioNoDisponible,
                                        SinCupo, TriajeProxy)
from servicios.gateway.mediador import SolicitudCitaMediator
from servicios.gateway.resiliencia import Bulkhead

NODO = nombre_nodo("gateway")
log = logger(NODO)

RESILIENCIA = env_bool("RESILIENCIA", True)
CLIENTE_COMPARTIDO = env_bool("CLIENTE_COMPARTIDO", True)
TIMEOUT_S = env_float("TIMEOUT_MS", 2000) / 1000.0
TRIAJE_URLS = [u.strip() for u in os.getenv("TRIAJE_URLS", "http://127.0.0.1:8101,http://127.0.0.1:8102").split(",") if u.strip()]
AGENDA_URL = os.getenv("AGENDA_URL", "http://127.0.0.1:8201")
broker = None  # type: Optional[BrokerPort]


def _aviso_circuito(nodo: str, viejo: EstadoCircuito, nuevo: EstadoCircuito) -> None:
    """Observer: cada cambio de estado de un breaker se publica como evento distribuido."""
    tipo = {EstadoCircuito.ABIERTO: "nodo.caido", EstadoCircuito.CERRADO: "nodo.recuperado",
            EstadoCircuito.SEMIABIERTO: "nodo.sondeo"}[nuevo]
    log.warning("circuito %s: %s -> %s", nodo, viejo.value, nuevo.value)
    if broker is not None:
        try:
            broker.publicar(PREFIJO_EVENTOS + tipo, nuevo_evento(tipo, NODO, nodo=nodo, de=viejo.value,
                                                                 a=nuevo.value))
        except Exception:
            pass


cliente_triaje = ClienteHTTP(CLIENTE_COMPARTIDO, TIMEOUT_S, env_int("BULKHEAD_TRIAJE", 32))
cliente_agenda = ClienteHTTP(CLIENTE_COMPARTIDO, TIMEOUT_S, env_int("BULKHEAD_AGENDA", 16))
triaje = TriajeProxy(TRIAJE_URLS, cliente_triaje, RESILIENCIA, env_int("RETRY_MAX", 2),
                     env_float("RETRY_BASE_MS", 50), env_float("RETRY_MAX_MS", 400),
                     env_int("CB_UMBRAL_FALLOS", 3), env_float("CB_SEGUNDOS_RECUPERACION", 10),
                     Bulkhead("triaje", env_int("BULKHEAD_TRIAJE", 32)), _aviso_circuito)
agenda = AgendaProxy(AGENDA_URL, cliente_agenda, RESILIENCIA, env_int("RETRY_MAX", 2),
                     env_float("RETRY_BASE_MS", 50), env_float("RETRY_MAX_MS", 400),
                     CircuitBreaker(env_int("CB_UMBRAL_FALLOS", 3), env_float("CB_SEGUNDOS_RECUPERACION", 10),
                                    al_cambiar=lambda a, b: _aviso_circuito("agenda", a, b)),
                     Bulkhead("agenda", env_int("BULKHEAD_AGENDA", 16)))
mediador = SolicitudCitaMediator(triaje, agenda, MotorReglas())
INICIO = time.time()

app = FastAPI(title="Valle Vital distribuido - API Gateway", version="1.0.0",
              description="Punto unico de entrada. Datos SINTETICOS; no diagnostica.")


@app.on_event("startup")
def _arranque() -> None:
    global broker
    hilos = configurar_pool_hilos()
    broker = crear_broker()
    log.info("arranque hilos=%s resiliencia=%s cliente_compartido=%s triaje=%s agenda=%s broker=%s",
             hilos, RESILIENCIA, CLIENTE_COMPARTIDO, TRIAJE_URLS, AGENDA_URL, broker.tipo)


@app.post("/api/citas", status_code=201)
def solicitar_cita(s: SolicitudCita, response: Response,
                   x_correlation_id: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    """Secuencia A: triaje (sincrono, replica k) -> agenda (sincrono) -> eventos (asincronos)."""
    cid = nuevo_cid(x_correlation_id)
    response.headers[CABECERA_CID] = cid
    try:
        r = mediador.solicitar(s, cid)
    except SinCupo as exc:
        raise HTTPException(409, str(exc))
    except ServicioNoDisponible as exc:
        log.error("cid=%s agenda no disponible: %s", cid, exc)
        return JSONResponse({"detail": "agenda no disponible", "correlacion_id": cid}, 503,
                            headers={"Retry-After": "5", CABECERA_CID: cid})
    except Exception as exc:  # sin resiliencia los errores de red llegan aqui
        log.error("cid=%s error: %s", cid, exc)
        return JSONResponse({"detail": "error en backend: {}".format(type(exc).__name__),
                             "correlacion_id": cid}, 502, headers={CABECERA_CID: cid})
    log.info("cid=%s cita=%s triaje=%s degradado=%s", cid, r["cita"]["id"], r["nodo_triaje"], r["modo_degradado"])
    return r


@app.post("/api/triaje")
def solo_triaje(s: SolicitudTriage, response: Response,
                x_correlation_id: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    """Solo la prioridad sugerida (usado por el experimento de escalado E1)."""
    cid = nuevo_cid(x_correlation_id)
    response.headers[CABECERA_CID] = cid
    try:
        res, nodo = triaje.evaluar(s, cid)
        degradado = False
    except ServicioNoDisponible:
        res, nodo, degradado = mediador.respaldo.evaluar(s), "gateway(reglas)", True
        mediador.degradadas += 1
    except Exception as exc:
        return JSONResponse({"detail": "error en backend: {}".format(type(exc).__name__),
                             "correlacion_id": cid}, 502, headers={CABECERA_CID: cid})
    response.headers["X-Nodo"] = nodo
    return {"correlacion_id": cid, "nodo_triaje": nodo, "modo_degradado": degradado,
            "triaje": res.model_dump(mode="json")}


class PedidoTareas(BaseModel):
    tipo: str = Field(default="farmacia", description="farmacia | simulada | cpu")
    cantidad: int = Field(default=1, ge=1, le=20000)
    datos: Dict[str, Any] = Field(default_factory=dict)


@app.post("/api/tareas", status_code=202)
def encolar_tareas(p: PedidoTareas, x_correlation_id: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    """Queue-Based Load Leveling: se acepta (202) y se procesa despues en el pool de workers."""
    cid = nuevo_cid(x_correlation_id)
    ids = [broker.encolar(nueva_tarea(p.tipo, p.datos, cid)) for _ in range(p.cantidad)]
    return {"correlacion_id": cid, "encoladas": len(ids), "en_cola": broker.longitud()}


@app.get("/api/estado")
def estado() -> Dict[str, Any]:
    return {"nodo": NODO, "uptime_s": round(time.time() - INICIO, 1), "resiliencia": RESILIENCIA,
            "cliente_compartido": CLIENTE_COMPARTIDO, "triaje": triaje.estado(),
            "agenda": {"url": agenda.url, "circuito": agenda.breaker.estado.value},
            "bulkheads": {b.nombre: {"capacidad": b.capacidad, "en_uso": b.en_uso, "rechazos": b.rechazos}
                          for b in (triaje.bulkhead, agenda.bulkhead)},
            "solicitudes_degradadas": mediador.degradadas,
            "broker": {"tipo": broker.tipo if broker else None,
                       "entrega_tareas": broker.entrega_tareas if broker else None,
                       "en_cola": broker.longitud() if broker else None,
                       "hechas": broker.hechas() if broker else None}}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"estado": "ok", "nodo": NODO}


@app.get("/health/ready")
def ready() -> JSONResponse:
    """Health Endpoint Monitoring agregado: el gateway pregunta a cada nodo."""
    partes = {}  # type: Dict[str, str]
    for rep in triaje.replicas:
        partes[rep.nombre] = _sondear(cliente_triaje, rep.url + "/health")
    partes["agenda"] = _sondear(cliente_agenda, AGENDA_URL + "/health")
    partes["broker"] = "ok" if broker is not None and broker.ping() else "caido"
    triaje_ok = any(v == "ok" for k, v in partes.items() if k.startswith("triaje"))
    listo = triaje_ok and partes["agenda"] == "ok" and partes["broker"] == "ok"
    estado_global = "listo" if listo and all(v == "ok" for v in partes.values()) else ("degradado" if listo else "no_listo")
    return JSONResponse({"estado": estado_global, "nodos": partes}, 200 if listo else 503)


def _sondear(cliente: ClienteHTTP, url: str) -> str:
    try:
        return "ok" if cliente.get(url, 1.0).status_code == 200 else "error"
    except Exception:
        return "caido"
