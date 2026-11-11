"""Proxy, Retry, Circuit Breaker por replica, Bulkhead y Mediator, con backends falsos."""
import httpx
import pytest

from common.circuit_breaker import CircuitBreaker
from common.dominio import Prioridad, SolicitudCita, SolicitudTriage
from common.reglas import MotorReglas
from servicios.gateway.clientes import AgendaProxy, ErrorBackend, ServicioNoDisponible, SinCupo, TriajeProxy
from servicios.gateway.mediador import SolicitudCitaMediator
from servicios.gateway.resiliencia import Bulkhead, BulkheadLleno, espera_backoff, reintentar

RES = {"prioridad": "media", "puntaje": 5, "explicacion": "x", "motor": "llm_simulado", "requiere_confirmacion": False}
CITA = {"id": "C1", "paciente_id": "PX", "profesional_id": "P1", "franja_id": "P1@1", "sede_id": "S1",
        "especialidad": "Medicina general", "inicio": "2026-11-10T08:00:00", "prioridad": "media",
        "estado": "confirmada"}


class FalsoCliente:
    """Simula la red: `caidas` son hosts que rechazan la conexion."""

    def __init__(self, caidas=(), status=200, cuerpo=None):
        self.caidas, self.status, self.cuerpo = set(caidas), status, cuerpo
        self.llamadas = []

    def post(self, url, json, headers):
        self.llamadas.append(url)
        host = url.split("/")[2]
        if host in self.caidas:
            raise httpx.ConnectError("conexion rechazada")
        return httpx.Response(self.status, json=self.cuerpo or RES, headers={"X-Nodo": host},
                              request=httpx.Request("POST", url))


def proxy(cliente, resiliencia=True, n=2):
    return TriajeProxy(["http://t{}".format(i + 1) for i in range(n)], cliente, resiliencia, 2, 1, 2, 3, 30,
                       Bulkhead("triaje", 8))


SOL = SolicitudTriage(sintomas=["tos"], edad=30)


def test_round_robin_reparte_entre_replicas():
    c = FalsoCliente()
    p = proxy(c)
    nodos = [p.evaluar(SOL)[1] for _ in range(4)]
    assert nodos == ["t1", "t2", "t1", "t2"]


def test_retry_salta_a_la_replica_viva():
    c = FalsoCliente(caidas={"t2"})
    p = proxy(c)
    assert all(p.evaluar(SOL)[1] == "t1" for _ in range(6))


def test_breaker_por_replica_se_abre_y_deja_de_llamarla():
    c = FalsoCliente(caidas={"t2"})
    p = proxy(c)
    for _ in range(10):
        p.evaluar(SOL)
    assert p.replicas[1].breaker.estado.value == "abierto"
    llamadas_t2 = sum(1 for u in c.llamadas if "//t2/" in u)
    assert llamadas_t2 == 3  # exactamente el umbral; despues el circuito evita la replica


def test_sin_resiliencia_el_error_llega_al_cliente():
    c = FalsoCliente(caidas={"t2"})
    p = proxy(c, resiliencia=False)
    p.evaluar(SOL)  # t1
    with pytest.raises(httpx.ConnectError):
        p.evaluar(SOL)  # t2


def test_todas_caidas_lanza_servicio_no_disponible():
    p = proxy(FalsoCliente(caidas={"t1", "t2"}))
    with pytest.raises(ServicioNoDisponible):
        p.evaluar(SOL)


def test_error_4xx_no_se_reintenta():
    c = FalsoCliente(status=422, cuerpo={"detail": "x"})
    p = proxy(c)
    with pytest.raises(ErrorBackend):
        p.evaluar(SOL)
    assert len(c.llamadas) == 1


def agenda(cliente, resiliencia=True):
    return AgendaProxy("http://agenda", cliente, resiliencia, 2, 1, 2, CircuitBreaker(3, 30), Bulkhead("agenda", 4))


def test_post_de_cita_no_se_reintenta_ante_500():
    c = FalsoCliente(status=500, cuerpo={"detail": "x"})
    with pytest.raises(ErrorBackend):
        agenda(c).asignar(_peticion())
    assert len(c.llamadas) == 1  # no idempotente: un solo intento


def test_post_de_cita_se_reintenta_si_la_conexion_no_se_abrio():
    c = FalsoCliente(caidas={"agenda"})
    with pytest.raises(ServicioNoDisponible):
        agenda(c).asignar(_peticion())
    assert len(c.llamadas) == 3  # 1 + 2 reintentos (ConnectError: la agenda nunca la recibio)


def test_agenda_sin_cupo():
    c = FalsoCliente(status=409, cuerpo={"detail": "sin franjas"})
    with pytest.raises(SinCupo):
        agenda(c).asignar(_peticion())


def _peticion():
    from common.dominio import PeticionAsignacion
    return PeticionAsignacion(paciente_id="PX", sede_id="S1", especialidad="Medicina general", prioridad=Prioridad.MEDIA)


def test_mediador_degrada_a_reglas_si_triaje_cae():
    triaje = proxy(FalsoCliente(caidas={"t1", "t2"}))
    m = SolicitudCitaMediator(triaje, agenda(FalsoCliente(cuerpo=CITA)), MotorReglas())
    r = m.solicitar(SolicitudCita(paciente_id="PX", sede_id="S1", especialidad="Medicina general",
                                  sintomas=["tos"], edad=30), "cid-1")
    assert r["modo_degradado"] is True and r["nodo_triaje"] == "gateway(reglas)" and r["cita"]["id"] == "C1"


def test_backoff_con_jitter_respeta_el_techo():
    for intento in range(6):
        assert 0 <= espera_backoff(intento, 50, 400) <= 0.4


def test_reintentar_solo_transitorios():
    llamadas = []

    def fn(i):
        llamadas.append(i)
        raise ValueError("permanente")

    with pytest.raises(ValueError):
        reintentar(fn, 3, 1, 2, lambda e: isinstance(e, KeyError), dormir=lambda s: None)
    assert llamadas == [0]


def test_bulkhead_rechaza_cuando_esta_lleno():
    import threading
    b = Bulkhead("x", 1, espera_s=0.05)
    evento = threading.Event()
    t = threading.Thread(target=lambda: b.ejecutar(evento.wait))
    t.start()
    import time
    time.sleep(0.05)
    with pytest.raises(BulkheadLleno):
        b.ejecutar(lambda: 1)
    evento.set()
    t.join()
    assert b.rechazos == 1
