"""Endpoints de los nodos con TestClient (sin red entre nodos; broker en memoria)."""
from fastapi.testclient import TestClient


def test_triaje_responde_y_marca_el_nodo():
    from servicios.triaje.app import app
    with TestClient(app) as c:
        r = c.post("/triaje", json={"sintomas": ["dolor de pecho"], "edad": 40}, headers={"X-Correlation-ID": "abc"})
        assert r.status_code == 200 and r.json()["prioridad"] == "urgente"
        assert r.headers["X-Correlation-ID"] == "abc" and r.headers["X-Nodo"]


def test_triaje_degrada_si_el_proveedor_ia_cae():
    from servicios.triaje.app import app, motor
    with TestClient(app) as c:
        motor.timeout_ms = 0
        c.post("/admin/proveedor-ia", json={"tasa_fallo": 1.0})
        r = c.post("/triaje", json={"sintomas": ["tos"], "edad": 30})
        assert r.json()["motor"] == "reglas" and "Modo degradado" in r.json()["explicacion"]
        c.post("/admin/proveedor-ia", json={"tasa_fallo": 0.0})


def test_agenda_asigna_publica_evento_y_encola_recordatorio():
    from servicios.agenda import app as modulo
    with TestClient(modulo.app) as c:
        sub = modulo.broker.suscribir()
        r = c.post("/citas", json={"paciente_id": "PX", "sede_id": "S1", "especialidad": "Medicina general",
                                   "prioridad": "alta"}, headers={"X-Correlation-ID": "cid9"})
        assert r.status_code == 201 and r.json()["estado"] == "pendiente_confirmacion"
        ev = sub.recibir(1.0)
        assert ev["tipo"] == "cita.asignada" and ev["cid"] == "cid9"
        assert modulo.broker.longitud() >= 1
        assert c.get("/resumen").json()["dobles_asignaciones"] == 0


def test_agenda_sin_cupo_devuelve_409():
    from servicios.agenda.app import app
    with TestClient(app) as c:
        r = c.post("/citas", json={"paciente_id": "PX", "sede_id": "S9", "especialidad": "X", "prioridad": "baja"})
        assert r.status_code == 409


def test_gateway_degrada_con_backends_caidos_y_reporta_no_listo():
    from servicios.gateway.app import app
    with TestClient(app) as c:
        r = c.post("/api/triaje", json={"sintomas": ["tos"], "edad": 30})
        assert r.status_code == 200 and r.json()["modo_degradado"] is True
        assert c.get("/health/ready").status_code == 503
        r = c.post("/api/citas", json={"paciente_id": "PX", "sede_id": "S1", "especialidad": "Medicina general",
                                       "sintomas": ["tos"], "edad": 30})
        assert r.status_code == 503 and r.headers["Retry-After"] == "5"


def test_gateway_encola_tareas():
    from servicios.gateway.app import app
    with TestClient(app) as c:
        r = c.post("/api/tareas", json={"tipo": "simulada", "cantidad": 5})
        assert r.status_code == 202 and r.json()["encoladas"] == 5
        assert c.get("/api/estado").json()["broker"]["en_cola"] >= 5
