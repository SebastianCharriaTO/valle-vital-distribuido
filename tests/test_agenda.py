import threading
from datetime import datetime

from common.dominio import Prioridad, Profesional
from servicios.agenda.servicio import AgendaService, SinDisponibilidad

PROFS = [Profesional(id="P1", nombre="Dr. Demo", especialidad="Medicina general", sede_id="S1")]
RELOJ = lambda: datetime(2026, 11, 9, 7, 0)  # noqa: E731


def _rafaga(servicio, n=50):
    barrera = threading.Barrier(n)
    errores = []

    def pedir(i):
        barrera.wait()
        try:
            servicio.asignar("PX{}".format(i), "S1", "Medicina general", Prioridad.URGENTE)
        except SinDisponibilidad:
            errores.append(i)

    hilos = [threading.Thread(target=pedir, args=(i,)) for i in range(n)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    return errores


def test_asigna_la_primera_franja_libre():
    s = AgendaService(PROFS, dias=2, reloj=RELOJ)
    c = s.asignar("PX", "S1", "Medicina general", Prioridad.URGENTE)
    assert c.franja_id == "P1@2026111008" and c.estado.value == "pendiente_confirmacion"


def test_baja_espera_dos_dias():
    s = AgendaService(PROFS, dias=5, reloj=RELOJ)
    c = s.asignar("PX", "S1", "Medicina general", Prioridad.BAJA)
    assert c.inicio.day == 11 and c.estado.value == "confirmada"


def test_sin_cupo_lanza_excepcion():
    s = AgendaService(PROFS, dias=1, reloj=RELOJ)
    for i in range(7):
        s.asignar("PX{}".format(i), "S1", "Medicina general", Prioridad.URGENTE)
    try:
        s.asignar("PX8", "S1", "Medicina general", Prioridad.URGENTE)
        assert False, "debio fallar"
    except SinDisponibilidad:
        pass


def test_con_lock_no_hay_dobles_asignaciones():
    s = AgendaService(PROFS, dias=7, usar_lock=True, latencia_escritura_ms=2, reloj=RELOJ)
    errores = _rafaga(s, 50)
    assert s.dobles_asignaciones() == 0
    assert len(s.citas()) == 49 and len(errores) == 1  # 49 franjas para 50 pacientes


def test_sin_lock_la_carrera_produce_dobles_asignaciones():
    """Reproduce el defecto heredado de la Act. 2 (por eso se agrego el lock)."""
    s = AgendaService(PROFS, dias=7, usar_lock=False, latencia_escritura_ms=2, reloj=RELOJ)
    _rafaga(s, 50)
    assert s.dobles_asignaciones() > 0
