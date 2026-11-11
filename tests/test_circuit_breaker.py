import threading

import pytest

from common.circuit_breaker import CircuitBreaker, CircuitoAbierto, EstadoCircuito


class Reloj:
    t = 0.0

    def __call__(self):
        return self.t


def falla():
    raise RuntimeError("x")


def test_abre_tras_umbral_de_fallos():
    cb = CircuitBreaker(3, 30, Reloj())
    for _ in range(3):
        with pytest.raises(RuntimeError):
            cb.llamar(falla)
    assert cb.estado is EstadoCircuito.ABIERTO
    with pytest.raises(CircuitoAbierto):
        cb.llamar(lambda: 1)


def test_exito_reinicia_contador():
    cb = CircuitBreaker(3, 30, Reloj())
    for _ in range(2):
        with pytest.raises(RuntimeError):
            cb.llamar(falla)
    assert cb.llamar(lambda: "ok") == "ok" and cb.fallos == 0


def test_semiabierto_deja_pasar_una_sola_sonda_y_se_recupera():
    r = Reloj()
    cb = CircuitBreaker(2, 30, r)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            cb.llamar(falla)
    r.t = 31
    assert cb.estado is EstadoCircuito.SEMIABIERTO
    assert cb.permitir() is True      # la sonda
    assert cb.permitir() is False     # nadie mas mientras la sonda esta en curso
    cb.registrar_exito()
    assert cb.estado is EstadoCircuito.CERRADO


def test_semiabierto_vuelve_a_abrir_si_falla():
    r = Reloj()
    cb = CircuitBreaker(2, 30, r)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            cb.llamar(falla)
    r.t = 31
    with pytest.raises(RuntimeError):
        cb.llamar(falla)
    assert cb.estado is EstadoCircuito.ABIERTO


def test_observador_recibe_cambios_de_estado():
    cambios = []
    cb = CircuitBreaker(1, 30, Reloj(), al_cambiar=lambda a, b: cambios.append((a.value, b.value)))
    with pytest.raises(RuntimeError):
        cb.llamar(falla)
    assert cambios == [("cerrado", "abierto")]


def test_contador_de_fallos_es_exacto_con_muchos_hilos():
    """AJUSTE de la Act. 4: el breaker es seguro entre hilos (no se pierden incrementos)."""
    cb = CircuitBreaker(10 ** 9, 30, Reloj())
    barrera = threading.Barrier(16)

    def golpear():
        barrera.wait()
        for _ in range(2000):
            cb.registrar_fallo()

    hilos = [threading.Thread(target=golpear) for _ in range(16)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    assert cb.fallos == 16 * 2000
