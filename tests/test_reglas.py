import pytest

from common.dominio import Prioridad, SolicitudTriage
from common.reglas import MotorReglas

A = MotorReglas()


def sol(sint, edad=30, dias=1, temp=None):
    return SolicitudTriage(sintomas=sint, edad=edad, dias_evolucion=dias, temperatura_c=temp)


@pytest.mark.parametrize("sintoma", ["dolor de pecho", "Dificultad para respirar", "convulsión", "perdida de conciencia"])
def test_bandera_roja_es_urgente(sintoma):
    r = A.evaluar(sol([sintoma]))
    assert r.prioridad is Prioridad.URGENTE and r.requiere_confirmacion


def test_sintoma_leve_es_baja():
    assert A.evaluar(sol(["congestion nasal"])).prioridad is Prioridad.BAJA


def test_fiebre_alta_en_adulto_mayor_es_alta():
    r = A.evaluar(sol(["fiebre", "tos", "fatiga"], edad=72, dias=8, temp=39.4))
    assert r.prioridad is Prioridad.ALTA and "temperatura" in r.explicacion


def test_prioridad_media():
    assert A.evaluar(sol(["fiebre", "vomito"], temp=38.2)).prioridad is Prioridad.MEDIA


def test_advertencia_no_diagnostico():
    assert "no es un diagnostico" in A.evaluar(sol(["tos"])).advertencia
