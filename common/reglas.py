"""Motor de reglas de triaje (Strategy, heredado de la Act. 2). Local, determinista, sin red.

Es el motor de respaldo (fallback) en el nodo triaje y tambien en el gateway cuando
todas las replicas de triaje estan caidas (modo degradado).
"""
from __future__ import annotations

import unicodedata

from common.dominio import Prioridad, ResultadoTriage, SolicitudTriage

BANDERAS_ROJAS = {
    "dolor de pecho": "dolor toracico",
    "dificultad para respirar": "compromiso respiratorio",
    "perdida de conciencia": "alteracion de la conciencia",
    "convulsion": "convulsiones",
    "sangrado abundante": "hemorragia",
    "debilidad en un lado del cuerpo": "posible compromiso neurologico",
}
PESOS = {
    "fiebre": 2, "tos": 1, "dolor de cabeza": 1, "dolor abdominal": 2, "vomito": 2,
    "diarrea": 1, "mareo": 2, "dolor de garganta": 1, "erupcion en la piel": 1,
    "dolor de espalda": 1, "fatiga": 1, "congestion nasal": 1,
}


def _norm(txt: str) -> str:
    t = unicodedata.normalize("NFD", txt.lower().strip())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


class MotorReglas:
    nombre = "reglas"

    def evaluar(self, solicitud: SolicitudTriage) -> ResultadoTriage:
        sintomas = [_norm(s) for s in solicitud.sintomas]
        rojas = [BANDERAS_ROJAS[s] for s in sintomas if s in BANDERAS_ROJAS]
        if rojas:
            return self._res(Prioridad.URGENTE, 99, "Bandera roja reportada ({}).".format(", ".join(rojas)))
        puntaje = sum(PESOS.get(s, 1) for s in sintomas)
        motivos = ["{} sintoma(s) reportado(s) (puntaje base {})".format(len(sintomas), puntaje)]
        t = solicitud.temperatura_c
        if t is not None and t >= 39.0:
            puntaje += 3
            motivos.append("temperatura >= 39 C (+3)")
        elif t is not None and t >= 38.0:
            puntaje += 1
            motivos.append("temperatura >= 38 C (+1)")
        if solicitud.edad < 5 or solicitud.edad >= 65:
            puntaje += 2
            motivos.append("grupo de edad de mayor riesgo (<5 o >=65 anos) (+2)")
        if solicitud.dias_evolucion >= 7:
            puntaje += 1
            motivos.append("evolucion >= 7 dias (+1)")
        if puntaje >= 8:
            p = Prioridad.ALTA
        elif puntaje >= 5:
            p = Prioridad.MEDIA
        else:
            p = Prioridad.BAJA
        return self._res(p, puntaje, "; ".join(motivos) + ".")

    def _res(self, p: Prioridad, puntaje: int, expl: str) -> ResultadoTriage:
        return ResultadoTriage(prioridad=p, puntaje=puntaje, explicacion=expl, motor=self.nombre,
                               requiere_confirmacion=p.requiere_confirmacion)
