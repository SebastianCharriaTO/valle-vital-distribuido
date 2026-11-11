"""Mediator (GoF): SolicitudCitaMediator coordina triaje -> agenda. Triaje y agenda no se
conocen entre si; solo el mediador conoce el orden del flujo. Para no volverse un
'God Object' (Refactoring.Guru, s. f.), no contiene reglas de negocio: solo orquesta."""
from __future__ import annotations

from typing import Any, Dict

from common.dominio import PeticionAsignacion, SolicitudCita
from common.reglas import MotorReglas
from servicios.gateway.clientes import AgendaProxy, ServicioNoDisponible, TriajeProxy


class SolicitudCitaMediator:
    def __init__(self, triaje: TriajeProxy, agenda: AgendaProxy, respaldo: MotorReglas) -> None:
        self.triaje, self.agenda, self.respaldo = triaje, agenda, respaldo
        self.degradadas = 0

    def solicitar(self, s: SolicitudCita, cid: str) -> Dict[str, Any]:
        degradado = False
        try:
            resultado, nodo = self.triaje.evaluar(s.triage(), cid)
        except ServicioNoDisponible:
            # todas las replicas caidas: el gateway usa la MISMA estrategia de reglas en local
            resultado, nodo, degradado = self.respaldo.evaluar(s.triage()), "gateway(reglas)", True
            self.degradadas += 1
        cita = self.agenda.asignar(PeticionAsignacion(paciente_id=s.paciente_id, sede_id=s.sede_id,
                                                      especialidad=s.especialidad,
                                                      prioridad=resultado.prioridad), cid)
        return {"correlacion_id": cid, "nodo_triaje": nodo, "modo_degradado": degradado,
                "triaje": resultado.model_dump(mode="json"), "cita": cita.model_dump(mode="json")}
