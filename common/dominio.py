"""Modelo de dominio compartido (heredado de la Act. 2) y contrato JSON entre nodos.

Compatible con Python 3.8: Pydantic evalua las anotaciones en tiempo de ejecucion,
por eso se usan typing.List / typing.Optional y no list[...] ni X | None.
Todos los datos son SINTETICOS (personas y entidad ficticias).
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class Prioridad(str, Enum):
    BAJA = "baja"
    MEDIA = "media"
    ALTA = "alta"
    URGENTE = "urgente"

    @property
    def requiere_confirmacion(self) -> bool:
        return self in (Prioridad.ALTA, Prioridad.URGENTE)


class SolicitudTriage(BaseModel):
    sintomas: List[str] = Field(min_length=1, description="Sintomas del catalogo o texto libre")
    edad: int = Field(ge=0, le=120)
    dias_evolucion: int = Field(default=1, ge=0, le=365)
    temperatura_c: Optional[float] = Field(default=None, ge=30, le=45)


class ResultadoTriage(BaseModel):
    prioridad: Prioridad
    puntaje: int
    explicacion: str
    motor: str
    requiere_confirmacion: bool
    advertencia: str = (
        "Orientacion automatizada de caracter academico: no es un diagnostico. "
        "La decision clinica corresponde al profesional de la salud."
    )


class SolicitudCita(BaseModel):
    """Entrada del gateway: datos del paciente sintetico + sintomas."""
    paciente_id: str
    sede_id: str
    especialidad: str
    sintomas: List[str] = Field(min_length=1)
    edad: int = Field(ge=0, le=120)
    dias_evolucion: int = Field(default=1, ge=0, le=365)
    temperatura_c: Optional[float] = Field(default=None, ge=30, le=45)

    def triage(self) -> SolicitudTriage:
        return SolicitudTriage(sintomas=self.sintomas, edad=self.edad,
                               dias_evolucion=self.dias_evolucion, temperatura_c=self.temperatura_c)


class PeticionAsignacion(BaseModel):
    """Contrato gateway -> agenda."""
    paciente_id: str
    sede_id: str
    especialidad: str
    prioridad: Prioridad


class EstadoCita(str, Enum):
    PENDIENTE_CONFIRMACION = "pendiente_confirmacion"
    CONFIRMADA = "confirmada"
    CANCELADA = "cancelada"


class Cita(BaseModel):
    id: str
    paciente_id: str
    profesional_id: str
    franja_id: str
    sede_id: str
    especialidad: str
    inicio: datetime
    prioridad: Prioridad
    estado: EstadoCita


class Franja(BaseModel):
    id: str
    profesional_id: str
    inicio: datetime
    ocupada: bool = False


class Profesional(BaseModel):
    id: str
    nombre: str
    especialidad: str
    sede_id: str


class InventarioSede(BaseModel):
    id: str
    sede_id: str
    medicamento_id: str
    stock: int = Field(ge=0)
    minimo: int = Field(ge=0)


class SugerenciaTraslado(BaseModel):
    id: str
    medicamento_id: str
    sede_destino_id: str
    sede_origen_id: Optional[str]
    cantidad: int
    motivo: str
