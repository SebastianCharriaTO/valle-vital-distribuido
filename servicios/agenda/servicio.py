"""Servicio de agenda (heredado de CitaService, Act. 2). Es el UNICO escritor de franjas y
citas (database per service), por eso corre en una sola replica.

AJUSTE de la Act. 4 (experimento E4): `asignar` hace "buscar franja libre -> marcarla ocupada".
FastAPI ejecuta los endpoints `def` en un pool de hilos, asi que dos solicitudes simultaneas
podian leer la MISMA franja libre y asignarla dos veces (condicion de carrera). La seccion
critica ahora va bajo un threading.Lock. `usar_lock=False` reproduce el defecto para medirlo.
"""
from __future__ import annotations

import itertools
import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from common.dominio import Cita, EstadoCita, Franja, Prioridad, Profesional

ESPERA_MIN_DIAS = {Prioridad.URGENTE: 0, Prioridad.ALTA: 0, Prioridad.MEDIA: 1, Prioridad.BAJA: 2}
HORAS = (8, 9, 10, 11, 14, 15, 16)


class SinDisponibilidad(Exception):
    pass


class AgendaService:
    def __init__(self, profesionales: List[Profesional], dias: int = 7, usar_lock: bool = True,
                 latencia_escritura_ms: float = 0.0,
                 reloj: Callable[[], datetime] = datetime.now) -> None:
        self.usar_lock = usar_lock
        self.latencia_escritura_ms = latencia_escritura_ms
        self._profs = {p.id: p for p in profesionales}
        self._reloj = reloj
        self._lock = threading.Lock()
        self._dias = dias
        self.reiniciar()

    def reiniciar(self) -> None:
        self._franjas = {}  # type: Dict[str, Franja]
        self._citas = {}  # type: Dict[str, Cita]
        self._seq = itertools.count(1)
        hoy = self._reloj().replace(hour=0, minute=0, second=0, microsecond=0)
        for p in self._profs.values():
            for d in range(1, self._dias + 1):
                for h in HORAS:
                    ini = (hoy + timedelta(days=d)).replace(hour=h)
                    fid = "{}@{:%Y%m%d%H}".format(p.id, ini)
                    self._franjas[fid] = Franja(id=fid, profesional_id=p.id, inicio=ini)

    def _buscar_libre(self, sede_id: str, especialidad: str, prioridad: Prioridad) -> Franja:
        profs = {pid for pid, p in self._profs.items()
                 if p.sede_id == sede_id and p.especialidad == especialidad}
        desde = self._reloj() + timedelta(days=ESPERA_MIN_DIAS[prioridad])
        libres = sorted((f for f in self._franjas.values()
                         if not f.ocupada and f.profesional_id in profs and f.inicio >= desde),
                        key=lambda f: f.inicio)
        if not libres:
            raise SinDisponibilidad("Sin franjas libres en {} para {}".format(sede_id, especialidad))
        return libres[0]

    def _seccion_critica(self, paciente_id: str, sede_id: str, especialidad: str,
                         prioridad: Prioridad) -> Cita:
        f = self._buscar_libre(sede_id, especialidad, prioridad)
        if self.latencia_escritura_ms > 0:  # ida y vuelta simulada a la base de datos
            time.sleep(self.latencia_escritura_ms / 1000.0)
        self._franjas[f.id] = f.model_copy(update={"ocupada": True})
        estado = EstadoCita.PENDIENTE_CONFIRMACION if prioridad.requiere_confirmacion else EstadoCita.CONFIRMADA
        cita = Cita(id="C{:05d}".format(next(self._seq)), paciente_id=paciente_id,
                    profesional_id=f.profesional_id, franja_id=f.id, sede_id=sede_id,
                    especialidad=especialidad, inicio=f.inicio, prioridad=prioridad, estado=estado)
        self._citas[cita.id] = cita
        return cita

    def asignar(self, paciente_id: str, sede_id: str, especialidad: str, prioridad: Prioridad) -> Cita:
        if self.usar_lock:
            with self._lock:
                return self._seccion_critica(paciente_id, sede_id, especialidad, prioridad)
        return self._seccion_critica(paciente_id, sede_id, especialidad, prioridad)

    def citas(self) -> List[Cita]:
        return sorted(self._citas.values(), key=lambda c: c.id)

    def dobles_asignaciones(self) -> int:
        """Citas de mas sobre una misma franja (deberia ser 0)."""
        conteo = {}  # type: Dict[str, int]
        for c in self._citas.values():
            conteo[c.franja_id] = conteo.get(c.franja_id, 0) + 1
        return sum(n - 1 for n in conteo.values() if n > 1)

    def resumen(self) -> dict:
        libres = sum(1 for f in self._franjas.values() if not f.ocupada)
        return {"franjas": len(self._franjas), "libres": libres, "citas": len(self._citas),
                "dobles_asignaciones": self.dobles_asignaciones(), "lock": self.usar_lock}

    def obtener(self, cita_id: str) -> Optional[Cita]:
        return self._citas.get(cita_id)
