"""Calculo de sugerencias de traslado (heredado de FarmaciaService, Act. 2) como funcion pura,
para que cualquier hilo de cualquier worker la ejecute sin estado compartido."""
from __future__ import annotations

from typing import List

from common.dominio import InventarioSede, SugerenciaTraslado

FACTOR_EXCEDENTE = 1.5  # una sede cede solo lo que supera 1.5 x su minimo


def sugerencias(inventario: List[InventarioSede]) -> List[SugerenciaTraslado]:
    salida = []  # type: List[SugerenciaTraslado]
    for falta in (i for i in inventario if i.stock < i.minimo):
        necesario = falta.minimo * 2 - falta.stock
        donantes = sorted((i for i in inventario if i.medicamento_id == falta.medicamento_id
                           and i.sede_id != falta.sede_id
                           and i.stock - int(i.minimo * FACTOR_EXCEDENTE) > 0),
                          key=lambda i: i.stock - i.minimo, reverse=True)
        if donantes:
            d = donantes[0]
            cant = min(necesario, d.stock - int(d.minimo * FACTOR_EXCEDENTE))
            origen = d.sede_id
            motivo = "Stock {} < minimo {}; {} tiene excedente ({}).".format(falta.stock, falta.minimo, d.sede_id, d.stock)
        else:
            cant, origen = necesario, None
            motivo = "Stock {} < minimo {}; ninguna sede tiene excedente: pedir a la central.".format(falta.stock, falta.minimo)
        salida.append(SugerenciaTraslado(id="S{:04d}".format(len(salida) + 1), medicamento_id=falta.medicamento_id,
                                         sede_destino_id=falta.sede_id, sede_origen_id=origen,
                                         cantidad=cant, motivo=motivo))
    return salida
