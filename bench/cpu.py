"""Saturacion: % de CPU de cada nodo (proceso) durante una ventana, leido de /proc (Linux/WSL)."""
from __future__ import annotations

import os
from typing import Dict

TICKS = os.sysconf("SC_CLK_TCK")


def _ticks(pid: int) -> int:
    try:
        campos = open("/proc/{}/stat".format(pid)).read().rsplit(")", 1)[1].split()
        return int(campos[11]) + int(campos[12])  # utime + stime
    except Exception:
        return 0


def instantanea(pids: Dict[str, int]) -> Dict[str, int]:
    return {n: _ticks(p) for n, p in pids.items()}


def porcentaje(antes: Dict[str, int], despues: Dict[str, int], segundos: float) -> Dict[str, float]:
    """100 % = un nucleo completo."""
    return {n: round(100.0 * (despues.get(n, 0) - antes.get(n, 0)) / TICKS / segundos, 1) for n in antes}
