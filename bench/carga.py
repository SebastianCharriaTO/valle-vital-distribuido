"""Generador de carga propio (asyncio + httpx), sin dependencias extra.

Cliente: HTTP/1.1 minimo sobre asyncio (ver _ConexionHTTP).
Modelo de usuarios en lazo cerrado: cada usuario virtual envia una peticion, espera la
respuesta y envia la siguiente. Registra por peticion: instante, latencia, codigo y nodo.
Las metricas siguen las cuatro senales doradas (Ewaschuk, 2017): latencia (p50/p95/p99,
separando exitos y errores), trafico (req/s), errores (%) y saturacion (se mide aparte).

  python -m bench.carga --url http://127.0.0.1:8000/api/triaje --usuarios 50 --duracion 20
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import statistics
import time
from typing import Callable, Dict, List, Optional

import json
from urllib.parse import urlparse

CUERPO_TRIAJE = {"sintomas": ["fiebre", "tos"], "edad": 34, "dias_evolucion": 2, "temperatura_c": 38.4}


class Registro:
    __slots__ = ("t", "lat_ms", "status", "nodo")

    def __init__(self, t: float, lat_ms: float, status: int, nodo: str) -> None:
        self.t, self.lat_ms, self.status, self.nodo = t, lat_ms, status, nodo


class _ConexionHTTP:
    """Cliente HTTP/1.1 minimo con keep-alive sobre asyncio (solo biblioteca estandar).
    Se usa en lugar de httpx.AsyncClient porque este consumia casi un nucleo entero del
    generador (medido): el generador no debe ser el cuello de botella del experimento."""

    def __init__(self, host: str, puerto: int) -> None:
        self.host, self.puerto = host, puerto
        self.r = None  # type: Optional[asyncio.StreamReader]
        self.w = None  # type: Optional[asyncio.StreamWriter]

    async def post(self, ruta: str, cuerpo: bytes, timeout_s: float):
        if self.w is None:
            self.r, self.w = await asyncio.wait_for(asyncio.open_connection(self.host, self.puerto), timeout_s)
        pet = ("POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\n"
               "Content-Length: {}\r\n\r\n").format(ruta, self.host, self.puerto, len(cuerpo)).encode() + cuerpo
        self.w.write(pet)
        return await asyncio.wait_for(self._respuesta(), timeout_s)

    async def _respuesta(self):
        cab = await self.r.readuntil(b"\r\n\r\n")
        lineas = cab.decode("latin-1").split("\r\n")
        status = int(lineas[0].split()[1])
        h = {}
        for l in lineas[1:]:
            if ":" in l:
                k, v = l.split(":", 1)
                h[k.strip().lower()] = v.strip()
        cuerpo = await self.r.readexactly(int(h.get("content-length", "0")))
        if h.get("connection", "").lower() == "close":
            self.cerrar()
        return status, h, cuerpo

    def cerrar(self) -> None:
        if self.w is not None:
            self.w.close()
        self.r = self.w = None


async def correr(url: str, usuarios: int, duracion_s: float, calentamiento_s: float = 3.0,
                 cuerpo: Optional[Callable[[int], dict]] = None, timeout_s: float = 10.0,
                 eventos: Optional[Dict[float, Callable[[], None]]] = None) -> List[Registro]:
    """Devuelve los registros posteriores al calentamiento. `eventos` = {segundo: accion} se
    ejecutan en un hilo aparte (p. ej. matar un nodo en t=10 s)."""
    cuerpo = cuerpo or (lambda i: CUERPO_TRIAJE)
    u = urlparse(url)
    registros = []  # type: List[Registro]
    t0 = time.perf_counter()
    fin = t0 + calentamiento_s + duracion_s
    contador = [0]

    async def usuario() -> None:
        con = _ConexionHTTP(u.hostname or "127.0.0.1", u.port or 80)
        while time.perf_counter() < fin:
            contador[0] += 1
            datos = json.dumps(cuerpo(contador[0])).encode()
            ini = time.perf_counter()
            try:
                status, h, _ = await con.post(u.path, datos, timeout_s)
                nodo = h.get("x-nodo", "")
            except Exception:
                status, nodo = 599, ""  # error de transporte o timeout del cliente
                con.cerrar()
            ahora = time.perf_counter()
            t_rel = ini - t0 - calentamiento_s
            if t_rel >= 0:
                registros.append(Registro(t_rel, (ahora - ini) * 1000.0, status, nodo))
        con.cerrar()

    async def programador() -> None:
        if not eventos:
            return
        loop = asyncio.get_event_loop()
        for seg in sorted(eventos):
            espera = t0 + calentamiento_s + seg - time.perf_counter()
            if espera > 0:
                await asyncio.sleep(espera)
            await loop.run_in_executor(None, eventos[seg])

    await asyncio.gather(programador(), *[usuario() for _ in range(usuarios)])
    return registros


def percentil(valores: List[float], p: float) -> float:
    if not valores:
        return float("nan")
    if len(valores) == 1:
        return valores[0]
    return statistics.quantiles(valores, n=100, method="inclusive")[int(p) - 1]


def resumen(registros: List[Registro], duracion_s: float) -> Dict[str, float]:
    ok = [r.lat_ms for r in registros if r.status < 400]
    err = [r for r in registros if r.status >= 400]
    n = len(registros)
    return {
        "peticiones": n,
        "rps": round(n / duracion_s, 1),
        "rps_ok": round(len(ok) / duracion_s, 1),
        "errores_pct": round(100.0 * len(err) / n, 2) if n else float("nan"),
        "p50_ms": round(percentil(ok, 50), 1),
        "p95_ms": round(percentil(ok, 95), 1),
        "p99_ms": round(percentil(ok, 99), 1),
        "media_ms": round(statistics.mean(ok), 1) if ok else float("nan"),
        "degradadas_pct": round(100.0 * sum(1 for r in registros if r.nodo == "gateway(reglas)") / n, 2) if n else 0.0,
    }


def por_segundo(registros: List[Registro], duracion_s: float) -> List[Dict[str, float]]:
    filas = []
    for s in range(int(duracion_s)):
        rs = [r for r in registros if s <= r.t < s + 1]
        ok = [r.lat_ms for r in rs if r.status < 400]
        filas.append({"segundo": s, "peticiones": len(rs),
                      "errores_pct": round(100.0 * sum(1 for r in rs if r.status >= 400) / len(rs), 2) if rs else 0.0,
                      "p95_ms": round(percentil(ok, 95), 1) if ok else float("nan")})
    return filas


def guardar_csv(ruta: str, filas: List[Dict[str, object]]) -> None:
    if not filas:
        return
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000/api/triaje")
    ap.add_argument("--usuarios", type=int, default=50)
    ap.add_argument("--duracion", type=float, default=20)
    ap.add_argument("--calentamiento", type=float, default=3)
    a = ap.parse_args()
    regs = asyncio.get_event_loop().run_until_complete(correr(a.url, a.usuarios, a.duracion, a.calentamiento))
    print(resumen(regs, a.duracion))


if __name__ == "__main__":
    main()
