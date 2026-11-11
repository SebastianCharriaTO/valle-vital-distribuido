"""Experimentos de escalabilidad y robustez E1-E6 (ver README e informe, seccion 7).

Levanta los nodos como procesos locales (scripts/run_local.py), genera carga y guarda los
resultados crudos y resumidos en bench/resultados/. Cada escenario se repite REPS veces y
se reporta la mediana.

  python -m bench.experimentos --broker redis            # todos (unos 20 min)
  python -m bench.experimentos --solo e1 e4 --reps 1     # rapido
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from bench import cpu  # noqa: E402
from bench.carga import CUERPO_TRIAJE, correr, guardar_csv, percentil, por_segundo, resumen  # noqa: E402
from common.broker import crear_broker, nueva_tarea  # noqa: E402
from common.dominio import SolicitudTriage  # noqa: E402
from common.reglas import MotorReglas  # noqa: E402
from scripts import run_local as rl  # noqa: E402

RES = RAIZ / "bench" / "resultados"
GW = "http://127.0.0.1:8000"
DUR, CAL = 20.0, 3.0
BROKER = "redis"
REPS = 3


def _loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())


def _post(url: str, cuerpo: dict) -> Any:
    req = urllib.request.Request(url, data=json.dumps(cuerpo).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _mediana(filas: List[Dict[str, Any]], claves: List[str]) -> Dict[str, Any]:
    return {k: round(statistics.median([f[k] for f in filas]), 3) for k in claves}


def _pids() -> Dict[str, int]:
    p = {k: v["pid"] for k, v in rl._leer().items()}
    p["generador"] = os.getpid()
    return p


def _url_broker() -> str:
    return "redis://127.0.0.1:6379/0" if BROKER == "redis" else "local://127.0.0.1:50000"


# --------------------------------------------------------------------------- E1
def e1_escalado(reps: int) -> Dict[str, Any]:
    """Replicas de triaje 1..4; 50 usuarios; IA simulada de 100 ms; 8 hilos por nodo de triaje."""
    filas = []
    for n in (1, 2, 3, 4):
        for rep in range(reps):
            rl.down()
            rl.up(BROKER, n, con_worker=False, con_monitor=False,
                  por_nodo={"triaje": {"HILOS_POR_NODO": "8", "LLM_LATENCIA_MS": "100"},
                            "gateway": {"BULKHEAD_TRIAJE": "64"}})
            pids = _pids()
            a, t = cpu.instantanea(pids), time.time()
            regs = _loop().run_until_complete(correr(GW + "/api/triaje", 50, DUR, CAL))
            uso = cpu.porcentaje(a, cpu.instantanea(pids), time.time() - t)
            f = {"replicas": n, "rep": rep + 1}
            f.update(resumen(regs, DUR))
            f["cpu_gateway_pct"] = uso.get("gateway", 0)
            f["cpu_triaje_prom_pct"] = round(statistics.mean([v for k, v in uso.items() if k.startswith("triaje")]), 1)
            f["cpu_generador_pct"] = uso.get("generador", 0)
            filas.append(f)
            print("E1", f, flush=True)
    guardar_csv(str(RES / "e1_escalado.csv"), filas)
    claves = ["rps", "p50_ms", "p95_ms", "p99_ms", "errores_pct", "cpu_gateway_pct", "cpu_triaje_prom_pct", "cpu_generador_pct"]
    return {str(n): _mediana([f for f in filas if f["replicas"] == n], claves) for n in (1, 2, 3, 4)}


# --------------------------------------------------------------------------- E6 (ajuste)
def e6_cliente_http(reps: int) -> Dict[str, Any]:
    """Ajuste: cliente HTTP por peticion (version inicial) vs. pool de conexiones compartido."""
    filas = []
    for comp in ("0", "1"):
        for rep in range(reps):
            rl.down()
            rl.up(BROKER, 2, con_worker=False, con_monitor=False,
                  por_nodo={"triaje": {"HILOS_POR_NODO": "8"},
                            "gateway": {"BULKHEAD_TRIAJE": "64", "CLIENTE_COMPARTIDO": comp}})
            pids = _pids()
            a, t = cpu.instantanea(pids), time.time()
            regs = _loop().run_until_complete(correr(GW + "/api/triaje", 50, DUR, CAL))
            uso = cpu.porcentaje(a, cpu.instantanea(pids), time.time() - t)
            f = {"cliente_compartido": int(comp), "rep": rep + 1}
            f.update(resumen(regs, DUR))
            f["cpu_gateway_pct"] = uso.get("gateway", 0)
            filas.append(f)
            print("E6", f, flush=True)
    guardar_csv(str(RES / "e6_cliente_http.csv"), filas)
    claves = ["rps", "p50_ms", "p95_ms", "p99_ms", "errores_pct", "cpu_gateway_pct"]
    return {("compartido" if c else "por_peticion"): _mediana([f for f in filas if f["cliente_compartido"] == c], claves)
            for c in (0, 1)}


# --------------------------------------------------------------------------- E3
def e3_caida_nodo(reps: int) -> Dict[str, Any]:
    """triaje-2 muere (SIGKILL) en t=10 s y se relanza en t=20 s; 20 usuarios; 30 s."""
    dur = 30.0
    salida = {}
    for res in ("0", "1"):
        filas, tiempos = [], []
        for rep in range(reps):
            rl.down()
            rl.up(BROKER, 2, con_worker=False, con_monitor=False,
                  por_nodo={"triaje": {"HILOS_POR_NODO": "8"},
                            "gateway": {"RESILIENCIA": res, "CB_SEGUNDOS_RECUPERACION": "5"}})
            eventos = {10.0: lambda: rl.stop("triaje-2", kill=True), 20.0: lambda: rl.start("triaje-2")}
            regs = _loop().run_until_complete(correr(GW + "/api/triaje", 20, dur, CAL, eventos=eventos))
            ventana = [r for r in regs if 10 <= r.t < 20]
            f = {"resiliencia": int(res), "rep": rep + 1}
            f.update(resumen(regs, dur))
            f["errores_ventana_pct"] = round(100.0 * sum(1 for r in ventana if r.status >= 400) / max(1, len(ventana)), 2)
            f["p95_ventana_ms"] = round(percentil([r.lat_ms for r in ventana if r.status < 400], 95), 1)
            vuelta = [r.t for r in regs if r.t >= 20 and r.nodo == "triaje-2"]
            f["recuperacion_s"] = round(min(vuelta) - 20, 2) if vuelta else float("nan")
            filas.append(f)
            print("E3", f, flush=True)
            seg = por_segundo(regs, dur)
            for s in seg:
                s.update({"resiliencia": int(res), "rep": rep + 1})
            tiempos.extend(seg)
        guardar_csv(str(RES / "e3_caida_{}.csv".format("con" if res == "1" else "sin")), tiempos)
        guardar_csv(str(RES / "e3_resumen_{}.csv".format("con" if res == "1" else "sin")), filas)
        salida["con_resiliencia" if res == "1" else "sin_resiliencia"] = _mediana(
            filas, ["rps", "errores_pct", "errores_ventana_pct", "p95_ms", "p95_ventana_ms", "recuperacion_s"])
    return salida


# --------------------------------------------------------------------------- E3b
def e3b_proveedor_ia(reps: int) -> Dict[str, Any]:
    """Proveedor de IA caido (cada llamada agota un timeout de 1 s): con y sin el Circuit Breaker
    del nodo triaje. La carga va DIRECTO al nodo triaje-1 para aislar su breaker (a traves del
    gateway, el breaker por replica del gateway tambien actua y enmascara el efecto)."""
    filas = []
    for cb in ("0", "1"):
        for rep in range(reps):
            rl.down()
            rl.up(BROKER, 1, con_worker=False, con_monitor=False,
                  por_nodo={"triaje": {"HILOS_POR_NODO": "8", "LLM_TASA_FALLO": "1", "LLM_TIMEOUT_MS": "1000",
                                       "CB_HABILITADO": cb, "CB_SEGUNDOS_RECUPERACION": "30"}})
            regs = _loop().run_until_complete(correr("http://127.0.0.1:8101/triaje", 20, DUR, CAL))
            f = {"circuit_breaker": int(cb), "rep": rep + 1}
            f.update(resumen(regs, DUR))
            filas.append(f)
            print("E3b", f, flush=True)
    guardar_csv(str(RES / "e3b_proveedor_ia.csv"), filas)
    claves = ["rps", "p50_ms", "p95_ms", "p99_ms", "errores_pct"]
    return {("con_cb" if c else "sin_cb"): _mediana([f for f in filas if f["circuit_breaker"] == c], claves) for c in (0, 1)}


# --------------------------------------------------------------------------- E4
async def _rafaga_citas(n: int) -> float:
    from bench.carga import _ConexionHTTP
    cuerpo = json.dumps({"paciente_id": "PX", "sede_id": "S1", "especialidad": "Medicina general",
                         "prioridad": "urgente"}).encode()
    conexiones = [_ConexionHTTP("127.0.0.1", 8201) for _ in range(n)]
    t0 = time.perf_counter()
    await asyncio.gather(*[c.post("/citas", cuerpo, 10) for c in conexiones])
    dt = time.perf_counter() - t0
    for c in conexiones:
        c.cerrar()
    return dt


def e4_carrera_agenda(reps: int) -> Dict[str, Any]:
    """50 solicitudes simultaneas a la misma sede y especialidad (49 franjas libres)."""
    filas = []
    for lat in ("0", "5"):
        rl.down()
        rl.up(BROKER, 1, con_worker=False, con_monitor=False,
              por_nodo={"agenda": {"AGENDA_LATENCIA_ESCRITURA_MS": lat, "AGENDA_DIAS": "7"}})
        for lock in (False, True):
            _post("http://127.0.0.1:8201/admin/lock", {"activo": lock})
            for rep in range(max(reps, 5)):
                _post("http://127.0.0.1:8201/admin/reiniciar", {})
                dt = _loop().run_until_complete(_rafaga_citas(50))
                r = _get("http://127.0.0.1:8201/resumen")
                f = {"latencia_escritura_ms": int(lat), "lock": int(lock), "rep": rep + 1,
                     "citas": r["citas"], "dobles_asignaciones": r["dobles_asignaciones"],
                     "duracion_rafaga_ms": round(dt * 1000, 1)}
                filas.append(f)
                print("E4", f, flush=True)
    guardar_csv(str(RES / "e4_carrera_agenda.csv"), filas)
    salida = {}
    for lat in (0, 5):
        for lock in (0, 1):
            fs = [f for f in filas if f["latencia_escritura_ms"] == lat and f["lock"] == lock]
            salida["lat{}_lock{}".format(lat, lock)] = {
                "dobles_mediana": statistics.median([f["dobles_asignaciones"] for f in fs]),
                "dobles_max": max(f["dobles_asignaciones"] for f in fs),
                "dobles_min": min(f["dobles_asignaciones"] for f in fs),
                "duracion_ms_mediana": statistics.median([f["duracion_rafaga_ms"] for f in fs]),
                "repeticiones": len(fs)}
    return salida


# --------------------------------------------------------------------------- E2
def _correr_worker(procesos: int, hilos: int, tipo: str, n: int, datos: dict) -> float:
    b = crear_broker(_url_broker())
    b.reiniciar()
    nombres = []
    for i in range(procesos):
        nombre = "worker-{}".format(i + 1)
        rl.lanzar(nombre, [rl.PY, "-m", "servicios.worker.worker"],
                  {"BROKER_URL": _url_broker(), "WORKER_HILOS": str(hilos)})
        nombres.append(nombre)
    time.sleep(1.5)
    t0 = time.perf_counter()
    for _ in range(n):
        b.encolar(nueva_tarea(tipo, datos))
    while b.hechas() < n:
        time.sleep(0.01)
        if time.perf_counter() - t0 > 120:
            break
    dt = time.perf_counter() - t0
    for nombre in nombres:
        rl.stop(nombre)
    return dt


def e2_pool_hilos(reps: int) -> Dict[str, Any]:
    rl.down()
    if BROKER == "redis":
        rl.lanzar("broker", ["redis-server", "--port", "6379", "--save", "", "--appendonly", "no"], {}, puerto_tcp=6379)
    else:
        rl.lanzar("broker", [rl.PY, "-m", "broker_local.servidor", "--puerto", "50000"], {}, puerto_tcp=50000)
    filas = []
    for w in (1, 2, 4, 8, 16, 32):  # E/S simulada: 200 tareas de 50 ms
        for rep in range(reps):
            dt = _correr_worker(1, w, "simulada", 200, {"ms": 50})
            filas.append({"tipo": "es", "procesos": 1, "hilos": w, "rep": rep + 1, "tareas": 200,
                          "segundos": round(dt, 3), "tareas_s": round(200 / dt, 1)})
            print("E2", filas[-1], flush=True)
    # calibrar la tarea de CPU (~20 ms en 1 hilo)
    t0 = time.perf_counter()
    acc = 0
    for i in range(200000):
        acc = (acc + i * i) % 1000003
    n_cpu = int(200000 * 0.020 / (time.perf_counter() - t0))
    for procesos, hilos in ((1, 1), (1, 4), (1, 8), (2, 1), (2, 4)):
        for rep in range(reps):
            dt = _correr_worker(procesos, hilos, "cpu", 100, {"n": n_cpu})
            filas.append({"tipo": "cpu", "procesos": procesos, "hilos": hilos, "rep": rep + 1, "tareas": 100,
                          "segundos": round(dt, 3), "tareas_s": round(100 / dt, 1)})
            print("E2", filas[-1], flush=True)
    rl.down()
    guardar_csv(str(RES / "e2_pool_hilos.csv"), filas)
    salida = {"n_cpu_iteraciones": n_cpu}
    for f in filas:
        k = "{}_{}p_{}h".format(f["tipo"], f["procesos"], f["hilos"])
        salida.setdefault(k, []).append(f["segundos"])
    return {k: (round(statistics.median(v), 3) if isinstance(v, list) else v) for k, v in salida.items()}


# --------------------------------------------------------------------------- E5
def e5_rafaga_cola(reps: int) -> Dict[str, Any]:
    """1000 tareas de 30 ms de golpe; 2 workers x 8 hilos; worker-1 muere (SIGKILL) en t=0,7 s."""
    global BROKER
    original = BROKER
    salida, serie = {}, []
    for br in ("local", "redis"):
        BROKER = br
        filas = []
        for rep in range(reps):
            rl.down()
            rl.up(br, 1, workers=2, hilos_worker=8, con_monitor=False)
            b = crear_broker(_url_broker())
            t0 = time.perf_counter()
            _post(GW + "/api/tareas", {"tipo": "simulada", "cantidad": 1000, "datos": {"ms": 30}})
            t_encolado = time.perf_counter() - t0
            matado = False
            fin_drenado = None
            while time.perf_counter() - t0 < 25:
                t = time.perf_counter() - t0
                if not matado and t >= 0.7:
                    rl.stop("worker-1", kill=True)
                    matado = True
                hechas, cola = b.hechas(), b.longitud()
                if rep == 0:
                    serie.append({"broker": br, "t_s": round(t, 2), "en_cola": cola, "hechas": hechas})
                if hechas >= 1000:
                    fin_drenado = t
                    break
                if cola == 0 and fin_drenado is None and t > 12:
                    break  # ya no queda nada por procesar (lo perdido no vuelve)
                time.sleep(0.1)
            hechas = b.hechas()
            f = {"broker": br, "rep": rep + 1, "encolado_s": round(t_encolado, 2), "hechas": hechas,
                 "perdidas": 1000 - hechas, "drenado_s": round(fin_drenado, 2) if fin_drenado else float("nan")}
            filas.append(f)
            print("E5", f, flush=True)
        salida[br] = {"perdidas_mediana": statistics.median([f["perdidas"] for f in filas]),
                      "perdidas_por_rep": [f["perdidas"] for f in filas],
                      "drenado_s_mediana": statistics.median([f["drenado_s"] for f in filas if f["drenado_s"] == f["drenado_s"]] or [float("nan")]),
                      "encolado_s_mediana": statistics.median([f["encolado_s"] for f in filas])}
        guardar_csv(str(RES / "e5_rafaga_{}.csv".format(br)), filas)
    guardar_csv(str(RES / "e5_serie.csv"), serie)
    BROKER = original
    rl.down()
    return salida


# --------------------------------------------------------------------------- costo de la red
def costo_red(reps: int) -> Dict[str, Any]:
    """Misma evaluacion de triaje (reglas, sin latencia de IA) en proceso, a 1 salto y a 2 saltos."""
    m, s = MotorReglas(), SolicitudTriage(**CUERPO_TRIAJE)
    filas = []
    for rep in range(reps):
        lat = []
        for _ in range(2000):
            t = time.perf_counter()
            m.evaluar(s)
            lat.append((time.perf_counter() - t) * 1000)
        filas.append({"via": "en_proceso", "rep": rep + 1, "p50_ms": round(percentil(lat, 50), 3), "p95_ms": round(percentil(lat, 95), 3)})
    rl.down()
    rl.up(BROKER, 1, con_worker=False, con_monitor=False, por_nodo={"triaje": {"LLM_LATENCIA_MS": "0"}})
    for via, url in (("1_salto_triaje", "http://127.0.0.1:8101/triaje"), ("2_saltos_gateway", GW + "/api/triaje")):
        for rep in range(reps):
            regs = _loop().run_until_complete(correr(url, 1, 10.0, 1.0))
            r = resumen(regs, 10.0)
            filas.append({"via": via, "rep": rep + 1, "p50_ms": r["p50_ms"], "p95_ms": r["p95_ms"]})
            print("RED", filas[-1], flush=True)
    rl.down()
    guardar_csv(str(RES / "e0_costo_red.csv"), filas)
    return {v: _mediana([f for f in filas if f["via"] == v], ["p50_ms", "p95_ms"])
            for v in ("en_proceso", "1_salto_triaje", "2_saltos_gateway")}


# --------------------------------------------------------------------------- E1c
COMBOS = [("S1", "Medicina general"), ("S2", "Medicina general"), ("S1", "Pediatria"),
          ("S3", "Medicina general"), ("S2", "Medicina interna")]
SINTOMAS = [["fiebre", "tos"], ["dolor de cabeza"], ["dolor abdominal", "vomito"], ["congestion nasal"], ["mareo", "fatiga"]]


def _cuerpo_cita(i: int) -> dict:
    sede, esp = COMBOS[i % len(COMBOS)]
    return {"paciente_id": "PX{}".format(i), "sede_id": sede, "especialidad": esp,
            "sintomas": SINTOMAS[i % len(SINTOMAS)], "edad": 20 + i % 60, "temperatura_c": 37.5}


def e1c_flujo_cita(reps: int) -> Dict[str, Any]:
    """Flujo completo POST /api/citas (triaje + agenda + evento + tarea), 2 replicas, 10 usuarios."""
    filas = []
    for rep in range(reps):
        rl.down()
        rl.up(BROKER, 2, workers=1, hilos_worker=8,
              por_nodo={"triaje": {"HILOS_POR_NODO": "8"}, "agenda": {"AGENDA_DIAS": "120"}})
        regs = _loop().run_until_complete(correr(GW + "/api/citas", 10, 15.0, 2.0, cuerpo=_cuerpo_cita))
        r = _get("http://127.0.0.1:8201/resumen")
        time.sleep(2)
        hechas = crear_broker(_url_broker()).hechas()
        f = {"rep": rep + 1}
        f.update(resumen(regs, 15.0))
        f.update({"citas": r["citas"], "dobles": r["dobles_asignaciones"], "recordatorios_hechos": hechas})
        filas.append(f)
        print("E1c", f, flush=True)
    guardar_csv(str(RES / "e1c_flujo_cita.csv"), filas)
    return _mediana(filas, ["rps", "p50_ms", "p95_ms", "p99_ms", "errores_pct", "citas", "dobles", "recordatorios_hechos"])


EXPERIMENTOS = {"e1c": e1c_flujo_cita, "red": costo_red, "e1": e1_escalado, "e6": e6_cliente_http, "e2": e2_pool_hilos,
                "e3": e3_caida_nodo, "e3b": e3b_proveedor_ia, "e4": e4_carrera_agenda, "e5": e5_rafaga_cola}


def entorno() -> Dict[str, Any]:
    try:
        modelo = [l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")][0]
    except Exception:
        modelo = platform.processor()
    try:
        mem = int([l.split()[1] for l in open("/proc/meminfo") if l.startswith("MemTotal")][0]) // 1024
    except Exception:
        mem = -1
    return {"python": platform.python_version(), "so": platform.platform(), "cpu": modelo,
            "nucleos_logicos": os.cpu_count(), "ram_mb": mem, "broker": BROKER,
            "fecha": time.strftime("%Y-%m-%d %H:%M:%S %z")}


def main() -> None:
    global BROKER
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", choices=["redis", "local"], default="redis")
    ap.add_argument("--reps", type=int, default=REPS)
    ap.add_argument("--solo", nargs="*", default=list(EXPERIMENTOS))
    a = ap.parse_args()
    BROKER = a.broker
    RES.mkdir(parents=True, exist_ok=True)
    ruta = RES / "resumen.json"
    resumen_total = json.loads(ruta.read_text()) if ruta.exists() else {}
    resumen_total["entorno"] = entorno()
    resumen_total["parametros"] = {"duracion_s": DUR, "calentamiento_s": CAL, "repeticiones": a.reps}
    for nombre in a.solo:
        t = time.time()
        try:
            resumen_total[nombre] = EXPERIMENTOS[nombre](a.reps)
        finally:
            rl.down()
        resumen_total[nombre + "_duracion_s"] = round(time.time() - t, 1)
        ruta.write_text(json.dumps(resumen_total, indent=2, ensure_ascii=False).replace(": NaN", ": null"))
        print("==", nombre, json.dumps(resumen_total[nombre], ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
