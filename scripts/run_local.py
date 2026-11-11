"""Plan B: levanta Valle Vital distribuido como PROCESOS locales (sin Docker), cada nodo en
su propio puerto, comunicandose por HTTP/TCP. Funciona con Python 3.8+.

  python scripts/run_local.py up                 # broker local + 2 triaje + agenda + gateway + monitor + 1 worker
  python scripts/run_local.py up --broker redis  # usa un redis-server local (debe estar instalado)
  python scripts/run_local.py status
  python scripts/run_local.py stop triaje-2      # simula la caida de un nodo (SIGTERM)
  python scripts/run_local.py stop worker-1 --kill   # caida abrupta (SIGKILL)
  python scripts/run_local.py start triaje-2     # lo vuelve a levantar
  python scripts/run_local.py down
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

RAIZ = Path(__file__).resolve().parents[1]
RUN = RAIZ / ".run"
ESTADO = RUN / "estado.json"
PY = sys.executable


def _leer() -> Dict[str, dict]:
    return json.loads(ESTADO.read_text()) if ESTADO.exists() else {}


def _guardar(e: Dict[str, dict]) -> None:
    RUN.mkdir(exist_ok=True)
    ESTADO.write_text(json.dumps(e, indent=2))


def _vivo(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:  # un proceso zombi no cuenta como vivo
        estado = Path("/proc/{}/stat".format(pid)).read_text().split()[2]
        return estado != "Z"
    except Exception:
        return True


def _puerto_libre(p: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", p)) != 0


def _esperar_http(url: str, timeout: float = 30.0) -> bool:
    fin = time.time() + timeout
    while time.time() < fin:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.2)
    return False


def _esperar_tcp(p: int, timeout: float = 15.0) -> bool:
    fin = time.time() + timeout
    while time.time() < fin:
        if not _puerto_libre(p):
            return True
        time.sleep(0.1)
    return False


def lanzar(nombre: str, cmd: List[str], env_extra: Dict[str, str], salud: Optional[str] = None,
           puerto_tcp: Optional[int] = None) -> int:
    RUN.mkdir(exist_ok=True)
    (RUN / "logs").mkdir(exist_ok=True)
    env = dict(os.environ)
    env.update(env_extra)
    env["PYTHONPATH"] = str(RAIZ)
    env["NODO_NOMBRE"] = nombre
    env.setdefault("PYTHONUNBUFFERED", "1")
    log = open(RUN / "logs" / (nombre + ".log"), "a")
    p = subprocess.Popen(cmd, cwd=str(RAIZ), env=env, stdout=log, stderr=subprocess.STDOUT)
    e = _leer()
    e[nombre] = {"pid": p.pid, "cmd": cmd, "env": env_extra, "salud": salud, "tcp": puerto_tcp}
    _guardar(e)
    if salud and not _esperar_http(salud):
        print("  ! {} no respondio en {}".format(nombre, salud))
    if puerto_tcp and not _esperar_tcp(puerto_tcp):
        print("  ! {} no abrio el puerto {}".format(nombre, puerto_tcp))
    return p.pid


def uvicorn(modulo: str, puerto: int) -> List[str]:
    return [PY, "-m", "uvicorn", modulo, "--host", "127.0.0.1", "--port", str(puerto),
            "--log-level", "warning", "--no-access-log"]


def up(broker: str = "local", replicas: int = 2, workers: int = 1, hilos_worker: int = 8,
       extra: Optional[Dict[str, str]] = None, con_worker: bool = True, con_monitor: bool = True,
       por_nodo: Optional[Dict[str, Dict[str, str]]] = None) -> None:
    """por_nodo: variables solo para los nodos cuyo nombre empieza por la clave (p. ej. 'triaje')."""
    extra = dict(extra or {})
    por_nodo = por_nodo or {}

    def env_de(nombre: str, base_env: Dict[str, str]) -> Dict[str, str]:
        e = dict(base_env)
        for prefijo, valores in por_nodo.items():
            if nombre.startswith(prefijo):
                e.update(valores)
        return e

    if broker == "redis":
        url = "redis://127.0.0.1:6379/0"
        if _puerto_libre(6379):
            lanzar("broker", ["redis-server", "--port", "6379", "--save", "", "--appendonly", "no"], {}, puerto_tcp=6379)
    else:
        url = "local://127.0.0.1:50000"
        lanzar("broker", [PY, "-m", "broker_local.servidor", "--puerto", "50000"], {}, puerto_tcp=50000)
    base = {"BROKER_URL": url}
    base.update(extra)
    urls = []
    for i in range(1, replicas + 1):
        puerto = 8100 + i
        urls.append("http://127.0.0.1:{}".format(puerto))
        lanzar("triaje-{}".format(i), uvicorn("servicios.triaje.app:app", puerto), env_de("triaje", base),
               "http://127.0.0.1:{}/health".format(puerto))
    lanzar("agenda", uvicorn("servicios.agenda.app:app", 8201), env_de("agenda", base), "http://127.0.0.1:8201/health")
    gw = dict(base)
    gw.update({"TRIAJE_URLS": ",".join(urls), "AGENDA_URL": "http://127.0.0.1:8201"})
    lanzar("gateway", uvicorn("servicios.gateway.app:app", 8000), env_de("gateway", gw), "http://127.0.0.1:8000/health")
    if con_monitor:
        lanzar("monitor", uvicorn("servicios.monitor.app:app", 8300), base, "http://127.0.0.1:8300/health")
    if con_worker:
        for i in range(1, workers + 1):
            w = env_de("worker", base)
            w["WORKER_HILOS"] = str(hilos_worker)
            lanzar("worker-{}".format(i), [PY, "-m", "servicios.worker.worker"], w)
    time.sleep(0.5)


def stop(nombre: str, kill: bool = False) -> None:
    e = _leer()
    if nombre not in e:
        print("no existe el nodo", nombre)
        return
    pid = e[nombre]["pid"]
    if _vivo(pid):
        os.kill(pid, signal.SIGKILL if kill else signal.SIGTERM)
        for _ in range(50):
            if not _vivo(pid):
                break
            time.sleep(0.1)
        try:
            os.waitpid(pid, os.WNOHANG)
        except Exception:
            pass
    print("detenido {} (pid {}{})".format(nombre, pid, ", SIGKILL" if kill else ""))


def start(nombre: str) -> None:
    e = _leer()
    d = e[nombre]
    if _vivo(d["pid"]):
        print(nombre, "ya esta vivo")
        return
    lanzar(nombre, d["cmd"], d["env"], d.get("salud"), d.get("tcp"))
    print("levantado", nombre)


def down() -> None:
    e = _leer()
    for nombre in reversed(list(e)):
        if _vivo(e[nombre]["pid"]):
            stop(nombre)
    _guardar({})


def status() -> None:
    for nombre, d in _leer().items():
        print("{:<10} pid={:<7} {}".format(nombre, d["pid"], "vivo" if _vivo(d["pid"]) else "CAIDO"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="orden", required=True)
    u = sub.add_parser("up")
    u.add_argument("--broker", choices=["local", "redis"], default="local")
    u.add_argument("--replicas", type=int, default=2)
    u.add_argument("--workers", type=int, default=1)
    u.add_argument("--hilos-worker", type=int, default=8)
    u.add_argument("--set", action="append", default=[], metavar="VAR=VALOR",
                   help="variable de entorno para todos los nodos (repetible)")
    s = sub.add_parser("stop")
    s.add_argument("nombre")
    s.add_argument("--kill", action="store_true")
    st = sub.add_parser("start")
    st.add_argument("nombre")
    sub.add_parser("down")
    sub.add_parser("status")
    a = ap.parse_args()
    if a.orden == "up":
        extra = dict(kv.split("=", 1) for kv in a.set)
        up(a.broker, a.replicas, a.workers, a.hilos_worker, extra)
        status()
        print("\nGateway: http://127.0.0.1:8000/docs   Monitor: http://127.0.0.1:8300/")
    elif a.orden == "stop":
        stop(a.nombre, a.kill)
    elif a.orden == "start":
        start(a.nombre)
    elif a.orden == "down":
        down()
    else:
        status()


if __name__ == "__main__":
    main()
