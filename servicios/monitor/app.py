"""Nodo MONITOR: suscriptor de eventos (Observer distribuido / Publish-Subscribe).
No conoce a quien publica: solo se suscribe al patron vv.eventos.* del broker."""
from __future__ import annotations

import html
import threading
import time
from collections import Counter, deque
from datetime import datetime
from typing import Any, Deque, Dict, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from common.broker import crear_broker, PREFIJO_EVENTOS
from common.nodo import logger, nombre_nodo

NODO = nombre_nodo("monitor")
log = logger(NODO)
eventos = deque(maxlen=300)  # type: Deque[Dict[str, Any]]
contadores = Counter()  # type: Counter
_lock = threading.Lock()
app = FastAPI(title="Valle Vital - monitor de eventos", version="1.0.0")


def _suscriptor() -> None:
    while True:
        try:
            sub = crear_broker().suscribir(PREFIJO_EVENTOS + "*")
            log.info("suscrito a %s*", PREFIJO_EVENTOS)
            while True:
                ev = sub.recibir(1.0)
                if ev is None:
                    continue
                with _lock:
                    eventos.appendleft(ev)
                    contadores[ev.get("tipo", "?")] += 1
                if ev.get("tipo", "").startswith("nodo."):
                    log.warning("evento %s %s", ev.get("tipo"), ev.get("datos"))
        except Exception as exc:  # broker caido: reintentar
            log.error("suscripcion perdida: %s", exc)
            time.sleep(1.0)


@app.on_event("startup")
def _arranque() -> None:
    threading.Thread(target=_suscriptor, name="suscriptor", daemon=True).start()


@app.get("/eventos")
def listar(n: int = 50) -> List[Dict[str, Any]]:
    with _lock:
        return list(eventos)[:n]


@app.get("/contadores")
def conteo() -> Dict[str, int]:
    with _lock:
        return dict(contadores)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"estado": "ok", "nodo": NODO}


@app.get("/", response_class=HTMLResponse)
def tablero() -> str:
    with _lock:
        evs, cont = list(eventos)[:25], dict(contadores)
    filas = "".join(
        "<tr class='{c}'><td>{h}</td><td>{t}</td><td>{o}</td><td>{cid}</td><td>{d}</td></tr>".format(
            c="alerta" if e.get("tipo", "").startswith("nodo.caido") else "",
            h=datetime.fromtimestamp(e.get("momento", 0)).strftime("%H:%M:%S"),
            t=html.escape(e.get("tipo", "")), o=html.escape(e.get("origen", "")),
            cid=html.escape(str(e.get("cid", ""))), d=html.escape(str(e.get("datos", ""))))
        for e in evs)
    chips = " ".join("<span class='chip'>{}: <b>{}</b></span>".format(html.escape(k), v) for k, v in sorted(cont.items()))
    return """<!doctype html><html lang="es"><head><meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>Monitor Valle Vital</title><style>
body{{font-family:Segoe UI,Arial,sans-serif;margin:24px;font-size:18px;color:#1d2433}}
h1{{font-size:28px;margin:0 0 6px}} .chip{{display:inline-block;background:#e8eef7;border-radius:14px;padding:4px 12px;margin:4px}}
table{{border-collapse:collapse;width:100%;margin-top:14px}} td,th{{border-bottom:1px solid #ccd;padding:6px 8px;text-align:left;font-size:16px}}
th{{background:#1f3a5f;color:#fff}} tr.alerta td{{background:#fde2e1;font-weight:bold}}
</style></head><body><h1>Monitor de eventos &middot; Valle Vital distribuido</h1>
<div>Suscrito a <code>vv.eventos.*</code> (Observer distribuido). Se actualiza cada 2 s. Datos sinteticos.</div>
<div>{chips}</div><table><tr><th>Hora</th><th>Evento</th><th>Origen</th><th>Correlation-ID</th><th>Datos</th></tr>{filas}</table>
</body></html>""".format(chips=chips, filas=filas)
