"""Graficos de los experimentos (matplotlib, 200 dpi, legibles en escala de grises).

  python -m bench.graficos --salida docs/graficos
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RES = Path(__file__).resolve().parent / "resultados"
AZUL, NARANJA, GRIS, TINTA = "#2a78d6", "#eb6834", "#8a8a85", "#0b0b0b"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": "#52514e", "axes.labelcolor": TINTA,
                     "xtick.color": "#52514e", "ytick.color": "#52514e", "axes.grid": True,
                     "grid.color": "#e4e3df", "grid.linewidth": 0.8, "axes.axisbelow": True,
                     "legend.frameon": False, "savefig.dpi": 200, "savefig.bbox": "tight"})


def leer(nombre):
    with open(RES / nombre, encoding="utf-8") as f:
        return [{k: _num(v) for k, v in fila.items()} for fila in csv.DictReader(f)]


def _num(v):
    try:
        return float(v)
    except ValueError:
        return v


def med(filas, clave, **filtro):
    vals = [f[clave] for f in filas if all(f[k] == v for k, v in filtro.items())]
    return statistics.median(vals)


def etiqueta_barras(ax, barras, fmt="{:.0f}", dy=0):
    for b in barras:
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 3 + dy), textcoords="offset points", ha="center", fontsize=10, color=TINTA)


def g_red(salida):
    r = json.loads((RES / "resumen.json").read_text())["red"]
    vias = [("en_proceso", "En proceso\n(Act. 2)"), ("1_salto_triaje", "1 salto\n(directo a triaje)"),
            ("2_saltos_gateway", "2 saltos\n(gateway → triaje)")]
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    x = range(len(vias))
    b1 = ax.bar([i - 0.18 for i in x], [r[v]["p50_ms"] for v, _ in vias], 0.34, color=AZUL, label="p50")
    b2 = ax.bar([i + 0.18 for i in x], [r[v]["p95_ms"] for v, _ in vias], 0.34, color=NARANJA, hatch="//",
                edgecolor="white", label="p95")
    ax.set_yscale("log")
    ax.set_ylim(0.001, 20)
    ax.set_xticks(list(x))
    ax.set_xticklabels([e for _, e in vias])
    ax.set_ylabel("Latencia (ms, escala log)")
    etiqueta_barras(ax, b1, "{:.3g}")
    etiqueta_barras(ax, b2, "{:.3g}")
    ax.legend(loc="upper left")
    ax.set_title("Misma evaluación de triaje (reglas), 1 usuario", fontsize=11, loc="left")
    fig.savefig(salida / "g0-costo-red.png")
    plt.close(fig)


def g_e1(salida):
    f = leer("e1_escalado.csv")
    n = [1, 2, 3, 4]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    rps = [med(f, "rps", replicas=k) for k in n]
    b = a1.bar(n, rps, 0.6, color=AZUL, label="Medido")
    a1.plot(n, [rps[0] * k for k in n], ls="--", color=GRIS, marker="o", ms=5, label="Lineal ideal (n × 1 réplica)")
    for bar in b:
        a1.annotate("{:.0f}".format(bar.get_height()), (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, -16), textcoords="offset points", ha="center", fontsize=10, color="white", fontweight="bold")
    a1.set_xticks(n)
    a1.set_xlabel("Réplicas de triaje")
    a1.set_ylabel("Throughput (req/s)")
    a1.set_title("(a) Throughput", loc="left", fontsize=11)
    a1.legend(loc="upper left")
    p50 = [med(f, "p50_ms", replicas=k) for k in n]
    p95 = [med(f, "p95_ms", replicas=k) for k in n]
    a2.plot(n, p50, marker="o", color=AZUL, lw=2, label="p50")
    a2.plot(n, p95, marker="s", color=NARANJA, lw=2, ls="--", label="p95")
    for xs, ys in ((n, p50), (n, p95)):
        for x_, y_ in zip(xs, ys):
            a2.annotate("{:.0f}".format(y_), (x_, y_), xytext=(6, 2), textcoords="offset points", fontsize=9)
    a2.set_xticks(n)
    a2.set_ylim(0, max(p95) * 1.15)
    a2.set_xlabel("Réplicas de triaje")
    a2.set_ylabel("Latencia (ms)")
    a2.set_title("(b) Latencia", loc="left", fontsize=11)
    a2.legend()
    fig.tight_layout()
    fig.savefig(salida / "g1-escalado-replicas.png")
    plt.close(fig)
    # saturacion: CPU del gateway
    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    cg = [med(f, "cpu_gateway_pct", replicas=k) for k in n]
    ct = [med(f, "cpu_triaje_prom_pct", replicas=k) for k in n]
    ax.plot(n, cg, marker="o", color=AZUL, lw=2, label="gateway")
    ax.plot(n, ct, marker="s", color=NARANJA, lw=2, ls="--", label="cada réplica de triaje (promedio)")
    ax.axhline(100, color=GRIS, lw=1, ls=":")
    ax.annotate("100 % = un núcleo (techo de un proceso Python)", (1, 100), xytext=(0, 4), textcoords="offset points", fontsize=9, color="#52514e")
    for x_, y_ in zip(n, cg):
        ax.annotate("{:.0f} %".format(y_), (x_, y_), xytext=(6, -12), textcoords="offset points", fontsize=9)
    ax.set_ylim(0, 115)
    ax.set_xticks(n)
    ax.set_xlabel("Réplicas de triaje")
    ax.set_ylabel("CPU del proceso (%)")
    ax.legend(loc="center right", bbox_to_anchor=(1.0, 0.36))
    fig.savefig(salida / "g1b-saturacion-cpu.png")
    plt.close(fig)


def g_e2(salida):
    f = leer("e2_pool_hilos.csv")
    hilos = [1, 2, 4, 8, 16, 32]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8), gridspec_kw={"width_ratios": [1.3, 1]})
    seg = [med(f, "segundos", tipo="es", hilos=h, procesos=1) for h in hilos]
    a1.plot(hilos, seg, marker="o", color=AZUL, lw=2, label="Medido")
    a1.plot(hilos, [200 * 0.05 / h for h in hilos], ls="--", color=GRIS, label="Ideal (200 × 50 ms / hilos)")
    for x_, y_ in zip(hilos, seg):
        a1.annotate("{:.2f} s".format(y_), (x_, y_), xytext=(4, 5), textcoords="offset points", fontsize=9)
    a1.set_xscale("log", base=2)
    a1.set_yscale("log")
    a1.set_xticks(hilos)
    a1.set_xticklabels([str(h) for h in hilos])
    a1.set_xlabel("Hilos del worker (max_workers)")
    a1.set_ylabel("Tiempo para 200 tareas (s, log)")
    a1.set_title("(a) Tareas de E/S (50 ms)", loc="left", fontsize=11)
    a1.legend()
    confs = [(1, 1), (1, 4), (1, 8), (2, 1), (2, 4)]
    vals = [med(f, "segundos", tipo="cpu", procesos=p, hilos=h) for p, h in confs]
    colores = [AZUL if p == 1 else NARANJA for p, _ in confs]
    b = a2.bar(range(len(confs)), vals, 0.6, color=colores,
               hatch=None, edgecolor="white")
    for bar, (p, _) in zip(b, confs):
        if p == 2:
            bar.set_hatch("//")
    etiqueta_barras(a2, b, "{:.2f}")
    a2.set_xticks(range(len(confs)))
    a2.set_xticklabels(["{}p×{}h".format(p, h) for p, h in confs])
    a2.set_xlabel("Procesos × hilos")
    a2.set_ylabel("Tiempo para 100 tareas (s)")
    a2.set_title("(b) Tareas de CPU (~20 ms)", loc="left", fontsize=11)
    from matplotlib.patches import Patch
    a2.legend(handles=[Patch(color=AZUL, label="1 proceso"), Patch(facecolor=NARANJA, hatch="//", edgecolor="white", label="2 procesos")])
    fig.tight_layout()
    fig.savefig(salida / "g2-pool-hilos.png")
    plt.close(fig)


def g_e3(salida):
    fig, ax = plt.subplots(figsize=(9, 3.6))
    for nombre, color, ls, mk, etq in (("e3_caida_sin.csv", NARANJA, "--", "s", "Sin Retry ni Circuit Breaker"),
                                       ("e3_caida_con.csv", AZUL, "-", "o", "Con Retry + Circuit Breaker por réplica")):
        f = leer(nombre)
        por_seg = defaultdict(list)
        for r in f:
            por_seg[int(r["segundo"])].append(r["errores_pct"])
        xs = sorted(por_seg)
        ax.plot(xs, [statistics.median(por_seg[s]) for s in xs], color=color, ls=ls, marker=mk, ms=4, lw=2, label=etq)
    ax.axvspan(10, 20, color="#e4e3df", alpha=0.7, lw=0)
    ax.annotate("triaje-2 caído\n(SIGKILL en t = 10 s,\nrelanzado en t = 20 s)", (15, 28), ha="center", fontsize=9, color="#52514e")
    ax.set_xlim(0, 30)
    ax.set_ylim(-2, 85)
    ax.set_xlabel("Tiempo (s)")
    ax.set_ylabel("Peticiones con error (%)")
    ax.legend(loc="upper right")
    fig.savefig(salida / "g3-caida-nodo.png")
    plt.close(fig)


def g_e3b_e4(salida):
    f = leer("e3b_proveedor_ia.csv")
    e4 = json.loads((RES / "resumen.json").read_text())["e4"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    x = [0, 1]
    p50 = [med(f, "p50_ms", circuit_breaker=c) for c in (0, 1)]
    p95 = [med(f, "p95_ms", circuit_breaker=c) for c in (0, 1)]
    b1 = a1.bar([i - 0.18 for i in x], p50, 0.34, color=AZUL, label="p50")
    b2 = a1.bar([i + 0.18 for i in x], p95, 0.34, color=NARANJA, hatch="//", edgecolor="white", label="p95")
    etiqueta_barras(a1, b1)
    etiqueta_barras(a1, b2)
    a1.set_xticks(x)
    a1.set_xticklabels(["Sin Circuit Breaker", "Con Circuit Breaker"])
    a1.set_ylabel("Latencia (ms)")
    a1.set_title("(a) E3b: proveedor de IA caído", loc="left", fontsize=11)
    a1.legend()
    a1.set_ylim(0, max(p95) * 1.18)
    cats = [("lat0_lock0", "0 ms\nsin lock"), ("lat0_lock1", "0 ms\ncon lock"),
            ("lat5_lock0", "5 ms\nsin lock"), ("lat5_lock1", "5 ms\ncon lock")]
    vals = [e4[k]["dobles_mediana"] for k, _ in cats]
    b = a2.bar(range(4), vals, 0.6, color=[NARANJA if "lock0" in k else AZUL for k, _ in cats], edgecolor="white")
    for bar, (k, _) in zip(b, cats):
        if "lock0" in k:
            bar.set_hatch("//")
    etiqueta_barras(a2, b)
    a2.set_xticks(range(4))
    a2.set_xticklabels([e for _, e in cats])
    a2.set_xlabel("Latencia de escritura simulada · sincronización")
    a2.set_ylabel("Asignaciones duplicadas")
    a2.set_title("(b) E4: 50 citas simultáneas", loc="left", fontsize=11)
    a2.set_ylim(0, 55)
    fig.tight_layout()
    fig.savefig(salida / "g4-proveedor-ia-y-carrera.png")
    plt.close(fig)


def g_e5(salida):
    f = leer("e5_serie.csv")
    fig, ax = plt.subplots(figsize=(9, 3.6))
    for br, color, ls, etq in (("local", NARANJA, "--", "broker_local (at-most-once)"),
                               ("redis", AZUL, "-", "Redis Streams + XACK (at-least-once)")):
        fs = [r for r in f if r["broker"] == br]
        ax.plot([r["t_s"] for r in fs], [r["hechas"] for r in fs], color=color, ls=ls, lw=2, label=etq)
    ax.axvline(0.7, color=GRIS, ls=":", lw=1.2)
    ax.annotate("worker-1 muere\n(SIGKILL, t = 0,7 s)", (0.75, 150), fontsize=9, color="#52514e")
    ax.axhline(1000, color=GRIS, lw=0.8)
    ax.set_xlabel("Tiempo desde la ráfaga (s)")
    ax.set_ylabel("Tareas completadas")
    ax.set_ylim(0, 1080)
    ax.legend(loc="lower right")
    fig.savefig(salida / "g5-rafaga-cola.png")
    plt.close(fig)


def g_e6(salida):
    f = leer("e6_cliente_http.csv")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    etiquetas = ["Cliente por petición\n(versión inicial)", "Pool compartido\n(ajuste)"]
    rps = [med(f, "rps", cliente_compartido=c) for c in (0, 1)]
    cpu = [med(f, "cpu_gateway_pct", cliente_compartido=c) for c in (0, 1)]
    for ax, vals, tit, yl, fmt in ((a1, rps, "(a) Throughput", "req/s", "{:.0f}"),
                                   (a2, cpu, "(b) CPU del gateway", "% de un núcleo", "{:.0f} %")):
        b = ax.bar([0, 1], vals, 0.55, color=[NARANJA, AZUL], edgecolor="white")
        b[0].set_hatch("//")
        etiqueta_barras(ax, b, fmt)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(etiquetas)
        ax.set_ylabel(yl)
        ax.set_title(tit, loc="left", fontsize=11)
        ax.set_ylim(0, max(vals) * 1.18)
    fig.tight_layout()
    fig.savefig(salida / "g6-cliente-http.png")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default=str(Path(__file__).resolve().parents[1] / "docs" / "graficos"))
    a = ap.parse_args()
    salida = Path(a.salida)
    salida.mkdir(parents=True, exist_ok=True)
    for g in (g_red, g_e1, g_e2, g_e3, g_e3b_e4, g_e5, g_e6):
        try:
            g(salida)
            print("ok", g.__name__)
        except (FileNotFoundError, KeyError) as exc:
            print("sin datos para", g.__name__, exc)


if __name__ == "__main__":
    main()
