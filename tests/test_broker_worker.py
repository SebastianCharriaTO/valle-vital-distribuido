"""Broker (cola + topicos) y worker con pool de hilos."""
import threading
import time

from common.broker import BrokerLocal, BrokerMemoria, nueva_tarea, nuevo_evento
from servicios.worker.worker import Worker


def test_cola_entrega_cada_tarea_a_un_solo_consumidor():
    b = BrokerMemoria()
    for _ in range(100):
        b.encolar(nueva_tarea("simulada", {"ms": 0}))
    vistas = []
    lock = threading.Lock()

    def consumir(nombre):
        while True:
            m = b.tomar(nombre, 0.05)
            if m is None:
                return
            with lock:
                vistas.append(m[0])

    hilos = [threading.Thread(target=consumir, args=("c{}".format(i),)) for i in range(4)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    assert len(vistas) == 100 and len(set(vistas)) == 100


def test_marcar_hecha_es_idempotente():
    b = BrokerMemoria()
    assert b.marcar_hecha("t1") is True
    assert b.marcar_hecha("t1") is False
    assert b.hechas() == 1


def test_publicar_llega_a_todos_los_suscriptores_del_patron():
    b = BrokerMemoria()
    s1, s2 = b.suscribir("vv.eventos.*"), b.suscribir("vv.eventos.nodo.*")
    b.publicar("vv.eventos.cita.asignada", nuevo_evento("cita.asignada", "agenda"))
    b.publicar("vv.eventos.nodo.caido", nuevo_evento("nodo.caido", "gateway"))
    assert [s1.recibir(0.1)["tipo"], s1.recibir(0.1)["tipo"]] == ["cita.asignada", "nodo.caido"]
    assert s2.recibir(0.1)["tipo"] == "nodo.caido" and s2.recibir(0.05) is None


def test_worker_con_mas_hilos_termina_antes():
    def medir(hilos):
        b = BrokerMemoria()
        for _ in range(16):
            b.encolar(nueva_tarea("simulada", {"ms": 50}))
        w = Worker(b, hilos, 0, "w")
        t = threading.Thread(target=w.ejecutar)
        t0 = time.perf_counter()
        t.start()
        while b.hechas() < 16:
            time.sleep(0.01)
        dt = time.perf_counter() - t0
        w.parar.set()
        t.join()
        return dt

    assert medir(8) < medir(1) / 3


def test_broker_local_por_tcp():
    """El broker del Plan B funciona entre 'nodos' a traves de un socket TCP real."""
    from broker_local.servidor import _Gestor
    gestor = _Gestor(address=("127.0.0.1", 0), authkey=b"prueba")
    servidor = gestor.get_server()
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    host, puerto = servidor.address
    b = BrokerLocal(host, puerto, b"prueba")
    assert b.ping()
    sub = b.suscribir("vv.eventos.*")
    b.encolar(nueva_tarea("simulada", {"ms": 0}))
    b.publicar("vv.eventos.prueba", nuevo_evento("prueba", "test"))
    msg_id, tarea = b.tomar("c1", 1.0)
    assert tarea["tipo"] == "simulada" and b.longitud() == 0
    assert sub.recibir(1.0)["tipo"] == "prueba"
