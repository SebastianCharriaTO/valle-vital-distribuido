# Valle Vital distribuido

Prototipo académico de la **Actividad 4 · Tejiendo redes: arquitectura de software entre hilos y nodos**
(Arquitectura de software, Tecnológica del Oriente, 2026-2). Extiende el Asistente Clínico-Logístico
Valle Vital de la Actividad 2 (monolito hexagonal) a una **arquitectura distribuida**: gateway,
servicios por contexto, broker de mensajes y un pool de workers con hilos.

> Entidad y datos **ficticios**. El triaje es una **prioridad orientativa** (no diagnostica) y la
> "IA" es un proveedor **simulado** con latencia y fallas configurables. No hay datos personales reales.

Equipo: Sebastián Charria Muñoz y Esteban Charria Muñoz.
Repositorio: [COMPLETAR: URL repo] · Video: [COMPLETAR: enlace video]

## Nodos

| Nodo | Proceso | Puerto | Concurrencia | Responsabilidad |
|---|---|---|---|---|
| `gateway` | FastAPI + httpx | 8000 | pool AnyIO (`HILOS_POR_NODO`, 40) | Punto único de entrada, `SolicitudCitaMediator`, `TriajeProxy`/`AgendaProxy`, round-robin, Retry, Circuit Breaker por réplica, Bulkhead, `X-Correlation-ID`, `/health/ready` agregado |
| `triaje-1..N` | FastAPI | 8101.. | pool AnyIO | Prioridad sugerida: IA simulada + Circuit Breaker + reglas (fallback). Sin estado → N réplicas |
| `agenda` | FastAPI | 8201 | pool AnyIO + `threading.Lock` | Franjas y citas (único escritor). Publica `cita.asignada` y encola `recordatorio` |
| `broker` | Redis 7 (Plan A) o `broker_local` (Plan B) | 6379 / 50000 | — | Cola `vv:tareas` (Streams + grupo de consumidores) y tópicos `vv.eventos.*` (Pub/Sub) |
| `worker` ×N | Python | — | `ThreadPoolExecutor(W)` | Competing Consumers: recordatorios, sugerencias de farmacia, tareas de prueba |
| `monitor` | FastAPI | 8300 | hilo suscriptor | Observer distribuido: tablero de eventos en `http://localhost:8300/` |

## Plan A · Docker Compose (Docker Desktop + WSL2)

```bash
docker compose up --build -d          # 8 contenedores en la red vv-net
docker compose ps
curl http://localhost:8000/health/ready
# demo de caída de nodo
docker compose stop triaje-2
docker compose start triaje-2
docker compose down
```

La imagen usa `python:3.12-slim`. `docker compose config` se validó, pero **el equipo debe
ejecutar el `up` en su máquina**: en el entorno donde se midió no había acceso a Docker Hub.

## Plan B · procesos locales (sin Docker, Python 3.8+)

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/run_local.py up                    # broker_local (solo biblioteca estándar)
python scripts/run_local.py up --broker redis     # si hay redis-server instalado
python scripts/run_local.py status
python scripts/run_local.py stop triaje-2         # caída de un nodo (SIGTERM)
python scripts/run_local.py stop worker-1 --kill  # caída abrupta (SIGKILL)
python scripts/run_local.py start triaje-2
python scripts/run_local.py down
```

Logs por nodo en `.run/logs/<nodo>.log` (cada línea lleva `nodo=`, `hilo=` y `cid=`).

## Probar el flujo

```bash
curl -X POST http://localhost:8000/api/citas -H "Content-Type: application/json" \
  -d '{"paciente_id":"PX1","sede_id":"S1","especialidad":"Medicina general","sintomas":["fiebre","tos"],"edad":70,"temperatura_c":39.2}'
curl -X POST http://localhost:8000/api/tareas -H "Content-Type: application/json" -d '{"tipo":"farmacia"}'
curl http://localhost:8000/api/estado            # breakers, bulkheads, cola
curl -X POST http://localhost:8101/admin/proveedor-ia -H "Content-Type: application/json" -d '{"tasa_fallo":1}'
```

Swagger: `http://localhost:8000/docs`. Monitor: `http://localhost:8300/`.

## Pruebas

```bash
python -m pytest -q        # 43 pruebas; pasan en Python 3.8.20 y 3.12.3
```

## Experimentos de escalabilidad y robustez

```bash
python -m bench.experimentos --broker redis            # E1-E6 completos (~20 min)
python -m bench.experimentos --solo e4 --reps 1        # uno solo
```

Resultados en `bench/resultados/` (CSV crudos y `resumen.json` con el entorno de medición).

| ID | Escenario | Qué mide |
|---|---|---|
| red | Misma evaluación en proceso, a 1 salto y a 2 saltos | Costo de la red |
| E1 | 1→4 réplicas de triaje, 50 usuarios, IA de 100 ms, 8 hilos por nodo | req/s, p50/p95/p99, CPU por nodo |
| E2 | Pool del worker 1→32 hilos (E/S) y hilos vs. procesos (CPU) | tiempo y tareas/s; límite del GIL |
| E3 | `triaje-2` muere en t=10 s y vuelve en t=20 s, sin y con resiliencia | % errores por segundo, recuperación |
| E3b | Proveedor de IA caído, sin y con Circuit Breaker | p50/p95 |
| E4 | 50 citas simultáneas, sin y con `Lock` | dobles asignaciones |
| E5 | Ráfaga de 1000 tareas y muerte de un worker, broker local vs. Redis Streams | tareas perdidas |
| E6 | Cliente HTTP por petición vs. pool compartido en el gateway | req/s y CPU del gateway |

### Resultados medidos (Plan B, Python 3.8.20, 2 vCPU, Redis 7.0.15; medianas de 3 repeticiones)

| Experimento | Resultado |
|---|---|
| Costo de la red (p50) | en proceso 0,004 ms · 1 salto 1,0 ms · 2 saltos 3,9 ms |
| E1 réplicas 1→2→3→4 | 78 → 156 → 213 → 226 req/s; el gateway llega al 89 % de un núcleo (cuello de botella) |
| E1c flujo completo (10 usuarios) | 75 req/s; p95 160 ms; 1.279 citas sin franjas duplicadas y 1.279 recordatorios hechos |
| E2 pool de hilos (E/S) | 200 tareas: 10,3 s con 1 hilo → 1,3 s con 8 → 0,38 s con 32 |
| E2 CPU | 1 hilo 2,2 s · 8 hilos 2,7 s · 2 procesos 1,2 s (GIL) |
| E3 caída de triaje-2 | errores en la ventana: 50 % sin resiliencia → 0 % con Retry + Circuit Breaker |
| E3b proveedor de IA caído | sin CB 8 req/s y p95 4,0 s → con CB 747 req/s y p95 36 ms |
| E4 carrera en la agenda (5 ms) | 47 asignaciones duplicadas (citas sobre una franja ya ocupada) sin lock → 0 con lock |
| E5 ráfaga + worker muerto | broker local pierde 8 tareas; Redis Streams + ACK pierde 0 |
| E6 cliente HTTP | por petición 25 req/s → pool compartido 155 req/s |

## Variables de entorno principales

`BROKER_URL` (`redis://…` o `local://…`), `HILOS_POR_NODO`, `TRIAJE_URLS`, `AGENDA_URL`,
`RESILIENCIA` (1/0), `CLIENTE_COMPARTIDO` (1/0), `RETRY_MAX`, `RETRY_BASE_MS`, `TIMEOUT_MS`,
`CB_UMBRAL_FALLOS`, `CB_SEGUNDOS_RECUPERACION`, `BULKHEAD_TRIAJE`, `BULKHEAD_AGENDA`,
`LLM_LATENCIA_MS`, `LLM_TASA_FALLO`, `LLM_TIMEOUT_MS`, `CB_HABILITADO`, `AGENDA_LOCK`,
`AGENDA_LATENCIA_ESCRITURA_MS`, `WORKER_HILOS`, `TAREA_LATENCIA_MS`.

## Estructura

```
common/          dominio (Pydantic), reglas (Strategy), CircuitBreaker seguro entre hilos, BrokerPort + adaptadores
servicios/       gateway/ triaje/ agenda/ worker/ monitor/
broker_local/    broker del Plan B (multiprocessing.managers por TCP)
scripts/         run_local.py (Plan B)
bench/           generador de carga, experimentos E1-E6 y resultados
docs/uml/        diagramas PlantUML (.puml y .png)
tests/           pytest
```

## Compatibilidad

El código corre en **Python 3.8** (WSL sin Docker) y en 3.12 (imagen Docker): `from __future__ import annotations`,
`typing.List/Optional/Dict` en los modelos Pydantic y versiones fijadas en `requirements.txt` que aún soportan 3.8.

## Limitaciones

Simulación local en una sola máquina (las cifras no representan un despliegue real en la nube); estado en
memoria; un solo gateway (punto único de falla); sin autenticación; datos sintéticos.
