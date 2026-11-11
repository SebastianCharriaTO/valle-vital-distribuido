# Una sola imagen para todos los nodos; cada servicio de Compose cambia el comando.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY servicios ./servicios
COPY broker_local ./broker_local
COPY data ./data
RUN useradd --create-home vv
USER vv
EXPOSE 8000
CMD ["uvicorn", "servicios.gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
