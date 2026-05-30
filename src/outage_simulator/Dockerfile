FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx aiofiles
COPY src/outage_simulator /app/outage_simulator
ENV PYTHONPATH=/app
CMD ["python", "outage_simulator/simulator.py", "--scenario", "menu"]
