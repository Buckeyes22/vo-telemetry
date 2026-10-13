FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY src/liveness/requirements.txt /tmp/liveness-requirements.txt
RUN pip install --no-cache-dir -r /tmp/liveness-requirements.txt

COPY src /app/src

EXPOSE 8723

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8723/healthz', timeout=2).read()"

CMD ["python", "-m", "liveness.server"]
