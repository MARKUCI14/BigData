FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/dashboard /app/dashboard
COPY src/storage /app/storage

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "from urllib.request import urlopen; urlopen('http://localhost:8501/_stcore/health', timeout=5)"

ENTRYPOINT ["streamlit", "run", "dashboard/app.py", \
  "--server.port=8501", \
  "--server.address=0.0.0.0", \
  "--server.headless=true"]
