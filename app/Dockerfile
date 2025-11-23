FROM python:3.11-slim

WORKDIR /app

# Copy everything (including backend/)
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8080

CMD ["bash", "-c", "uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8080}"]