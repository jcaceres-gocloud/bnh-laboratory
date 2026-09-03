FROM python:3.12-slim

WORKDIR /app

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client/client.py .
COPY client/organizaciones_client.py .
COPY generated /app/generated

ENV PYTHONPATH=/app/generated

CMD ["python", "client.py"]
