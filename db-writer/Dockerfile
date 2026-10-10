FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 psycopg2-binary
COPY writer.py .
CMD ["python", "-u", "writer.py"]
