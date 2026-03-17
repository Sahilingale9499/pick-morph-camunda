FROM python:3.12-alpine

# docker CLI needed to run docker compose commands
RUN apk add --no-cache docker-cli docker-cli-compose

# Python Docker SDK
RUN pip install --no-cache-dir docker

COPY autoscaler.py /app/autoscaler.py

CMD ["python", "/app/autoscaler.py"]
