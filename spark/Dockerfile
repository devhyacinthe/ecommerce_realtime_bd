FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# venv isolé : plus besoin de --break-system-packages
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir pyspark==3.5.3 clickhouse-connect==0.7.19

ENV PYSPARK_PYTHON=/opt/venv/bin/python
WORKDIR /app
COPY jobs/ /app/jobs/