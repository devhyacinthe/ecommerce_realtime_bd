FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --break-system-packages \
    pyspark==3.5.3 clickhouse-connect==0.7.19
ENV PYSPARK_PYTHON=python3
WORKDIR /app
COPY jobs/ /app/jobs/