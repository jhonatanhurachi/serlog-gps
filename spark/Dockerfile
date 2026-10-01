# Imagen local para ejecutar spark/reportes_operativos.py (Java 17 + PySpark 3.5).
# El driver JDBC se descarga dentro de la imagen: nunca se versiona en el repo.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ARG PG_JDBC_VERSION=42.7.4
ENV JDBC_JAR=/opt/jars/postgresql-${PG_JDBC_VERSION}.jar
RUN mkdir -p /opt/jars \
 && curl -fsSL -o "${JDBC_JAR}" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"

ENV PYTHONUNBUFFERED=1
WORKDIR /app
ENTRYPOINT ["python", "spark/reportes_operativos.py"]
