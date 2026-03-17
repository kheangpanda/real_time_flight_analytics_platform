FROM python:3.11-slim

# Java is required for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download PostgreSQL JDBC driver into PySpark jars directory
ENV PYSPARK_VER=3.5.0
RUN PYSPARK_JARS_DIR=$(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar" \
    -o "${PYSPARK_JARS_DIR}/postgresql-42.7.1.jar"

COPY streaming_job.py .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "streaming_job.py"]
