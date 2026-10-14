FROM apache/airflow:2.10.2

USER root

# 1. Java 17 install karna (PySpark ke liye OpenJDK mandatory hai)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Java environment variables set karna
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# 3. requirements.txt copy karke saari libraries install karna
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt