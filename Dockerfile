FROM nedbank-de-challenge/base:1.0

# Fix missing procps (suppresses ps warning)
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Bake Delta jars for --network=none compatibility
RUN JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars && \
    curl -L -o $JARS_DIR/delta-spark_2.12-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o $JARS_DIR/delta-storage-3.1.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o $JARS_DIR/antlr4-runtime-4.9.3.jar \
        https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

# Only copy pipeline code — config comes from volume mount at /data/config
COPY pipeline/ /app/pipeline/
COPY config/   /data/config/

ENV PYTHONPATH="/app"
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_LOCAL_DIRS=/tmp
ENV JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"
