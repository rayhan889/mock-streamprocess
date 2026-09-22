FROM apache/spark:4.0.0


ARG SPARK_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ARG KAFKA_CLIENTS_VERSION=3.9.0
ARG COMMONS_POOL2_VERSION=2.12.0

USER root

RUN curl -fL -o "$SPARK_HOME/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -fL -o "$SPARK_HOME/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -fL -o "$SPARK_HOME/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
 && curl -fL -o "$SPARK_HOME/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark

ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark-sentiment-service
COPY schema.py cleaning.py sentiment.py job.py ./
