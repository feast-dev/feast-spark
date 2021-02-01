FROM maven:3.6-jdk-11 as builder

RUN apt-get update && apt-get install -y build-essential
WORKDIR /build

COPY . .
ARG VERSION=dev

RUN REVISION=$VERSION make build-ingestion-jar-no-tests

FROM gcr.io/kf-feast/spark-py:v3.0.1 as runtime

ARG VERSION=dev

ARG HADOOP_AWS_VERSION=3.2.1
ARG AWS_JAVA_SDK_VERSION=1.11.874
ARG TFRECORD_VERSION=0.3.0
ARG GCS_CONNECTOR_VERSION=2.0.1
ARG BQ_CONNECTOR_VERSION=0.18.1

USER root
# Add HADOOP_AWS_JAR and AWS_JAVA_SDK
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/linkedin/sparktfrecord/spark-tfrecord_2.12/${TFRECORD_VERSION}/spark-tfrecord_2.12-${TFRECORD_VERSION}.jar /opt/spark/jars
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-${GCS_CONNECTOR_VERSION}.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BQ_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar /opt/spark/jars

COPY --from=builder /build/spark/ingestion/target/feast-ingestion-spark-${VERSION}.jar /opt/spark/jars


ENTRYPOINT [ "/opt/entrypoint.sh" ]