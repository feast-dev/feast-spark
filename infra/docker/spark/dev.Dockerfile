FROM gcr.io/kf-feast/feast-spark-base:v3.0.3 as runtime

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

# Fix arrow issue for jdk-11
RUN mkdir -p /opt/spark/conf
RUN echo 'spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"' >> $SPARK_HOME/conf/spark-defaults.conf
RUN echo 'spark.driver.extraJavaOptions="-Dcom.google.cloud.spark.bigquery.repackaged.io.netty.tryReflectionSetAccessible=true"' >> $SPARK_HOME/conf/spark-defaults.conf
RUN echo 'spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"' >> $SPARK_HOME/conf/spark-defaults.conf
RUN echo 'spark.executor.extraJavaOptions="-Dcom.google.cloud.spark.bigquery.repackaged.io.netty.tryReflectionSetAccessible=true"' >> $SPARK_HOME/conf/spark-defaults.conf \

# python dependencies
RUN pip3 install pandas==1.3.5 great-expectations==0.13.2 pyarrow==2.0.0 Jinja2==3.0.3 datadog==0.44.0 'numpy<1.20.0'

# For logging to /dev/termination-log
RUN mkdir -p /dev

COPY spark/ingestion/target/feast-ingestion-spark-${VERSION}.jar /opt/spark/jars


ENTRYPOINT [ "/opt/entrypoint.sh" ]