# TODO: openblas vs intel mkl?
# TODO: manually install cuda libraries and create intermediate build stage for basic spark on ubuntu
# TODO: create user

ARG SPARK_VERSION=3.1.3
ARG HADOOP_VERSION=2.7
ARG JAVA_VERSION=11
ARG USE_NATIVE_LA_LIBS
ARG SPARK_BUILD=${USE_NATIVE_LA_LIBS:+build}

FROM alpine:latest AS spark-release
ARG SPARK_VERSION
ARG HADOOP_VERSION
RUN apk --no-cache add curl \
  && curl "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /spark.tgz


FROM maven:3.9.3-eclipse-temurin-8 AS spark-build

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG BUILD_NAME=custom-spark
ARG SPARK_SOURCE_HOME=/spark

ENV LANG C.UTF-8

RUN mkdir -p $SPARK_SOURCE_HOME \
  && curl "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}.tgz" -o /tmp/spark.tgz \
  && tar -xzvf /tmp/spark.tgz --strip-components=1 -C $SPARK_SOURCE_HOME \
  && rm -f /tmp/spark.tgz

WORKDIR $SPARK_SOURCE_HOME

RUN ./dev/make-distribution.sh --name ${BUILD_NAME} --tgz -Pnetlib-lgpl -DskipTests -Pyarn -Phadoop-${HADOOP_VERSION} \
  && mv spark-${SPARK_VERSION}-bin-${BUILD_NAME}.tgz /spark.tgz


FROM spark-${SPARK_BUILD:-release} AS source
FROM eclipse-temurin:${JAVA_VERSION}-jdk-jammy AS spark-dev

ENV LANG C.UTF-8
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin
ENV SPARK_MASTER_PORT 7077
ENV PYTHONHASHSEED 1

COPY --from=source /spark.tgz /tmp/spark.tgz

RUN mkdir -p $SPARK_HOME \
  && tar -xzvf /tmp/spark.tgz --strip-components=1 -C $SPARK_HOME \
  && rm -f /tmp/spark.tgz

RUN apt-get update \
  && apt-get install -y --no-install-recommends libgomp1 \
  && rm -rf /var/lib/apt/lists/*

COPY install_native_la_libs.sh /

RUN chmod +x /install_native_la_libs.sh \
  && /install_native_la_libs.sh \
  && rm -f /install_native_la_libs.sh

WORKDIR $SPARK_HOME
