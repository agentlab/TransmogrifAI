FROM ubuntu:latest

SHELL ["/bin/bash", "-c"]

ARG SPARK_VERSION=3.1.3
ARG HADOOP_VERSION=2.7

ENV LANG C.UTF-8
ENV SPARK_HOME /opt/spark
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV SPARK_MASTER_PORT 7077
ENV PATH $PATH:$SPARK_HOME/bin
ENV PYTHONHASHSEED 1

RUN apt-get update \
  && apt-get install -y curl wget nano zip unzip sudo libgomp1 libopenblas-base

RUN mkdir -p $SPARK_HOME \
  && curl "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz \
  && tar -xzvf /tmp/spark.tgz --strip-components=1 -C $SPARK_HOME \
  && rm -f /tmp/spark.tgz

ARG USERNAME=user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
  && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME -s /bin/bash \
  && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
  && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME
