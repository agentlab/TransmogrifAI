# TODO: openblas vs intel mkl?

ARG SPARK_VERSION=3.1.3
ARG HADOOP_VERSION=2.7
ARG JAVA_VERSION=11
ARG USE_NATIVE_LA_LIBS
ARG SPARK_BUILD=${USE_NATIVE_LA_LIBS:+build}
ARG SPARK_USERNAME=spark
ARG SPARK_USER_UID=1000
ARG SPARK_USER_GID=${SPARK_USER_UID}
ARG CUDA_VERSION=12.2.0

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

ARG SPARK_USERNAME
ARG SPARK_USER_UID
ARG SPARK_USER_GID

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

RUN groupadd --gid $SPARK_USER_GID $SPARK_USERNAME \
  && useradd --uid $SPARK_USER_UID --gid $SPARK_USER_GID -s /bin/bash -m $SPARK_USERNAME

RUN chown -R $SPARK_USERNAME:$SPARK_USERNAME $SPARK_HOME \
  && chmod -R g+w $SPARK_HOME

WORKDIR $SPARK_HOME

USER $SPARK_USERNAME


# see https://gitlab.com/nvidia/container-images/cuda/-/blob/master/dist/12.2.0/ubuntu2204/base/Dockerfile
FROM spark-dev AS spark-cuda

ARG SPARK_USERNAME

COPY conf/spark-env.gpu.sh $SPARK_HOME/conf/spark-env.sh

USER root

ENV NVARCH x86_64
ENV NV_CUDA_CUDART_VERSION 12.2.53-1
ENV NV_CUDA_COMPAT_PACKAGE cuda-compat-12-2

RUN apt-get update && apt-get install -y --no-install-recommends \
  gnupg2 curl ca-certificates && \
  curl -fsSLO https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/${NVARCH}/cuda-keyring_1.0-1_all.deb && \
  dpkg -i cuda-keyring_1.0-1_all.deb && \
  apt-get purge --autoremove -y curl \
  && rm -rf /var/lib/apt/lists/*

ENV CUDA_VERSION 12.2.0

# For libraries in the cuda-compat-* package: https://docs.nvidia.com/cuda/eula/index.html#attachment-a
RUN apt-get update && apt-get install -y --no-install-recommends \
  cuda-cudart-12-2=${NV_CUDA_CUDART_VERSION} \
  ${NV_CUDA_COMPAT_PACKAGE} \
  && rm -rf /var/lib/apt/lists/*

# Required for nvidia-docker v1
RUN echo "/usr/local/nvidia/lib" >> /etc/ld.so.conf.d/nvidia.conf \
  && echo "/usr/local/nvidia/lib64" >> /etc/ld.so.conf.d/nvidia.conf

ENV PATH /usr/local/nvidia/bin:/usr/local/cuda/bin:${PATH}
ENV LD_LIBRARY_PATH /usr/local/nvidia/lib:/usr/local/nvidia/lib64

# nvidia-container-runtime
ENV NVIDIA_VISIBLE_DEVICES all
ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

ARG RAPIDS_PLUGIN_HOME=/opt/sparkRapidsPlugin

RUN mkdir -p $RAPIDS_PLUGIN_HOME \
  && cd $RAPIDS_PLUGIN_HOME \
  && wget -q --show-progress https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.06.0/rapids-4-spark_2.12-23.06.0.jar \
  && wget -q https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh \
  && chown -R $SPARK_USERNAME:$SPARK_USERNAME $RAPIDS_PLUGIN_HOME \
  && chmod +x $RAPIDS_PLUGIN_HOME/getGpusResources.sh

USER $SPARK_USERNAME
