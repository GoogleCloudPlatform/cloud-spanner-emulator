################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:18.04 as build

# Setup java
RUN apt-get update && apt-get -qq install -y default-jre default-jdk

# Install prerequisites for bazel
RUN apt-get update && apt-get -qq install curl tar build-essential wget        \
    python python3 zip unzip

ENV BAZEL_VERSION=5.4.0

# Install bazel from source
RUN mkdir -p bazel                                                          && \
    cd bazel                                                                && \
    wget https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-dist.zip &&\
    unzip bazel-${BAZEL_VERSION}-dist.zip                                              && \
    rm -rf bazel-${BAZEL_VERSION}-dist.zip
ENV PATH=$PATH:/usr/bin:/usr/local/bin
ENV EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk"
RUN cd bazel && bash ./compile.sh
RUN cp /bazel/output/bazel  /usr/local/bin
