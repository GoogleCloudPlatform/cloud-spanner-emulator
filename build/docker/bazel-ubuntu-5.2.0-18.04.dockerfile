################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:18.04 as build

# Setup java
RUN apt-get update && apt-get -qq install -y default-jre default-jdk

# Install prerequisites for bazel
RUN apt-get update && apt-get -qq install curl tar build-essential wget        \
    python python3 zip unzip

# Install bazel from source
RUN mkdir -p bazel                                                          && \
    cd bazel                                                                && \
    wget https://github.com/bazelbuild/bazel/releases/download/5.2.0/bazel-5.2.0-dist.zip &&\
    unzip bazel-5.2.0-dist.zip                                              && \
    rm -rf bazel-5.2.0-dist.zip
ENV PATH=$PATH:/usr/bin:/usr/local/bin
ENV EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk"
RUN cd bazel && bash ./compile.sh
RUN cp /bazel/output/bazel  /usr/local/bin
