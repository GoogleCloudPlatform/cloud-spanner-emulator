################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:18.04 as build

# Install prerequisites for bazel
RUN apt-get update && apt-get -qq install curl tar build-essential wget        \
    python python3.8 zip unzip

# Setup java
RUN apt-get update && apt-get -qq install -y default-jre default-jdk

# Install bazel
ARG TARGETARCH
RUN wget https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-linux-${TARGETARCH}
RUN cp bazelisk-linux-${TARGETARCH} /usr/local/bin/bazel
RUN chmod u+x /usr/local/bin/bazel
ENV PATH=$PATH:/usr/bin:/usr/local/bin
ENV EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk"

RUN apt-get update && DEBIAN_FRONTEND="noninteractive"                         \
    TZ="America/Los_Angeles" apt-get install -y tzdata

# Unfortunately ZetaSQL has issues with clang (default bazel compiler), so
# we install GCC. Also install make for rules_foreign_cc bazel rules.
ENV GCC_VERSION=11
RUN apt-get -qq update                                                      && \
    apt-get -qq install -y software-properties-common make rename  git
RUN add-apt-repository ppa:ubuntu-toolchain-r/test                          && \
    apt-get -qq update                                                      && \
    apt-get -qq install -y gcc-${GCC_VERSION} g++-${GCC_VERSION}            && \
    apt-get -qq install -y ca-certificates libgnutls30                      && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 90  \
                        --slave   /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION} && \
    update-alternatives --set gcc /usr/bin/gcc-${GCC_VERSION}

# Install the en* locales required for PG scalar functions.
RUN apt-get -qq install language-pack-en

ENV BAZEL_CXXOPTS="-std=c++17"

# Install google-cloud-sdk to get gcloud.
RUN curl https://sdk.cloud.google.com > install.sh                          && \
    bash install.sh --disable-prompts                                       && \
    ln -s /root/google-cloud-sdk/bin/gcloud /usr/bin/gcloud                 && \
    ln -s /root/google-cloud-sdk/bin/gsutil /usr/bin/gsutil

ENV GCLOUD_DIR="/usr/bin"

# Configure gcloud to use emulator locally.
ENV SPANNER_EMULATOR_HOST=localhost:9010
RUN gcloud config configurations create emulator                            && \
    gcloud config set auth/disable_credentials true                         && \
    gcloud config set account emulator-account                              && \
    gcloud config set project emulator-project                              && \
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
