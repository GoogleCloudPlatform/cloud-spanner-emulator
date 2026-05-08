################################################################################
#                                     BUILD                                    #
################################################################################

FROM ubuntu:22.04 as build

# Perform all APT installs in a single layer to reduce image size.
ENV GCC_VERSION=13
ARG DEBIAN_FRONTEND="noninteractive"
ARG TZ="America/Los_Angeles"
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    wget \
    zip \
    unzip \
    git \
    make \
    rename \
    software-properties-common \
    gnupg \
    build-essential \
    ca-certificates \
    libgnutls30 \
    tzdata \
    language-pack-en \
    default-jre \
    default-jdk \
    protobuf-compiler \
    python3 \
    && add-apt-repository ppa:ubuntu-toolchain-r/test -y \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc-${GCC_VERSION} \
       g++-${GCC_VERSION} \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 90 \
       --slave /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION} \
    && apt-get install -y --no-install-recommends --only-upgrade libstdc++6

# Create a symlink so 'python' points to 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Bazelisk
ENV USE_BAZEL_VERSION=7.6.1
ARG TARGETARCH
RUN wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.17.0/bazelisk-linux-${TARGETARCH} \
    && chmod +x /usr/local/bin/bazel
ENV EXTRA_BAZEL_ARGS="--tool_java_runtime_version=local_jdk"

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
