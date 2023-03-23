################################################################################
#                                     BUILD                                    #
################################################################################

FROM gcr.io/cloud-spanner-emulator/bazel-ubuntu:5.4.0-18.04 as build

RUN apt-get update && DEBIAN_FRONTEND="noninteractive"                         \
    TZ="America/Los_Angeles" apt-get install -y tzdata

# Unfortunately ZetaSQL has issues with clang (default bazel compiler), so
# we install GCC. Also install make for rules_foreign_cc bazel rules.
RUN apt-get -qq install -y software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test                          && \
    apt-get -qq update                                                      && \
    apt-get -qq install -y gcc-8 g++-8 make rename  git                     && \
    apt-get -qq install -y ca-certificates libgnutls30                      && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 90           \
                        --slave   /usr/bin/g++ g++ /usr/bin/g++-8           && \
    update-alternatives --set gcc /usr/bin/gcc-8
ENV BAZEL_CXXOPTS="-std=c++17"

ENV CLOUD_SDK_VERSION=416.0.0
# Install google-cloud-sdk to get gcloud.
RUN mkdir -p /usr/local/gcloud                                              && \
    cd /usr/local/gcloud                                                    && \
    curl -s -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar -xf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz       && \
    /usr/local/gcloud/google-cloud-sdk/install.sh                           && \
    ln -s /usr/local/gcloud/google-cloud-sdk/bin/gcloud /usr/bin/gcloud     && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz

# Configure gcloud to use emulator locally.
ENV SPANNER_EMULATOR_HOST=localhost:9010
RUN gcloud config configurations create emulator                            && \
    gcloud config set auth/disable_credentials true                         && \
    gcloud config set account emulator-account                              && \
    gcloud config set project emulator-project                              && \
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
