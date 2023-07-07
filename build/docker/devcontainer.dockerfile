################################################################################
#                                     BUILD                                    #
################################################################################

FROM gcr.io/cloud-spanner-emulator/bazel-ubuntu:5.4.0-18.04 as build

RUN apt-get update && DEBIAN_FRONTEND="noninteractive"                         \
    TZ="America/Los_Angeles" apt-get install -y tzdata

# Unfortunately ZetaSQL has issues with clang (default bazel compiler), so
# we install GCC. Also install make for rules_foreign_cc bazel rules.
ENV GCC_VERSION=8
RUN apt-get -qq update                                                      && \
    apt-get -qq install -y software-properties-common make rename  git
RUN add-apt-repository ppa:ubuntu-toolchain-r/test                          && \
    apt-get -qq update                                                      && \
    apt-get -qq install -y gcc-${GCC_VERSION} g++-${GCC_VERSION}            && \
    apt-get -qq install -y ca-certificates libgnutls30                      && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 90  \
                        --slave   /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION} && \
    update-alternatives --set gcc /usr/bin/gcc-${GCC_VERSION}

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
