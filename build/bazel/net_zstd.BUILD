package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "zstdlib",
    srcs = glob([
        "common/*.c",
        "common/*.h",
        "compress/*.c",
        "compress/*.h",
        "decompress/*.c",
        "decompress/*.h",
        "decompress/huf_decompress_amd64.S",
    ]),
    hdrs = [
        "zstd.h",
        "zstd_errors.h"
    ],
)
