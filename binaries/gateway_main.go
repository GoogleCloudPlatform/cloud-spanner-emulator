//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// gateway_main is a REST gateway for the emulator grpc server. The emulator grpc server needs
// to be implemented in C++ due to its heavy dependence on ZetaSQL. To make it easy to use, we
// run the grpc server C++ binary as a subprocess of the REST gateway.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"cloud_spanner_emulator/gateway"
)

var (
	// Networking related flags.
	hostname = flag.String("hostname", "localhost", "Hostname for the emulator servers.")
	grpcPort = flag.Int("grpc_port", 9010, "Port on which to run the emulator grpc server.")
	httpPort = flag.Int("http_port", 9020, "Port on which to run the emulator http server.")

	// Subprocess related flags.
	grpcBinary = flag.String("grpc_binary", "emulator_main", "Location of the grpc binary.")

	// Logging related flags.
	copyEmulatorStdout = flag.Bool("copy_emulator_stdout", false,
		"If true, the gateway will copy the emulator's stdout to its stdout.")
	copyEmulatorStderr = flag.Bool("copy_emulator_stderr", true,
		"If true, the gateway will copy the emulator's stderr to its stderr.")
	logRequests = flag.Bool("log_requests", false,
		"If true, gRPC requests and responses will be logged to stdout.")

	// Emulator specific flags.
	enableFaultInjection = flag.Bool("enable_fault_injection", false,
		"If true, the emulator will inject faults at runtime (e.g. randomly abort commit "+
			"requests to allow testing application abort-retry behavior).")
	disableQueryNullFilteredIndexCheck = flag.Bool("disable_query_null_filtered_index_check", false,
		"If true, then queries that use NULL_FILTERED indexes will be answered.")
	overrideMaxDatabasesPerInstance = flag.Int("override_max_databases_per_instance", 100,
		"If set at a value greater than the default limit of Spanner, overrides the allowed "+
			"maximum number of databases per instance. If the "+
			"MAX_DATABASES_PER_INSTANCE environment variable is set, it overrides the "+
			"value set in this flag but it is only respected if it's greater than "+
			"the default limit of Spanner.")
	overrideChangeStreamPartitionTokenAliveSeconds = flag.Int("override_change_stream_partition_token_alive_seconds", -1,
		"If set to X seconds, and X is greater than 0, then override the default partition token alive"+
			"time from 20-40 seconds(default for Emulator only, not for production Spanner) to X-2X seconds.")
)

// resolveGRPCBinary figures out the full path to the grpc binary from the --grpc_binary flag.
func resolveGRPCBinary() string {
	retval := *grpcBinary

	// Early-exit if we were given the full path (else we assume we were only given the filename).
	if path.IsAbs(retval) {
		return retval
	}

	// Get the location of this executable's directory.
	gwPath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	gwDir := filepath.Dir(gwPath)

	// Check if the grpc binary is in the same directory as the gateway binary (this happens in the
	// release package) else check if the grpc binary is in the parent directory of the gateway
	// binary (this happens when using bazel run).
	retval = filepath.Join(gwDir, *grpcBinary)
	_, err = os.Stat(retval)
	if os.IsNotExist(err) {
		retval = filepath.Join(filepath.Dir(gwDir), *grpcBinary)
	}
	_, err = os.Stat(retval)
	if os.IsNotExist(err) {
		log.Fatal(err)
	}

	return retval
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	instanceDbs := *overrideMaxDatabasesPerInstance
	var err error
	// If the environment variable is set, it overrides the value specified
	// in the --override_max_databases_per_instance flag.
	if os.Getenv("MAX_DATABASES_PER_INSTANCE") != "" {
		instanceDbs, err = strconv.Atoi(os.Getenv("MAX_DATABASES_PER_INSTANCE"))
		if err != nil {
			log.Fatal(err)
		}
	}
	// If the environment variable is set, it overrides the value specified
	// in the --change_stream_partition_token_alive_seconds flag.
	overrideChangeStreamPartitionTokenAliveSeconds := *overrideChangeStreamPartitionTokenAliveSeconds
	if os.Getenv("CHANGE_STREAM_PARTITION_TOKEN_ALIVE_SECONDS") != "" {
		overrideChangeStreamPartitionTokenAliveSeconds, err = strconv.Atoi(os.Getenv("CHANGE_STREAM_PARTITION_TOKEN_ALIVE_SECONDS"))
		if err != nil {
			log.Fatal(err)
		}
	}
	// Start the gateway http server. This will run the emulator grpc server as a subprocess and
	// proxy http/json requests into grpc requests.
	gwopts := gateway.Options{
		GatewayAddress:                     fmt.Sprintf("%s:%d", *hostname, *httpPort),
		FrontendBinary:                     resolveGRPCBinary(),
		FrontendAddress:                    fmt.Sprintf("%s:%d", *hostname, *grpcPort),
		CopyEmulatorStdout:                 *copyEmulatorStdout,
		CopyEmulatorStderr:                 *copyEmulatorStderr,
		LogRequests:                        *logRequests,
		EnableFaultInjection:               *enableFaultInjection,
		DisableQueryNullFilteredIndexCheck: *disableQueryNullFilteredIndexCheck,
		OverrideMaxDatabasesPerInstance:    instanceDbs,
		OverrideChangeStreamPartitionTokenAliveSeconds: overrideChangeStreamPartitionTokenAliveSeconds,
	}
	gw := gateway.New(gwopts)
	gw.Run()
}
