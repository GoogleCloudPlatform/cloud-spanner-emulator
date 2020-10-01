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

// Package gateway implements a REST gateway for the emulator GRPC service.
package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"google.golang.org/grpc"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"

	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	lrgw "cloud_spanner_emulator/gateway/longrunning_operations_gateway"
	dagw "cloud_spanner_emulator/gateway/spanner_admin_database_gateway"
	spgw "cloud_spanner_emulator/gateway/spanner_gateway"
	iagw "cloud_spanner_emulator/gateway/spanner_admin_instance_gateway"
)

// Options encapsulates options for the emulator gateway.
type Options struct {
	GatewayAddress       string
	FrontendBinary       string
	FrontendAddress      string
	CopyEmulatorStdout   bool
	CopyEmulatorStderr   bool
	LogRequests          bool
	EnableFaultInjection bool
}

// Gateway implements the emulator gateway server.
type Gateway struct {
	opts Options
}

// New returns a new gateway.
func New(opts Options) *Gateway {
	return &Gateway{opts}
}

// Run starts the emulator gateway server.
func (gw *Gateway) Run() {
	// Start the emulator grpc server and redirect its output.
	emulatorArgs := []string{

		"--host_port", gw.opts.FrontendAddress,
	}
	if gw.opts.LogRequests {
		emulatorArgs = append(emulatorArgs, "--log_requests")
	}
	if gw.opts.EnableFaultInjection {
		emulatorArgs = append(emulatorArgs, "--enable_fault_injection")
	}

	cmd := exec.Command(gw.opts.FrontendBinary, emulatorArgs...)

	// Proxy emulator log to gateway log.
	if gw.opts.CopyEmulatorStdout {
		cmd.Stdout = os.Stdout
	}
	if gw.opts.CopyEmulatorStderr {
		cmd.Stderr = os.Stderr
	}

	// Start the grpc server but won't block for the grpc server to be up.
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	// Terminate the grpc server if the gateway server is terminated.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		// Release resources e.g., network ports associated with the process.
		// This is required since gateway may receive an interrupt signal for
		// shutdown before Wait() returns.
		cmd.Process.Release()
		cmd.Process.Kill()
		os.Exit(0)
	}()

	// Terminate the gateway server if the grpc server is terminated.
	go func() {
		cmd.Wait()
		log.Println("Shutting down gateway server since grpc server is terminated.")
		os.Exit(cmd.ProcessState.ExitCode())
	}()

	// Wait for the grpc server to be up.
	ctx := context.Background()
	addr := gw.opts.FrontendAddress
	if err = waitForReady(ctx, addr); err != nil {
		log.Fatal(fmt.Errorf("Error waiting for emulator to start: %v", err))
	}

	// Setup the gateway services.
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{OrigName: false}))
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err = spgw.RegisterSpannerHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = iagw.RegisterInstanceAdminHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = dagw.RegisterDatabaseAdminHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}
	err = lrgw.RegisterOperationsHandlerFromEndpoint(ctx, mux, addr, opts)
	if err != nil {
		log.Fatal(err)
	}

	// Start the gateway http server.
	log.Println("Cloud Spanner emulator running.")
	log.Println("REST server listening at", gw.opts.GatewayAddress)
	log.Println("gRPC server listening at", gw.opts.FrontendAddress)
	err = http.ListenAndServe(gw.opts.GatewayAddress, mux)
	if err != nil {
		log.Fatal(err)
	}
}

func waitForReady(ctx context.Context, endpoint string) error {
	timeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	// To test whether the server is up, wait for ListInstanceConfigs to respond
	// for a dummy project.
	instanceAdminClient := instancepb.NewInstanceAdminClient(conn)
	if _, err = instanceAdminClient.ListInstanceConfigs(ctx,
		&instancepb.ListInstanceConfigsRequest{
			Parent: "projects/test-project",
		}); err != nil {
		return fmt.Errorf("emulator failed to come up at %v within %v deadline: %v",
			endpoint, timeout.String(), err)
	}
	return nil
}
