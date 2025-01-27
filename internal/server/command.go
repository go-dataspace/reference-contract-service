// Copyright 2025 go-dataspace
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides the server subcommand.
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"

	"github.com/go-dataspace/reference-contract-service/internal/authprocessor"
	"github.com/go-dataspace/reference-contract-service/internal/cli"
	"github.com/go-dataspace/reference-contract-service/internal/contract"
	"github.com/go-dataspace/run-dsp/logging"
	contractservice "github.com/go-dataspace/run-dsrpc/gen/go/dsp/v1alpha2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

//nolint:lll
type Command struct {
	// Generic settings
	FileRoot string `help:"Root directory to write files in" default:"/var/lib/run-dsp/contract"`

	// GRPC settings
	GRPCListenAddr               string `help:"Listen address for the GRPC service" default:"0.0.0.0" env:"GRPC_LISTEN_ADDR"`
	GRPCPort                     int    `help:"Port for the GRPC service" default:"9092" env:"GRPC_PORT"`
	GRPCInsecure                 bool   `help:"Disable TLS" default:"false" env:"GRPC_INSECURE"`
	GRPCCert                     string `help:"Client certificate to use to authenticate with provider" env:"CONTRACT_SERVICE_CLIENT_CERT"`
	GRPCCertKey                  string `help:"Key to the client certificate" env:"CONTRACT_SERVICE_CLIENT_CERT_KEY"`
	GRPCVerifyClientCertificates bool   `help:"Require validated client certificates to connect" default:"false" env:"GRPC_VERIFY_CLIENT_CERTIFICATES" `
	GRPCClientCACert             string `help:"Custom CA certificate to verify client certificates with" env:"CONTRACT_SERVICE_CA"`

	// Client certificate settings
	GRPCControlInsecure bool   `help:"Disable TLS" default:"false" env:"CONTROL_INSECURE"`
	GRPCControlCert     string `help:"Client certificate to use to authenticate with provider" env:"CONTROL_CLIENT_CERT"`
	GRPCControlCertKey  string `help:"Key to the client certificate" env:"CONTROL_CLIENT_CERT_KEY"`
	GRPCControlCACert   string `help:"Custom CA certificate to verify client certificates with" env:"CONTROL_SERVICE_CA"`
}

func (c *Command) Validate() error {
	_, err := checkFile(c.FileRoot, true)
	if err != nil {
		return fmt.Errorf("file-root: %w", err)
	}

	if c.GRPCInsecure {
		return nil
	}

	certSupplied, err := checkFile(c.GRPCCert, false)
	if err != nil {
		return fmt.Errorf("GRPC certificate: %w", err)
	}
	keySupplied, err := checkFile(c.GRPCCertKey, false)
	if err != nil {
		return fmt.Errorf("GRPC certificate key: %w", err)
	}

	if !certSupplied {
		return fmt.Errorf("GRPC certificate not supplied")
	}

	if !keySupplied {
		return fmt.Errorf("GRPC certificate key not supplied")
	}

	if c.GRPCVerifyClientCertificates {
		caSupplied, err := checkFile(c.GRPCClientCACert, false)
		if err != nil {
			return fmt.Errorf("GRPC client CA certificate: %w", err)
		}
		if !caSupplied {
			return fmt.Errorf("GRPC client CA certificate required when using client cert auth")
		}
	}

	return nil
}

func checkFile(l string, wantDir bool) (bool, error) {
	if l != "" {
		f, err := os.Stat(l)
		if err != nil {
			return true, fmt.Errorf("could not read %s: %w", l, err)
		}
		if f.IsDir() != wantDir {
			return true, fmt.Errorf("%s is a directory", l)
		}
		return true, nil
	}
	return false, nil
}

func (c *Command) Run(p cli.Params) error {
	ctx, cancel := signal.NotifyContext(p.Context(), os.Interrupt, os.Kill)
	defer cancel()

	logger := logging.Extract(ctx)
	logger.Info("Starting up",
		"file_root", c.FileRoot,
		"grpc_addr", c.GRPCListenAddr,
		"grpc_port", c.GRPCPort,
	)

	wg := &sync.WaitGroup{}

	credentials, err := c.loadControlTLSCredentials()
	if err != nil {
		return fmt.Errorf("couldn't load control client credentials: %w", err)
	}

	contract, err := contract.New(ctx, credentials)
	if err != nil {
		return fmt.Errorf("couldn't start FS provider: %w", err)
	}

	err = c.startGRPC(ctx, wg, contract)
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

func (c *Command) startGRPC(ctx context.Context, wg *sync.WaitGroup, cs *contract.Server) error {
	wg.Add(1)
	logger := logging.Extract(ctx).With("grpc_addr", c.GRPCListenAddr, "grpc_port", c.GRPCPort)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", c.GRPCListenAddr, c.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen on %s:%d: %w", c.GRPCListenAddr, c.GRPCPort, err)
	}

	tlsCredentials, err := c.loadTLSCredentials()
	if err != nil {
		return fmt.Errorf("could not load TLS credentials: %w", err)
	}

	logOpts := []grpclog.Option{
		grpclog.WithLogOnEvents(grpclog.StartCall, grpclog.FinishCall),
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(tlsCredentials),
		grpc.ChainUnaryInterceptor(
			grpclog.UnaryServerInterceptor(interceptorLogger(logger), logOpts...),
			authprocessor.UnaryInterceptor,
		),
		grpc.ChainStreamInterceptor(
			grpclog.StreamServerInterceptor(interceptorLogger(logger), logOpts...),
			authprocessor.StreamInterceptor,
		),
	)
	contractservice.RegisterContractServiceServer(grpcServer, cs)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("Starting GRPC service")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("GRPC service exited with error", "error", err)
		}
		logger.Info("GRPC service shutdown.")
	}()

	// Wait until we get the done signal and then shut down the grpc service.
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logger.Info("Shutting down GRPC service.")
		grpcServer.GracefulStop()
	}()
	return nil
}

func (c *Command) loadTLSCredentials() (credentials.TransportCredentials, error) {
	if c.GRPCInsecure {
		return insecure.NewCredentials(), nil
	}

	serverCert, err := tls.LoadX509KeyPair(c.GRPCCert, c.GRPCCertKey)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert,
	}

	if c.GRPCVerifyClientCertificates {
		pemServerCA, err := os.ReadFile(c.GRPCClientCACert)
		if err != nil {
			return nil, fmt.Errorf("couldn't read CA file: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemServerCA) {
			return nil, fmt.Errorf("failed to add server CA certificate")
		}
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = certPool
	}

	return credentials.NewTLS(config), nil
}

func (c *Command) loadControlTLSCredentials() (credentials.TransportCredentials, error) {
	if c.GRPCControlInsecure {
		return insecure.NewCredentials(), nil
	}

	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if c.GRPCControlCACert != "" {
		pemServerCA, err := os.ReadFile(c.GRPCControlCACert)
		if err != nil {
			return nil, fmt.Errorf("couldn't read CA file: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemServerCA) {
			return nil, fmt.Errorf("failed to add server CA certificate")
		}
		config.RootCAs = certPool
	}

	if c.GRPCControlCert != "" {
		clientCert, err := tls.LoadX509KeyPair(c.GRPCControlCert, c.GRPCControlCertKey)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{clientCert}
	}

	return credentials.NewTLS(config), nil
}

func interceptorLogger(l *slog.Logger) grpclog.Logger {
	return grpclog.LoggerFunc(func(ctx context.Context, lvl grpclog.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}
