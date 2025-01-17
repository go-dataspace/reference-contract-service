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

// Package contract implements a simple RUN-DSP server using files in a directory structure
// as dataset entries.
// TODO: Describe how path is ascertained from Aiuth
// IMPORTANT: This provider is not meant for production use, it is just to demonstrate how to
// implement a RUN-DSP provider, IDs also don't persist through restarts.
package contract

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"

	"github.com/go-dataspace/run-dsp/logging"
	rundsrpc "github.com/go-dataspace/run-dsrpc/gen/go/dsp/v1alpha2"
	"github.com/google/uuid"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var controlClient rundsrpc.ControlServiceClient

// fileInfo is a small struct to keep file info together.
type fileInfo struct {
	ID       uuid.UUID
	FullPath string
	DirEntry fs.DirEntry
}

// Server implements both the ProviderService, and the publish http handler.
type Server struct {
	rundsrpc.UnimplementedContractServiceServer

	dir         fs.FS
	filesByID   map[uuid.UUID]*fileInfo
	registry    *publishRegistry
	publishRoot *url.URL
	credentials credentials.TransportCredentials
}

func missingClientError() error {
	return status.Errorf(codes.Internal, "no functional connection to RUN-DSP available")
}

// New creates a new provider service. dir is the root of the files, publishRoot is the URL the
// mux is mounted under.
func New(ctx context.Context, tlsCredentials credentials.TransportCredentials) (*Server, error) {
	return &Server{
		publishRoot: &url.URL{},
		credentials: tlsCredentials,
	}, nil
}

// Configure sets some callback configuration parameters.
func (s *Server) Configure(ctx context.Context, req *rundsrpc.ContractServiceConfigureRequest) (*rundsrpc.ContractServiceConfigureResponse, error) {
	logger := logging.Extract(ctx)
	logOpts := []grpclog.Option{
		grpclog.WithLogOnEvents(grpclog.StartCall, grpclog.FinishCall),
	}
	conn, err := grpc.NewClient(
		req.ConnectorAddress,
		grpc.WithTransportCredentials(s.credentials),
		grpc.WithChainUnaryInterceptor(
			grpclog.UnaryClientInterceptor(interceptorLogger(logger), logOpts...),
		),
		grpc.WithChainStreamInterceptor(
			grpclog.StreamClientInterceptor(interceptorLogger(logger), logOpts...),
		),
	)
	if err != nil {
		controlClient = nil
		return nil, fmt.Errorf("could not create client for the RUN-DSP control service: %w", err)
	}

	controlClient = rundsrpc.NewControlServiceClient(conn)

	_, err = controlClient.VerifyConnection(ctx, &rundsrpc.VerifyConnectionRequest{
		VerificationToken: req.VerificationToken,
	})
	if err != nil {
		controlClient = nil
		return nil, fmt.Errorf("could not verify connection to the RUN-DSP control service: %w", err)
	}

	return &rundsrpc.ContractServiceConfigureResponse{}, nil
}

// RequestReceived is called when RUN-DSP receives a ContractRequestMessage.
func (s *Server) RequestReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceRequestReceivedRequest,
) (*rundsrpc.ContractServiceRequestReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	controlClient.ContractOffer(ctx, &rundsrpc.ContractOfferRequest{
		Offer: req.Offer,
		Pid:   &req.Pid,
	})

	return &rundsrpc.ContractServiceRequestReceivedResponse{}, nil
}

// OfferReceived is called when RUN-DSP receives a ContractOfferMessage.
func (s *Server) OfferReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceOfferReceivedRequest,
) (*rundsrpc.ContractServiceOfferReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	controlClient.ContractAccept(ctx, &rundsrpc.ContractAcceptRequest{
		Pid: req.Pid,
	})

	return &rundsrpc.ContractServiceOfferReceivedResponse{}, nil
}

// AcceptedReceived is called when RUN-DSP receives an Accepted event message.
func (s *Server) AcceptedReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceAcceptedReceivedRequest,
) (*rundsrpc.ContractServiceAcceptedReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	controlClient.ContractAgree(ctx, &rundsrpc.ContractAgreeRequest{
		Pid: req.Pid,
	})
	return &rundsrpc.ContractServiceAcceptedReceivedResponse{}, nil
}

// AgreementReceived is called when RUN-DSP receives a ContractAgreementMessage.
func (s *Server) AgreementReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceAgreementReceivedRequest,
) (*rundsrpc.ContractServiceAgreementReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	controlClient.ContractVerify(ctx, &rundsrpc.ContractVerifyRequest{
		Pid: req.Pid,
	})

	return &rundsrpc.ContractServiceAgreementReceivedResponse{}, nil
}

// VerificationReceived is called when RUN-DSP receives a ContractVerificationMessage.
func (s *Server) VerificationReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceVerificationReceivedRequest,
) (*rundsrpc.ContractServiceVerificationReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	controlClient.ContractFinalize(ctx, &rundsrpc.ContractFinalizeRequest{
		Pid: req.Pid,
	})

	return &rundsrpc.ContractServiceVerificationReceivedResponse{}, nil
}

// FinalizationReceived is called when RUN-DSP receives a Finalized event message.
func (s *Server) FinalizationReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceFinalizationReceivedRequest,
) (*rundsrpc.ContractServiceFinalizationReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	// Probably store the finalized state of the negotiation, or similar.

	return &rundsrpc.ContractServiceFinalizationReceivedResponse{}, nil
}

// TerminationReceived is called when RUN-DSP receives a ContractTerminationMessage
func (s *Server) TerminationReceived(
	ctx context.Context,
	req *rundsrpc.ContractServiceTerminationReceivedRequest,
) (*rundsrpc.ContractServiceTerminationReceivedResponse, error) {
	if controlClient == nil {
		return nil, missingClientError()
	}

	// Probably storing the terminated state of the negotiation, and maybe logging an error.

	return &rundsrpc.ContractServiceTerminationReceivedResponse{}, nil
}

func interceptorLogger(l *slog.Logger) grpclog.Logger {
	return grpclog.LoggerFunc(func(ctx context.Context, lvl grpclog.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}
