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

// Package authprocessor checks if there's an authorization header, and then processes it into a
// path prefix and injects it into the context.
// It will construct an auth header like "foo;bar;baz" to "foo/bar/baz", which is used as a
// prefix to check for the files, relative to the serve root of the service.
// We are aware this is not an actual auth method.
// To repeat DO NOT USE THIS AS-IS IN PRODUCTION, THIS IS JUST TO DEMONSTRATE THAT THE AUTH HEADER
// IS BEING FORWARDED BY RUN-DSP. THIS OFFERS NO SECURITY.
package authprocessor

import (
	"context"
	"path"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKeyType string

const contextKey contextKeyType = "searchPrefix"

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errUnauthorised    = status.Errorf(codes.Unauthenticated, "unauthorised")
)

// UnaryInterceptor will process the authorization header, convert it to a prefix, and then
// inject it into the context.
func UnaryInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	// md, ok := metadata.FromIncomingContext(ctx)
	// if !ok {
	// 	return nil, errMissingMetadata
	// }
	// ctx, err := injectPrefix(ctx, md)
	// if err != nil && info.FullMethod != "/dsp.v1alpha2.ProviderService/Ping" {
	// 	return nil, err
	// }
	return handler(ctx, req)
}

func injectPrefix(ctx context.Context, md metadata.MD) (context.Context, error) {
	var prefix string
	authContents := md["authorization"]
	if len(authContents) == 1 {
		parts := strings.Split(authContents[0], ";")
		prefix = path.Join(parts...)
	}
	// Checking for "valid" "authorization", again, this is for demonstration purposes.
	ctx = context.WithValue(ctx, contextKey, prefix)
	if prefix == "" {
		return ctx, errUnauthorised
	}
	return ctx, nil
}

type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStream) Context() context.Context {
	return s.ctx
}

// StreamInterceptor will process the authorization header, convert it to a prefix, and then
// inject it into the context, but for streams.
func StreamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	// md, ok := metadata.FromIncomingContext(ctx)
	// if !ok {
	// 	return errMissingMetadata
	// }
	// ctx, err := injectPrefix(ctx, md)
	// if err != nil {
	// 	return err
	// }
	return handler(srv, &serverStream{ss, ctx})
}

// ExtractPrefix will extract the prefix from the context.
func ExtractPrefix(ctx context.Context) string {
	ctxVal := ctx.Value(contextKey)
	if ctxVal == nil {
		return ""
	}
	val, ok := ctxVal.(string)
	if !ok {
		panic("Prefix not of right type")
	}
	return val
}
