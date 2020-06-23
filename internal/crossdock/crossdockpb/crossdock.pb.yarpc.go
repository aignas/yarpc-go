// Code generated by protoc-gen-yarpc-go. DO NOT EDIT.
// source: internal/crossdock/crossdockpb/crossdock.proto

// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package crossdockpb

import (
	"context"
	"io/ioutil"
	"reflect"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/fx"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/x/restriction"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/encoding/protobuf/reflection"
)

var _ = ioutil.NopCloser

// EchoYARPCClient is the YARPC client-side interface for the Echo service.
type EchoYARPCClient interface {
	Echo(context.Context, *Ping, ...yarpc.CallOption) (*Pong, error)
}

func newEchoYARPCClient(clientConfig transport.ClientConfig, anyResolver jsonpb.AnyResolver, options ...protobuf.ClientOption) EchoYARPCClient {
	return &_EchoYARPCCaller{protobuf.NewStreamClient(
		protobuf.ClientParams{
			ServiceName:  "uber.yarpc.internal.crossdock.Echo",
			ClientConfig: clientConfig,
			AnyResolver:  anyResolver,
			Options:      options,
		},
	)}
}

// NewEchoYARPCClient builds a new YARPC client for the Echo service.
func NewEchoYARPCClient(clientConfig transport.ClientConfig, options ...protobuf.ClientOption) EchoYARPCClient {
	return newEchoYARPCClient(clientConfig, nil, options...)
}

// EchoYARPCServer is the YARPC server-side interface for the Echo service.
type EchoYARPCServer interface {
	Echo(context.Context, *Ping) (*Pong, error)
}

type buildEchoYARPCProceduresParams struct {
	Server      EchoYARPCServer
	AnyResolver jsonpb.AnyResolver
}

func buildEchoYARPCProcedures(params buildEchoYARPCProceduresParams) []transport.Procedure {
	handler := &_EchoYARPCHandler{params.Server}
	return protobuf.BuildProcedures(
		protobuf.BuildProceduresParams{
			ServiceName: "uber.yarpc.internal.crossdock.Echo",
			UnaryHandlerParams: []protobuf.BuildProceduresUnaryHandlerParams{
				{
					MethodName: "Echo",
					Handler: protobuf.NewUnaryHandler(
						protobuf.UnaryHandlerParams{
							Handle:      handler.Echo,
							NewRequest:  newEchoServiceEchoYARPCRequest,
							AnyResolver: params.AnyResolver,
						},
					),
				},
			},
			OnewayHandlerParams: []protobuf.BuildProceduresOnewayHandlerParams{},
			StreamHandlerParams: []protobuf.BuildProceduresStreamHandlerParams{},
		},
	)
}

// BuildEchoYARPCProcedures prepares an implementation of the Echo service for YARPC registration.
func BuildEchoYARPCProcedures(server EchoYARPCServer) []transport.Procedure {
	return buildEchoYARPCProcedures(buildEchoYARPCProceduresParams{Server: server})
}

// FxEchoYARPCClientParams defines the input
// for NewFxEchoYARPCClient. It provides the
// paramaters to get a EchoYARPCClient in an
// Fx application.
type FxEchoYARPCClientParams struct {
	fx.In

	Provider    yarpc.ClientConfig
	AnyResolver jsonpb.AnyResolver  `name:"yarpcfx" optional:"true"`
	Restriction restriction.Checker `optional:"true"`
}

// FxEchoYARPCClientResult defines the output
// of NewFxEchoYARPCClient. It provides a
// EchoYARPCClient to an Fx application.
type FxEchoYARPCClientResult struct {
	fx.Out

	Client EchoYARPCClient

	// We are using an fx.Out struct here instead of just returning a client
	// so that we can add more values or add named versions of the client in
	// the future without breaking any existing code.
}

// NewFxEchoYARPCClient provides a EchoYARPCClient
// to an Fx application using the given name for routing.
//
//  fx.Provide(
//    crossdockpb.NewFxEchoYARPCClient("service-name"),
//    ...
//  )
func NewFxEchoYARPCClient(name string, options ...protobuf.ClientOption) interface{} {
	return func(params FxEchoYARPCClientParams) FxEchoYARPCClientResult {
		cc := params.Provider.ClientConfig(name)

		if params.Restriction != nil {
			if namer, ok := cc.GetUnaryOutbound().(transport.Namer); ok {
				if err := params.Restriction.Check(protobuf.Encoding, namer.TransportName()); err != nil {
					panic(err.Error())
				}
			}
		}

		return FxEchoYARPCClientResult{
			Client: newEchoYARPCClient(cc, params.AnyResolver, options...),
		}
	}
}

// FxEchoYARPCProceduresParams defines the input
// for NewFxEchoYARPCProcedures. It provides the
// paramaters to get EchoYARPCServer procedures in an
// Fx application.
type FxEchoYARPCProceduresParams struct {
	fx.In

	Server      EchoYARPCServer
	AnyResolver jsonpb.AnyResolver `name:"yarpcfx" optional:"true"`
}

// FxEchoYARPCProceduresResult defines the output
// of NewFxEchoYARPCProcedures. It provides
// EchoYARPCServer procedures to an Fx application.
//
// The procedures are provided to the "yarpcfx" value group.
// Dig 1.2 or newer must be used for this feature to work.
type FxEchoYARPCProceduresResult struct {
	fx.Out

	Procedures     []transport.Procedure `group:"yarpcfx"`
	ReflectionMeta reflection.ServerMeta `group:"yarpcfx"`
}

// NewFxEchoYARPCProcedures provides EchoYARPCServer procedures to an Fx application.
// It expects a EchoYARPCServer to be present in the container.
//
//  fx.Provide(
//    crossdockpb.NewFxEchoYARPCProcedures(),
//    ...
//  )
func NewFxEchoYARPCProcedures() interface{} {
	return func(params FxEchoYARPCProceduresParams) FxEchoYARPCProceduresResult {
		return FxEchoYARPCProceduresResult{
			Procedures: buildEchoYARPCProcedures(buildEchoYARPCProceduresParams{
				Server:      params.Server,
				AnyResolver: params.AnyResolver,
			}),
			ReflectionMeta: reflection.ServerMeta{
				ServiceName:     "uber.yarpc.internal.crossdock.Echo",
				FileDescriptors: yarpcFileDescriptorClosure6acfd671bab786d8,
			},
		}
	}
}

type _EchoYARPCCaller struct {
	streamClient protobuf.StreamClient
}

func (c *_EchoYARPCCaller) Echo(ctx context.Context, request *Ping, options ...yarpc.CallOption) (*Pong, error) {
	responseMessage, err := c.streamClient.Call(ctx, "Echo", request, newEchoServiceEchoYARPCResponse, options...)
	if responseMessage == nil {
		return nil, err
	}
	response, ok := responseMessage.(*Pong)
	if !ok {
		return nil, protobuf.CastError(emptyEchoServiceEchoYARPCResponse, responseMessage)
	}
	return response, err
}

type _EchoYARPCHandler struct {
	server EchoYARPCServer
}

func (h *_EchoYARPCHandler) Echo(ctx context.Context, requestMessage proto.Message) (proto.Message, error) {
	var request *Ping
	var ok bool
	if requestMessage != nil {
		request, ok = requestMessage.(*Ping)
		if !ok {
			return nil, protobuf.CastError(emptyEchoServiceEchoYARPCRequest, requestMessage)
		}
	}
	response, err := h.server.Echo(ctx, request)
	if response == nil {
		return nil, err
	}
	return response, err
}

func newEchoServiceEchoYARPCRequest() proto.Message {
	return &Ping{}
}

func newEchoServiceEchoYARPCResponse() proto.Message {
	return &Pong{}
}

var (
	emptyEchoServiceEchoYARPCRequest  = &Ping{}
	emptyEchoServiceEchoYARPCResponse = &Pong{}
)

var yarpcFileDescriptorClosure6acfd671bab786d8 = [][]byte{
	// internal/crossdock/crossdockpb/crossdock.proto
	[]byte{
		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xd2, 0xcb, 0xcc, 0x2b, 0x49,
		0x2d, 0xca, 0x4b, 0xcc, 0xd1, 0x4f, 0x2e, 0xca, 0x2f, 0x2e, 0x4e, 0xc9, 0x4f, 0xce, 0x46, 0xb0,
		0x0a, 0x92, 0x10, 0x6c, 0xbd, 0x82, 0xa2, 0xfc, 0x92, 0x7c, 0x21, 0xd9, 0xd2, 0xa4, 0xd4, 0x22,
		0xbd, 0xca, 0xc4, 0xa2, 0x82, 0x64, 0xb8, 0x56, 0x3d, 0xb8, 0x22, 0x25, 0x29, 0x2e, 0x96, 0x80,
		0xcc, 0xbc, 0x74, 0x21, 0x21, 0x2e, 0x96, 0xa4, 0xd4, 0xd4, 0x02, 0x09, 0x46, 0x05, 0x46, 0x0d,
		0xce, 0x20, 0x30, 0x1b, 0x2c, 0x97, 0x0f, 0x95, 0xcb, 0xcf, 0x47, 0xc8, 0xe5, 0xe7, 0x17, 0x28,
		0xc9, 0x72, 0xb1, 0x86, 0xe4, 0x67, 0xa7, 0xe6, 0x09, 0x89, 0x70, 0xb1, 0x96, 0x25, 0xe6, 0x94,
		0xa6, 0x42, 0x65, 0x21, 0x1c, 0xa3, 0x08, 0x2e, 0x16, 0xd7, 0xe4, 0x8c, 0x7c, 0xa1, 0x00, 0x28,
		0xad, 0xac, 0x87, 0xd7, 0x19, 0x7a, 0x20, 0x37, 0x48, 0x11, 0x54, 0x94, 0x9f, 0x97, 0xee, 0xc4,
		0x1b, 0xc5, 0x8d, 0xe4, 0xdd, 0x24, 0x36, 0xb0, 0x2f, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff,
		0x71, 0x97, 0xff, 0x4f, 0x17, 0x01, 0x00, 0x00,
	},
}

func init() {
	yarpc.RegisterClientBuilder(
		func(clientConfig transport.ClientConfig, structField reflect.StructField) EchoYARPCClient {
			return NewEchoYARPCClient(clientConfig, protobuf.ClientBuilderOptions(clientConfig, structField)...)
		},
	)
}
