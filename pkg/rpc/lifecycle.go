// Package rpc adapts bounded contract documents to the persistent gRPC service.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

type Lifecycle interface {
	Init(context.Context, contract.InitRequest) (contract.InitResponse, error)
	Run(context.Context, contract.RunRequest) (contract.RunResponse, error)
	Cancel(context.Context, contract.CancelRequest) (contract.CancelResponse, error)
	Health(context.Context) (contract.HealthResponse, error)
}
type Client struct{ conn pb.LifecycleClient }

func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{conn: pb.NewLifecycleClient(conn)}
}
func encode(value any) (*pb.Document, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > contract.MaxRecordBytes {
		return nil, errors.New("lifecycle document exceeds 64 KiB")
	}
	return &pb.Document{Json: data}, nil
}
func decode(document *pb.Document, value any) error {
	if document == nil || len(document.Json) > contract.MaxRecordBytes {
		return errors.New("invalid lifecycle document size")
	}
	d := json.NewDecoder(bytes.NewReader(document.Json))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing lifecycle JSON")
	}
	return nil
}
func (c *Client) Init(ctx context.Context, req contract.InitRequest) (out contract.InitResponse, err error) {
	in, err := encode(req)
	if err != nil {
		return out, err
	}
	doc, err := c.conn.Init(ctx, in)
	if err == nil {
		err = decode(doc, &out)
	}
	return
}
func (c *Client) Run(ctx context.Context, req contract.RunRequest) (out contract.RunResponse, err error) {
	in, err := encode(req)
	if err != nil {
		return out, err
	}
	doc, err := c.conn.Run(ctx, in)
	if err == nil {
		err = decode(doc, &out)
	}
	return
}
func (c *Client) Cancel(ctx context.Context, req contract.CancelRequest) (out contract.CancelResponse, err error) {
	in, err := encode(req)
	if err != nil {
		return out, err
	}
	doc, err := c.conn.Cancel(ctx, in)
	if err == nil {
		err = decode(doc, &out)
	}
	return
}
func (c *Client) Health(ctx context.Context) (out contract.HealthResponse, err error) {
	doc, err := c.conn.Health(ctx, &pb.Empty{})
	if err == nil {
		err = decode(doc, &out)
	}
	return
}

type server struct {
	pb.UnimplementedLifecycleServer
	lifecycle Lifecycle
}

func Register(registrar grpc.ServiceRegistrar, lifecycle Lifecycle) {
	pb.RegisterLifecycleServer(registrar, &server{lifecycle: lifecycle})
}
func malformed(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.InvalidArgument, "invalid lifecycle document")
}
func (s *server) Init(ctx context.Context, in *pb.Document) (*pb.Document, error) {
	var req contract.InitRequest
	if err := decode(in, &req); err != nil {
		return nil, malformed(err)
	}
	out, err := s.lifecycle.Init(ctx, req)
	if err != nil {
		return nil, err
	}
	return encode(out)
}
func (s *server) Run(ctx context.Context, in *pb.Document) (*pb.Document, error) {
	var req contract.RunRequest
	if err := decode(in, &req); err != nil {
		return nil, malformed(err)
	}
	out, err := s.lifecycle.Run(ctx, req)
	if err != nil {
		return nil, err
	}
	return encode(out)
}
func (s *server) Cancel(ctx context.Context, in *pb.Document) (*pb.Document, error) {
	var req contract.CancelRequest
	if err := decode(in, &req); err != nil {
		return nil, malformed(err)
	}
	out, err := s.lifecycle.Cancel(ctx, req)
	if err != nil {
		return nil, err
	}
	return encode(out)
}
func (s *server) Health(ctx context.Context, _ *pb.Empty) (*pb.Document, error) {
	out, err := s.lifecycle.Health(ctx)
	if err != nil {
		return nil, err
	}
	return encode(out)
}
