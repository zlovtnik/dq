package manager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/rpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type serviceCredentials struct {
	token    string
	insecure bool
}

func (c serviceCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}
func (c serviceCredentials) RequireTransportSecurity() bool { return !c.insecure }

// Connect creates one persistent connection per deployment-configured endpoint.
// Neither registered manifests nor invocation parameters supply dial addresses.
func Connect(c config.Config) ([]*Worker, func(), error) {
	var transport credentials.TransportCredentials
	if c.InsecureDevelopment {
		transport = insecure.NewCredentials()
	} else {
		ca, err := os.ReadFile(c.WorkerCA)
		if err != nil {
			return nil, nil, errors.New("worker CA unavailable")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			return nil, nil, errors.New("invalid worker CA")
		}
		certificate, err := tls.LoadX509KeyPair(c.WorkerCert, c.WorkerKey)
		if err != nil {
			return nil, nil, errors.New("worker mutual TLS identity unavailable")
		}
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{certificate}})
	}
	if len(c.Workers) > 0 && len(c.WorkerToken) < 32 {
		return nil, nil, errors.New("worker service token required")
	}
	workers := []*Worker{}
	connections := []*grpc.ClientConn{}
	closeAll := func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}
	for _, approved := range c.Workers {
		connection, err := grpc.NewClient("dns:///"+approved.Address, grpc.WithTransportCredentials(transport), grpc.WithPerRPCCredentials(serviceCredentials{token: c.WorkerToken, insecure: c.InsecureDevelopment}), grpc.WithStatsHandler(otelgrpc.NewClientHandler()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(contract.MaxRecordBytes+1024), grpc.MaxCallSendMsgSize(contract.MaxRecordBytes+1024)))
		if err != nil {
			closeAll()
			return nil, nil, errors.New("invalid approved worker connection")
		}
		connections = append(connections, connection)
		workers = append(workers, &Worker{Approved: approved, Client: rpc.NewClient(connection)})
	}
	return workers, closeAll, nil
}
