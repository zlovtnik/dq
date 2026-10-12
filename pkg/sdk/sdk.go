// Package sdk supplies resource access to processors. Processors do not create
// infrastructure clients or receive infrastructure credentials.
package sdk

import (
	"context"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"io"
)

type Output interface {
	io.WriteCloser
	Commit(context.Context) (contract.ArtifactRef, error)
}

type Artifacts interface {
	OpenRead(context.Context, string, contract.ArtifactRef) (io.ReadCloser, error)
	CreateOutput(context.Context, string, string) (Output, error)
}

type Reporter interface {
	Report(context.Context, contract.Envelope) error
}
type ResourceFactory interface {
	ForAttempt(contract.RunRequest) (Artifacts, error)
}
type RunContext struct {
	Artifacts Artifacts
	Progress  func(context.Context, int64, []byte) error
}
type Processor interface {
	Run(context.Context, contract.RunRequest, RunContext) (contract.OutputManifest, error)
}

// Failure exposes a bounded public code without exposing underlying storage,
// broker, or payload details to a lifecycle record.
type Failure struct{ Code, Class string }

func (e *Failure) Error() string  { return e.Code }
func Permanent(code string) error { return &Failure{Code: code, Class: "permanent"} }
