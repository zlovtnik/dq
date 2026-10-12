package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/telemetry"
)

const MaxRecordBytes = 65536

var ErrUnavailable = errors.New("coordinator unavailable")
var ErrResponse = errors.New("invalid coordinator response")

type Actor struct {
	ID     string
	Scopes []string
	Roles  []string
}
type actorKey struct{}

func WithActor(ctx context.Context, actor Actor) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}
func ServiceContext(ctx context.Context) context.Context { return WithActor(ctx, Actor{}) }

type StatusError struct{ Status int }

func (e *StatusError) Error() string { return "coordinator rejected request" }

type API interface {
	Do(context.Context, string, string, url.Values, any, any) error
}
type Client struct {
	BaseURL, Token string
	HTTP           *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/") + "/internal/dq/v1", Token: token, HTTP: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	ctx,span:=telemetry.Start(ctx,"dq.control");defer span.End()
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil || len(data) > MaxRecordBytes {
			return errors.New("control record exceeds limit")
		}
	}
	endpoint := c.BaseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid coordinator request")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	tp,ts:=telemetry.Fields(ctx);if tp!=""{req.Header.Set("traceparent",tp)};if ts!=""{req.Header.Set("tracestate",ts)}
	if a, ok := ctx.Value(actorKey{}).(Actor); ok && a.ID != "" {
		req.Header.Set("X-DQ-Actor", a.ID)
		req.Header.Set("X-DQ-Scopes", strings.Join(a.Scopes, ","))
		req.Header.Set("X-DQ-Roles", strings.Join(a.Roles, ","))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxRecordBytes))
		return &StatusError{Status: resp.StatusCode}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxRecordBytes+1))
		return err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxRecordBytes+1))
	if err != nil || len(b) > MaxRecordBytes {
		return ErrResponse
	}
	if err := json.Unmarshal(b, out); err != nil {
		return ErrResponse
	}
	return nil
}

func Query(installation, namespace string) url.Values {
	return url.Values{"installation_id": {installation}, "namespace": {namespace}}
}
