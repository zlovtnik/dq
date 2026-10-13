package config

import (
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Identity struct {
	ID             string   `json:"id"`
	Token          string   `json:"token"`
	Roles          []string `json:"roles"`
	InstallationID string   `json:"installation_id"`
	Namespaces     []string `json:"namespaces"`
	Scopes         []string `json:"scopes"`
}

type Worker struct {
	Address     string `json:"address"`
	ProcessorID string `json:"processor_id"`
	Version     string `json:"processor_version"`
	Digest      string `json:"implementation_digest"`
	Service     string `json:"worker_service"`
}

type Config struct {
	HTTPAddr, RemoteHTTPAddr, ControlURL, ControlToken                 string
	InstallationID, Namespace, UIPath, RemotePath, RemoteOrigin        string
	Identities                                                         []Identity
	Workers                                                            []Worker
	BetaEnabled, InsecureDevelopment, BrokerTLS, ControlAllowPlaintext bool
	DispatchLimit                                                      int
	PollInterval                                                       time.Duration
	Brokers                                                            []string
	BrokerUsername, BrokerPassword, ConsumerGroup, BrokerCA            string
	WorkerCA, WorkerCert, WorkerKey, WorkerToken                       string
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("DQ_HTTP_ADDR", ":8080"), RemoteHTTPAddr: env("DQ_REMOTE_HTTP_ADDR", ":8081"),
		ControlURL: os.Getenv("DQ_CONTROL_URL"), ControlToken: os.Getenv("DQ_CONTROL_TOKEN"),
		InstallationID: os.Getenv("DQ_INSTALLATION_ID"), Namespace: os.Getenv("DQ_NAMESPACE"),
		UIPath: env("DQ_UI_DIR", "ui/dist"), RemotePath: env("DQ_REMOTE_DIR", "ui/remotes-dist"), RemoteOrigin: os.Getenv("DQ_REMOTE_ORIGIN"),
		BetaEnabled: os.Getenv("DQ_BETA_ENABLED") == "true", InsecureDevelopment: os.Getenv("DQ_INSECURE_DEVELOPMENT") == "true",
		ControlAllowPlaintext: os.Getenv("DQ_CONTROL_ALLOW_PLAINTEXT") == "true",
		BrokerTLS:             env("DQ_BROKER_TLS", "true") == "true", ConsumerGroup: env("DQ_CONSUMER_GROUP", "dq-manager-v1"),
		BrokerUsername: os.Getenv("DQ_BROKER_SASL_USERNAME"), BrokerPassword: os.Getenv("DQ_BROKER_SASL_PASSWORD"), BrokerCA: os.Getenv("DQ_BROKER_CA_PATH"),
		WorkerCA: os.Getenv("DQ_WORKER_CA_PATH"), WorkerCert: os.Getenv("DQ_WORKER_CERT_PATH"), WorkerKey: os.Getenv("DQ_WORKER_KEY_PATH"), WorkerToken: os.Getenv("DQ_WORKER_TOKEN")}
	var err error
	c.ControlToken, err = secret("DQ_CONTROL_TOKEN")
	if err != nil {
		return c, err
	}
	c.WorkerToken, err = secret("DQ_WORKER_TOKEN")
	if err != nil {
		return c, err
	}
	c.BrokerPassword, err = secret("DQ_BROKER_SASL_PASSWORD")
	if err != nil {
		return c, err
	}
	c.DispatchLimit, err = strconv.Atoi(env("DQ_DISPATCH_LIMIT", "32"))
	if err != nil || c.DispatchLimit < 1 || c.DispatchLimit > 100 {
		return c, errors.New("invalid dispatch limit")
	}
	c.PollInterval, err = time.ParseDuration(env("DQ_POLL_INTERVAL", "1s"))
	if err != nil || c.PollInterval < 100*time.Millisecond || c.PollInterval > time.Minute {
		return c, errors.New("invalid poll interval")
	}
	identities, err := secret("DQ_IDENTITIES_JSON")
	if err != nil {
		return c, err
	}
	if identities == "" {
		identities = "[]"
	}
	if err = json.Unmarshal([]byte(identities), &c.Identities); err != nil {
		return c, errors.New("invalid identities configuration")
	}
	if err = json.Unmarshal([]byte(env("DQ_WORKERS_JSON", "[]")), &c.Workers); err != nil {
		return c, errors.New("invalid approved workers configuration")
	}
	if b := os.Getenv("DQ_BROKERS"); b != "" {
		for _, broker := range strings.Split(b, ",") {
			if broker = strings.TrimSpace(broker); broker != "" {
				c.Brokers = append(c.Brokers, broker)
			}
		}
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	u, err := url.Parse(c.ControlURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !((c.InsecureDevelopment || c.ControlAllowPlaintext) && u.Scheme == "http")) {
		return errors.New("control URL requires HTTPS or explicit trusted-network approval")
	}
	if len(c.ControlToken) < 32 || c.InstallationID == "" || c.Namespace == "" {
		return errors.New("service authentication and installation configuration required")
	}
	if len(c.Identities) == 0 {
		return errors.New("at least one scoped identity required")
	}
	seen := map[string]bool{}
	for _, i := range c.Identities {
		if i.ID == "" || len(i.Token) < 32 || seen[i.Token] || i.InstallationID == "" || len(i.Namespaces) == 0 || len(i.Scopes) == 0 || len(i.Roles) == 0 {
			return errors.New("invalid scoped identity")
		}
		for _, r := range i.Roles {
			if r != "viewer" && r != "operator" && r != "admin" && r != "processor_owner" {
				return errors.New("unknown identity role")
			}
		}
		seen[i.Token] = true
	}
	for _, w := range c.Workers {
		if _, _, err := net.SplitHostPort(w.Address); err != nil || w.ProcessorID == "" || w.Version == "" || w.Digest == "" || w.Service == "" {
			return errors.New("invalid approved worker")
		}
	}
	if !c.InsecureDevelopment && (!c.BrokerTLS || c.WorkerCA == "" || c.WorkerCert == "" || c.WorkerKey == "") {
		return errors.New("production broker TLS and worker mutual TLS required")
	}
	if (c.BrokerUsername == "") != (c.BrokerPassword == "") {
		return errors.New("broker SASL credentials must be paired")
	}
	if !c.InsecureDevelopment && (c.BrokerUsername == "" || c.BrokerPassword == "") {
		return errors.New("scoped broker SASL credentials required")
	}
	if c.RemoteOrigin != "" {
		r, e := url.Parse(c.RemoteOrigin)
		if e != nil || r.Host == "" || r.User != nil || r.Path != "" || (r.Scheme != "https" && !(c.InsecureDevelopment && r.Scheme == "http")) {
			return errors.New("invalid remote origin")
		}
	}
	return nil
}

func env(k, fallback string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return fallback
}
func secret(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil || len(data) > 65536 {
			return "", errors.New("bounded secret file unavailable")
		}
		return strings.TrimSpace(string(data)), nil
	}
	return os.Getenv(name), nil
}
