package config

import (
	"strings"
	"testing"
)

func secureConfig() Config {
	return Config{ControlURL: "https://octopus.internal", ControlToken: strings.Repeat("a", 32), InstallationID: "test", Namespace: "ns", BrokerTLS: true, BrokerUsername: "dq", BrokerPassword: "secret", WorkerCA: "ca.pem", WorkerCert: "client.pem", WorkerKey: "client-key.pem", Identities: []Identity{{ID: "operator", Token: strings.Repeat("b", 32), Roles: []string{"operator"}, InstallationID: "test", Namespaces: []string{"ns"}, Scopes: []string{"floor-a"}}}}
}
func TestConfigFailsClosed(t *testing.T) {
	valid := secureConfig()
	if valid.Validate() != nil {
		t.Fatal(valid.Validate())
	}
	for _, change := range []func(*Config){func(c *Config) { c.Identities = nil }, func(c *Config) { c.ControlToken = "short" }, func(c *Config) { c.ControlURL = "http://octopus.internal" }, func(c *Config) { c.ControlURL = "https://user:secret@octopus.internal" }, func(c *Config) { c.BrokerTLS = false }, func(c *Config) { c.WorkerCA = "" }, func(c *Config) { c.BrokerPassword = "" }} {
		c := secureConfig()
		change(&c)
		if c.Validate() == nil {
			t.Fatal("unsafe config accepted")
		}
	}
}
func TestTrustedControlHTTPDoesNotDisableWorkerTLS(t *testing.T) {
	c := secureConfig()
	c.ControlURL = "http://octopus.internal:8081"
	c.ControlAllowPlaintext = true
	if c.Validate() != nil {
		t.Fatal(c.Validate())
	}
	c.WorkerCert = ""
	if c.Validate() == nil {
		t.Fatal("HTTP option disabled worker TLS")
	}
}

func TestDispatchLimitMatchesCoordinatorBound(t *testing.T) {
	t.Setenv("DQ_CONTROL_URL", "http://octopus.internal:8081")
	t.Setenv("DQ_CONTROL_TOKEN", strings.Repeat("a", 32))
	t.Setenv("DQ_CONTROL_TOKEN_FILE", "")
	t.Setenv("DQ_IDENTITIES_JSON_FILE", "")
	t.Setenv("DQ_IDENTITIES_JSON", `[{"id":"test","token":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","roles":["operator"],"installation_id":"test","namespaces":["ns"],"scopes":["floor-a"]}]`)
	t.Setenv("DQ_INSTALLATION_ID", "test")
	t.Setenv("DQ_NAMESPACE", "ns")
	t.Setenv("DQ_INSECURE_DEVELOPMENT", "true")
	for _, value := range []string{"1", "100", "101", "128", "0", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("DQ_DISPATCH_LIMIT", value)
			_, err := Load()
			valid := value == "1" || value == "100"
			if valid != (err == nil) {
				t.Fatalf("limit %s: %v", value, err)
			}
		})
	}
}
