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
