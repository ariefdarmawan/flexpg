package flexpg

import (
	"net/url"
	"testing"

	"github.com/sebarcode/codekit"
)

func TestConnectionStringSupportsSSLRootAlias(t *testing.T) {
	conn := &Connection{}
	conn.Host = "database.example.com:5432"
	conn.Database = "tenant"
	conn.User = "tenant-user"
	conn.Password = "password/with@reserved:characters"
	conn.Config = codekit.M{
		"sslmode": "verify-full",
		"sslroot": "/etc/ssl/certs/global-bundle.pem",
	}

	parsed, err := url.Parse(conn.connectionString())
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	if parsed.User.Username() != conn.User {
		t.Fatalf("username = %q, want %q", parsed.User.Username(), conn.User)
	}
	password, ok := parsed.User.Password()
	if !ok || password != conn.Password {
		t.Fatalf("password was not URL encoded correctly")
	}
	if got := parsed.Query().Get("sslmode"); got != "verify-full" {
		t.Fatalf("sslmode = %q, want verify-full", got)
	}
	if got := parsed.Query().Get("sslrootcert"); got != "/etc/ssl/certs/global-bundle.pem" {
		t.Fatalf("sslrootcert = %q", got)
	}
	if got := parsed.Query().Get("sslroot"); got != "" {
		t.Fatalf("unsupported sslroot parameter was retained: %q", got)
	}
}

func TestConnectionStringKeepsExplicitSSLRootCert(t *testing.T) {
	conn := &Connection{}
	conn.Config = codekit.M{"sslroot": "alias.pem", "sslrootcert": "explicit.pem"}
	parsed, err := url.Parse(conn.connectionString())
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Query().Get("sslrootcert"); got != "explicit.pem" {
		t.Fatalf("sslrootcert = %q, want explicit.pem", got)
	}
}
