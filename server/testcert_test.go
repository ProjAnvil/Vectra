package server

// Throwaway PKI for TLS tests (Task 1: HTTP plane, Task 2: raft plane).
// All material is crypto/ecdsa P-256 with 1h validity, generated into a
// single t.TempDir(); TempDir cleanup is the only teardown (t.Cleanup: none).

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// certPaths collects the files genTLSCerts generated.
type certPaths struct {
	CA         string
	CAKey      string
	ServerCert string
	ServerKey  string
	ClientCert string
	ClientKey  string
}

// genTLSCerts generates into t.TempDir(): a CA (ca.pem/ca-key.pem), a server
// leaf (server.pem/server-key.pem, SANs: 127.0.0.1, localhost) and a client
// leaf (client.pem/client-key.pem), then returns their paths.
func genTLSCerts(t *testing.T) certPaths {
	t.Helper()
	dir := t.TempDir()

	// CA (self-signed)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genTLSCerts: generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vectra-test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("genTLSCerts: create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("genTLSCerts: parse CA cert: %v", err)
	}
	writeTestPEM(t, filepath.Join(dir, "ca.pem"), "CERTIFICATE", caDER)
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		t.Fatalf("genTLSCerts: marshal CA key: %v", err)
	}
	writeTestPEM(t, filepath.Join(dir, "ca-key.pem"), "EC PRIVATE KEY", caKeyDER)

	// Server leaf: SANs 127.0.0.1 + localhost so Go clients accept it.
	serverKey, serverCert := genTestLeaf(t, caCert, caKey, big.NewInt(2),
		pkix.Name{CommonName: "localhost"},
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")},
		x509.ExtKeyUsageServerAuth)
	writeTestPEM(t, filepath.Join(dir, "server.pem"), "CERTIFICATE", serverCert)
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("genTLSCerts: marshal server key: %v", err)
	}
	writeTestPEM(t, filepath.Join(dir, "server-key.pem"), "EC PRIVATE KEY", serverKeyDER)

	// Client leaf for mTLS.
	clientKey, clientCert := genTestLeaf(t, caCert, caKey, big.NewInt(3),
		pkix.Name{CommonName: "vectra-test-client"}, nil, nil,
		x509.ExtKeyUsageClientAuth)
	writeTestPEM(t, filepath.Join(dir, "client.pem"), "CERTIFICATE", clientCert)
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("genTLSCerts: marshal client key: %v", err)
	}
	writeTestPEM(t, filepath.Join(dir, "client-key.pem"), "EC PRIVATE KEY", clientKeyDER)

	return certPaths{
		CA:         filepath.Join(dir, "ca.pem"),
		CAKey:      filepath.Join(dir, "ca-key.pem"),
		ServerCert: filepath.Join(dir, "server.pem"),
		ServerKey:  filepath.Join(dir, "server-key.pem"),
		ClientCert: filepath.Join(dir, "client.pem"),
		ClientKey:  filepath.Join(dir, "client-key.pem"),
	}
}

// genTestLeaf issues one leaf certificate under the given CA.
func genTestLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey,
	serial *big.Int, subject pkix.Name, dns []string, ips []net.IP, eku x509.ExtKeyUsage,
) (*ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genTLSCerts: generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("genTLSCerts: create leaf cert: %v", err)
	}
	return key, der
}

func writeTestPEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("genTLSCerts: write %s: %v", path, err)
	}
}
