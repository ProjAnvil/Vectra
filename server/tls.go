package server

// TLS material loading for the HTTP plane (Task 1) and the raft plane
// (Task 2). Certificates are loaded once at New and fail fast: an unreadable
// or malformed file aborts startup with an error naming the path — never a
// silent fallback to plaintext.

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// buildHTTPTLSConfig returns a *tls.Config with the leaf pair loaded and
// (when clientCA != "") ClientCAs populated + ClientAuth = RequireAndVerifyClientCert.
// MinVersion: TLS 1.2. Any unreadable/malformed file → error naming the path.
func buildHTTPTLSConfig(certFile, keyFile, clientCA string) (*tls.Config, error) {
	cert, err := loadTLSKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if clientCA != "" {
		pool, err := loadTLSCAPool(clientCA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// buildNodeTLSConfig returns a *tls.Config usable as BOTH server and client
// (raft peers dial each other): leaf pair + RootCAs=ClientCAs=ca +
// ClientAuth=RequireAndVerifyClientCert. MinVersion: TLS 1.2.
func buildNodeTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := loadTLSKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadTLSCAPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// loadTLSKeyPair loads a PEM cert/key pair. The wrapper names both files:
// crypto/tls's own errors ("failed to find any PEM data") would otherwise not
// say which file was bad.
func loadTLSKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: load cert %s / key %s: %w", certFile, keyFile, err)
	}
	return cert, nil
}

// loadTLSCAPool reads a PEM CA bundle into a certificate pool.
func loadTLSCAPool(caFile string) (*x509.CertPool, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("tls: CA file %s contains no valid PEM certificates", caFile)
	}
	return pool, nil
}
