package harness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// The file names WriteCerts writes, and the directory the harness mounts
// them on in the container.
const (
	CAFile        = "ca.pem"
	CertFile      = "server.pem"
	KeyFile       = "server-key.pem"
	ContainerTLS  = "/etc/recon/tls"
	certValidity  = 30 * 24 * time.Hour
	certBackdated = time.Hour
)

// Certs is a CA and a server certificate for localhost, written to Dir.
type Certs struct {
	Dir  string
	Pool *x509.CertPool // holds only the CA
}

// WriteCerts generates a CA and a server certificate for localhost,
// 127.0.0.1 and ::1, signed by it, and writes CAFile, CertFile and KeyFile
// into dir, which it creates.
func WriteCerts(dir string) (*Certs, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	now := time.Now()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "recon-sidecar test CA"},
		NotBefore:             now.Add(-certBackdated),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-certBackdated),
		NotAfter:     now.Add(certValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}

	files := []struct {
		name  string
		block *pem.Block
		perm  os.FileMode
	}{
		{CAFile, &pem.Block{Type: "CERTIFICATE", Bytes: caDER}, 0o644},
		{CertFile, &pem.Block{Type: "CERTIFICATE", Bytes: der}, 0o644},
		{KeyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}, 0o600},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), pem.EncodeToMemory(f.block), f.perm); err != nil {
			return nil, fmt.Errorf("write %s: %w", f.name, err)
		}
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &Certs{Dir: dir, Pool: pool}, nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err) // crypto/rand never fails on Linux
	}
	return n
}
