package app

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"maps"
	"testing"

	"github.com/z5labs/bedrock/bedrocktest"
	"github.com/z5labs/bedrock/config"
	"github.com/z5labs/humus"
	"github.com/z5labs/humus/humustest"
)

// testEnv is a host with a certificate, a data directory and a cgroup
// memory.max, and an environment that names them.
type testEnv struct {
	host   *humustest.Host
	ca     *humustest.CA
	env    map[string]string
	cgroup string
}

const testCgroupLimit = 1 << 30

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ca := humustest.NewCA(t)
	certPEM, keyPEM := pemPair(t, ca.Issue(t, "localhost").TLS())
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{
		"tls/server.pem":      certPEM,
		"tls/server-key.pem":  keyPEM,
		"data/.keep":          nil,
		"cgroup/memory.max":   []byte("1073741824\n"),
		"cgroup/nolimit.max":  []byte("max\n"),
		"tls/other-key.pem":   otherKey(t, ca),
		"tls/not-a-cert.pem":  []byte("not PEM"),
		"cgroup/garbage.max":  []byte("lots"),
		"cgroup/negative.max": []byte("-1"),
	}))
	return &testEnv{
		host: h,
		ca:   ca,
		env: map[string]string{
			"RECON_TLS_CERT_FILE":    h.Path("tls/server.pem"),
			"RECON_TLS_KEY_FILE":     h.Path("tls/server-key.pem"),
			"RECON_DATA_DIR":         h.Path("data"),
			"RECON_DISK_LIMIT_BYTES": "1GiB",
		},
		cgroup: h.Path("cgroup/memory.max"),
	}
}

// with returns a copy of the environment with changes applied; an empty
// value unsets the variable.
func (e *testEnv) with(changes map[string]string) map[string]string {
	env := maps.Clone(e.env)
	for k, v := range changes {
		if v == "" {
			delete(env, k)
			continue
		}
		env[k] = v
	}
	return env
}

func (e *testEnv) setup(t *testing.T, env map[string]string) *humus.Setup {
	t.Helper()
	return &humus.Setup{
		Process: bedrocktest.NewProcess(t, bedrocktest.WithEnv(env)).Process(),
		Host:    e.host.Host(),
	}
}

// pemPair encodes cert's chain and key as PEM.
func pemPair(t *testing.T, cert tls.Certificate) (certPEM, keyPEM []byte) {
	t.Helper()
	for _, der := range cert.Certificate {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	return certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// otherKey returns the PEM key of a certificate other than the server's.
func otherKey(t *testing.T, ca *humustest.CA) []byte {
	t.Helper()
	_, key := pemPair(t, ca.Issue(t, "other").TLS())
	return key
}

// failedNames returns the names of every *config.Error in err's tree.
func failedNames(err error) []string {
	var names []string
	var walk func(error)
	walk = func(err error) {
		if j, ok := err.(interface{ Unwrap() []error }); ok {
			for _, e := range j.Unwrap() {
				walk(e)
			}
			return
		}
		if ce, ok := errors.AsType[*config.Error](err); ok {
			names = append(names, ce.Name)
		}
	}
	walk(err)
	return names
}
