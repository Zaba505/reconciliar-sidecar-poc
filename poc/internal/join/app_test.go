package join_test

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/z5labs/humus"
	"github.com/z5labs/humus/grpc/grpcserver/grpcservertest"
	"github.com/z5labs/humus/humustest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/app"
	"poc/internal/join"
)

// TestApp_a_dropped_record_ends_the_Join_INTERNAL runs the App in-process
// and joins with the test hook set. It lives here, in package join_test,
// because the hook is unexported in package join.
func TestApp_a_dropped_record_ends_the_Join_INTERNAL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ca := humustest.NewCA(t)
		cert := ca.Issue(t, "localhost").TLS()
		key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{
			"cert.pem":   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}),
			"key.pem":    pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}),
			"data/.keep": nil,
		}))
		p, exit := humustest.Start(t, app.Init,
			humustest.WithApp(humus.WithHost(h.Host()), humus.WithTelemetry(humustest.NewTelemetry().Builder())),
			humustest.WithEnv(map[string]string{
				"RECON_LISTEN":             ":7443",
				"RECON_TLS_CERT_FILE":      h.Path("cert.pem"),
				"RECON_TLS_KEY_FILE":       h.Path("key.pem"),
				"RECON_DATA_DIR":           h.Path("data"),
				"RECON_MEMORY_LIMIT_BYTES": "64MiB",
				"RECON_DISK_LIMIT_BYTES":   "64MiB",
			}),
		)
		synctest.Wait()
		c := reconv1.NewReconcilerClient(grpcservertest.NewClient(t, h, "localhost:7443", ca.Pool()))
		ctx := t.Context()

		created, err := c.CreateRun(ctx, &reconv1.CreateRunRequest{BucketCount: 4})
		if err != nil {
			t.Fatal(err)
		}
		id := created.GetRunId()
		stream, err := c.Ingest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		records := &reconv1.RecordBatch{Records: []*reconv1.Record{{Key: []byte("a")}, {Key: []byte("b")}}}
		if err := stream.Send(&reconv1.IngestRequest{RunId: id, Batch: &reconv1.IngestRequest_Left{Left: records}}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.CloseAndRecv(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SealRun(ctx, &reconv1.SealRunRequest{RunId: id}); err != nil {
			t.Fatal(err)
		}

		join.DropRecord(t)
		js, err := c.Join(ctx, &reconv1.JoinRequest{RunId: id})
		if err != nil {
			t.Fatal(err)
		}
		for err == nil {
			_, err = js.Recv()
		}
		if errors.Is(err, io.EOF) || status.Code(err) != codes.Internal {
			t.Errorf("Join with a dropped record: %v, want INTERNAL", err)
		}

		p.Signal(syscall.SIGTERM)
		if st := <-exit; st.Code() != 0 {
			t.Errorf("exit %d: %v", st.Code(), st.Cause())
		}
	})
}
