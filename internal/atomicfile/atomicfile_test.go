package atomicfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// onlyArtifact asserts dir holds exactly one entry, the target — no temp
// litter survives any outcome.
func onlyArtifact(t *testing.T, dir, name string) {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 1 || es[0].Name() != name {
		names := make([]string, len(es))
		for i, e := range es {
			names[i] = e.Name()
		}
		t.Fatalf("directory %s holds %v; want exactly [%s]", dir, names, name)
	}
}

func TestWriteReplacesAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.db")

	if err := Write(path, func(w io.Writer) error { _, err := io.WriteString(w, "first"); return err }); err != nil {
		t.Fatalf("Write(first): %v", err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "first" {
		t.Fatalf("after first write: %q, %v", b, err)
	}
	// Replace in place: the previous complete file is what a crash would leave.
	if err := Write(path, func(w io.Writer) error { _, err := io.WriteString(w, "second"); return err }); err != nil {
		t.Fatalf("Write(second): %v", err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "second" {
		t.Fatalf("after second write: %q, %v", b, err)
	}
	onlyArtifact(t, dir, "artifact.db")
}

func TestWriteCleansUpOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.db")
	boom := errors.New("boom")

	err := Write(path, func(io.Writer) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Write error = %v, want boom", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed write created the target: %v", err)
	}
	es, _ := os.ReadDir(dir)
	if len(es) != 0 {
		t.Fatalf("failed write left temp litter: %v", es)
	}

	// A pre-existing target survives a failed replacement untouched.
	if err := Write(path, func(w io.Writer) error { _, err := io.WriteString(w, "old"); return err }); err != nil {
		t.Fatalf("Write(old): %v", err)
	}
	if err := Write(path, func(io.Writer) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Write(fail) error = %v, want boom", err)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "old" {
		t.Fatalf("previous file damaged by failed write: %q, %v", b, err)
	}
	onlyArtifact(t, dir, "artifact.db")
}

func TestWriteTempLivesInTargetDirectory(t *testing.T) {
	// rename(2) must not cross filesystems; the temp file therefore sits
	// next to the target even when the caller passes a bare filename and
	// the process cwd differs from the target dir.
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "artifact.db")
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, func(w io.Writer) error { _, err := io.WriteString(w, "x"); return err }); err != nil {
		t.Fatalf("Write: %v", err)
	}
	onlyArtifact(t, filepath.Join(dir, "nested"), "artifact.db")
}

func TestWriteConcurrentSamePath(t *testing.T) {
	// The temp name is unique per writer (os.CreateTemp): two concurrent
	// Writes to one path never share a temp file, so both complete and the
	// survivor is one complete payload — no interleaved bytes, no litter.
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.db")
	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Go(func() {
			errs <- Write(path, func(w io.Writer) error {
				_, err := fmt.Fprintf(w, "writer-%d", i)
				return err
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.HasPrefix(s, "writer-") || len(s) > len("writer-")+2 {
		t.Fatalf("final artifact = %q; want one complete writer-N payload", s)
	}
	onlyArtifact(t, dir, "artifact.db")
}
