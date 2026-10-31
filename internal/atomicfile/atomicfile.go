// Package atomicfile owns the durable file-replacement ritual shared by every
// persisted artifact in the project (checkpoint snapshots, the WAL manifest,
// the vector-forest cache, the server's meta.json identity file): write a
// uniquely-named temp file in the target's directory, fsync it, close, rename
// over the target, then best-effort fsync the directory so the rename itself
// is durable. A crash at any point leaves either the previous complete file
// or the new complete one — never a truncated mix.
//
// The ritual's invariants live here, once, instead of per call site: the temp
// file must sit in the same directory as the target (rename must not cross
// filesystems) and must not collide with another in-flight writer's temp
// (os.CreateTemp guarantees both), the file's data must hit disk before the
// rename, the rename must land before the caller believes the artifact
// persisted, and the temp file must not survive any failure branch.
package atomicfile

import (
	"io"
	"os"
	"path/filepath"
)

// Write atomically replaces path with whatever write produces: uniquely-named
// temp file (os.CreateTemp, mode 0600) in the same directory, write, fsync,
// close, rename, then a best-effort directory fsync (the rename is not
// durable on all filesystems until the parent directory is — checkpoint
// manifests point at these files). The temp file is removed on every failure
// branch. write's bytes are the file's final bytes: the helper changes where
// bytes land, never what they are — wire formats stay the call sites'
// property.
func Write(path string, write func(w io.Writer) error) error {
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "." // bare filename: the temp must sit next to the target, not in TMPDIR
	}
	f, err := os.CreateTemp(dir, base+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := write(f); err != nil {
		f.Close() // close error deliberately ignored: the dominant error is in hand and the temp is removed below
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close() // as above: the dominant error wins and the temp is removed below
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	// Best-effort: some filesystems reject directory fsync (EINVAL), and a
	// failure here does not undo the rename.
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}
