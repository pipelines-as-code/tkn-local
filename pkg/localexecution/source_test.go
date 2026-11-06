package localexecution

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func gitTest(t *testing.T, root string, args ...string) string {
	t.Helper()
	output, err := gitOutput(t.Context(), root, append([]string{"-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "-c", "user.name=Test", "-c", "user.email=test@example.com"}, args...)...)
	assert.NilError(t, err)
	return strings.TrimSpace(output)
}

func testRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	gitTest(t, root, "init")
	writeSource(t, root, "tracked", "committed", 0o640)
	gitTest(t, root, "add", "tracked")
	gitTest(t, root, "commit", "-m", "fixture")
	return root
}

func writeSource(t *testing.T, root, name, content string, mode os.FileMode) {
	t.Helper()
	filename := filepath.Join(root, name)
	assert.NilError(t, os.MkdirAll(filepath.Dir(filename), 0o700))
	assert.NilError(t, os.WriteFile(filename, []byte(content), mode))
	assert.NilError(t, os.Chmod(filename, mode))
}

func TestCapture(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*testing.T, string)
		capacity     int64
		sourceSubdir string
		wantErr      string
	}{
		{name: "working tree selection", setup: func(t *testing.T, root string) {
			t.Helper()
			writeSource(t, root, "deleted", "deleted", 0o600)
			writeSource(t, root, "ignored-tracked", "tracked despite ignore", 0o600)
			gitTest(t, root, "add", "deleted", "ignored-tracked")
			gitTest(t, root, "commit", "-m", "more files")
			writeSource(t, root, ".gitignore", "ignored*\n", 0o600)
			writeSource(t, root, "ignored-secret", "excluded", 0o600)
			writeSource(t, root, "tracked", "staged", 0o640)
			gitTest(t, root, "add", "tracked")
			writeSource(t, root, "tracked", "working tree", 0o640)
			writeSource(t, root, "untracked", "new source", 0o600)
			writeSource(t, root, "dir/script", "#!/bin/sh\nexit 0\n", 0o700)
			assert.NilError(t, os.Remove(filepath.Join(root, "deleted")))
		}},
		{name: "symlink", wantErr: "symlinks", setup: func(t *testing.T, root string) {
			t.Helper()
			assert.NilError(t, os.Symlink("tracked", filepath.Join(root, "link")))
		}},
		{name: "symlink parent", wantErr: "symlink", setup: func(t *testing.T, root string) {
			t.Helper()
			writeSource(t, root, "dir/file", "inside", 0o600)
			gitTest(t, root, "add", "dir/file")
			assert.NilError(t, os.RemoveAll(filepath.Join(root, "dir")))
			outside := t.TempDir()
			writeSource(t, outside, "file", "outside", 0o600)
			assert.NilError(t, os.Symlink(outside, filepath.Join(root, "dir")))
		}},
		{name: "submodule", wantErr: "submodules", setup: func(t *testing.T, root string) {
			t.Helper()
			sha := gitTest(t, root, "rev-parse", "HEAD")
			gitTest(t, root, "update-index", "--add", "--cacheinfo", "160000,"+sha+",submodule")
		}},
		{name: "file limit", wantErr: "file limit", setup: func(t *testing.T, root string) {
			t.Helper()
			file, err := os.Create(filepath.Join(root, "large"))
			assert.NilError(t, err)
			assert.NilError(t, file.Truncate(maxFileSize+1))
			assert.NilError(t, file.Close())
		}},
		{name: "PVC capacity reserve", wantErr: "source archive exceeds", capacity: 4096, setup: func(t *testing.T, root string) {
			t.Helper()
			writeSource(t, root, "large", strings.Repeat("x", 2048), 0o600)
		}},
		{name: "source subdirectory", sourceSubdir: "sub", wantErr: "working tree root", setup: func(t *testing.T, root string) {
			t.Helper()
			assert.NilError(t, os.Mkdir(filepath.Join(root, "sub"), 0o700))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := testRepository(t)
			tt.setup(t, root)
			capacity := tt.capacity
			if capacity == 0 {
				capacity = 1 << 30
			}
			source := root
			if tt.sourceSubdir != "" {
				source = filepath.Join(root, tt.sourceSubdir)
			}
			snap, err := Capture(t.Context(), source, capacity)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			archiveName := snap.archive.Name()
			t.Cleanup(func() {
				if _, err := os.Stat(archiveName); err == nil {
					_ = snap.Close()
				}
			})
			assert.Assert(t, snap.Dirty)
			writeSource(t, root, "tracked", "changed after capture", 0o640)
			captured, err := snap.ReadFile("tracked")
			assert.NilError(t, err)
			assert.Equal(t, "working tree", string(captured))
			_, err = snap.ReadFile("ignored-secret")
			assert.ErrorContains(t, err, "absent")
			_, err = snap.ReadFile("deleted")
			assert.ErrorContains(t, err, "absent")
			_, err = snap.ReadFile("../tracked")
			assert.ErrorContains(t, err, "unsafe")
			_, err = snap.ReadFile(".git/config")
			assert.ErrorContains(t, err, "unsafe")
			content, err := snap.ReadFile("ignored-tracked")
			assert.NilError(t, err)
			assert.Equal(t, "tracked despite ignore", string(content))
			assert.DeepEqual(t, snap.Deleted, []string{"deleted"})
			archive, err := snap.Archive()
			assert.NilError(t, err)
			data, err := io.ReadAll(archive)
			assert.NilError(t, err)
			digest := sha256.Sum256(data)
			assert.Equal(t, hex.EncodeToString(digest[:]), snap.Digest)
			reader := tar.NewReader(strings.NewReader(string(data)))
			foundExecutable := false
			for {
				header, err := reader.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				assert.NilError(t, err)
				assert.Assert(t, !hasGitComponent(header.Name))
				assert.Equal(t, 0, header.Uid)
				assert.Equal(t, 0, header.Gid)
				if header.Typeflag == tar.TypeDir {
					assert.Assert(t, header.Mode&0o2070 == 0o2070)
				}
				if header.Name == "dir/script" {
					foundExecutable = true
					assert.Equal(t, int64(0o770), header.Mode)
				}
			}
			assert.Assert(t, foundExecutable)
			assert.NilError(t, snap.Close())
			_, err = os.Stat(archiveName)
			assert.Assert(t, os.IsNotExist(err))
		})
	}
}
