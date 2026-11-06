package localexecution

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

const (
	maxArchiveSize int64 = 512 << 20
	maxFileSize    int64 = 128 << 20
	maxEntries           = 50000
)

type SourceFile struct {
	Path         string
	Size         int64
	OriginalMode int64
	UploadedMode int64
	offset       int64
}

type Snapshot struct {
	Root          string
	Head          string
	CommitterDate string
	Dirty         bool
	Digest        string
	Bytes         int64
	Files         []SourceFile
	Deleted       []string
	Warnings      []string
	archive       *os.File
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LC_ALL=C", "LANG=C"}
	b, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return string(b), nil
}

func Capture(ctx context.Context, source string, capacity int64) (_ *Snapshot, err error) {
	rootPath, err := filepath.Abs(source)
	if err != nil {
		return nil, err
	}
	rootPath, err = filepath.EvalSymlinks(rootPath)
	if err != nil {
		return nil, err
	}
	top, err := gitOutput(ctx, rootPath, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("source must be a Git working tree: %w", err)
	}
	topPath, err := filepath.EvalSymlinks(strings.TrimSpace(top))
	if err != nil || topPath != rootPath {
		return nil, fmt.Errorf("--source must be the Git working tree root (%s)", strings.TrimSpace(top))
	}
	head, err := gitOutput(ctx, rootPath, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("source needs an existing HEAD commit: %w", err)
	}
	committerDate, err := gitOutput(ctx, rootPath, "show", "-s", "--format=%ct", strings.TrimSpace(head))
	if err != nil {
		return nil, fmt.Errorf("read source commit timestamp: %w", err)
	}
	index, err := gitOutput(ctx, rootPath, "ls-files", "--stage", "-z")
	if err != nil {
		return nil, err
	}
	for entry := range strings.SplitSeq(index, "\x00") {
		if strings.HasPrefix(entry, "160000 ") {
			return nil, fmt.Errorf("submodules are unsupported: %s", strings.SplitN(entry, "\t", 2)[1])
		}
	}
	listing, err := gitOutput(ctx, rootPath, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	status, err := gitOutput(ctx, rootPath, "status", "--porcelain", "-z", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	archive, err := os.CreateTemp("", "tkn-local-source-*.tar")
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{Root: rootPath, Dirty: status != "", archive: archive}
	snap.Head = strings.TrimSpace(head)
	snap.CommitterDate = strings.TrimSpace(committerDate)
	defer func() {
		if err != nil {
			err = errors.Join(err, snap.Close())
		}
	}()
	tempRoot, err := filepath.EvalSymlinks(filepath.Dir(archive.Name()))
	if err != nil {
		return nil, err
	}
	relative, relErr := filepath.Rel(rootPath, tempRoot)
	if relErr == nil && (relative == "." || filepath.IsLocal(relative)) {
		return nil, fmt.Errorf("temporary archive must be outside --source; set TMPDIR to an external directory")
	}
	limit := min(maxArchiveSize, capacity/2)
	if limit <= 1024 {
		return nil, fmt.Errorf("storage size leaves no room for source")
	}
	hash := sha256.New()
	bounded := &archiveWriter{writer: io.MultiWriter(archive, hash), limit: limit}
	tw := tar.NewWriter(bounded)
	seen := map[string]bool{}
	dirs := map[string]bool{}
	names := strings.Split(strings.TrimSuffix(listing, "\x00"), "\x00")
	slices.Sort(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if hasGitComponent(name) {
			continue
		}
		if !validPath(name) {
			return nil, fmt.Errorf("unsafe source path %q", name)
		}
		if len(seen) > maxEntries {
			return nil, fmt.Errorf("source exceeds %d entries", maxEntries)
		}
		info, statErr := root.Lstat(name)
		if os.IsNotExist(statErr) {
			snap.Deleted = append(snap.Deleted, name)
			continue
		}
		if statErr != nil {
			return nil, statErr
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s: only regular files are supported (symlinks and special files are rejected)", name)
		}
		parents := strings.Split(path.Dir(name), "/")
		parent := ""
		parentInfo := map[string]os.FileInfo{}
		for _, part := range parents {
			if part == "." {
				continue
			}
			parent = path.Join(parent, part)
			d, err := root.Lstat(parent)
			if err != nil {
				return nil, err
			}
			if !d.IsDir() || d.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("%s: symlink or non-directory parent", parent)
			}
			parentInfo[parent] = d
			if !dirs[parent] {
				if len(dirs)+len(snap.Files) >= maxEntries {
					return nil, fmt.Errorf("source exceeds %d archive entries", maxEntries)
				}
				// Keep new files in the shared group when Tasks use different primary groups
				if err := tw.WriteHeader(&tar.Header{Name: parent + "/", Typeflag: tar.TypeDir, Mode: int64(d.Mode().Perm() | 0o2070)}); err != nil {
					return nil, err
				}
				dirs[parent] = true
			}
		}
		if len(dirs)+len(snap.Files) >= maxEntries {
			return nil, fmt.Errorf("source exceeds %d archive entries", maxEntries)
		}
		if info.Size() > maxFileSize {
			return nil, fmt.Errorf("%s exceeds the 128 MiB file limit", name)
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		entry, warning, err := captureFile(tw, bounded, f, name, info, root)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for parent, before := range parentInfo {
			current, err := root.Lstat(parent)
			if err != nil || !current.IsDir() || !os.SameFile(before, current) {
				return nil, fmt.Errorf("source directory %q changed during capture; retry", parent)
			}
		}
		snap.Files = append(snap.Files, entry)
		snap.Bytes += entry.Size
		if warning != "" {
			snap.Warnings = append(snap.Warnings, warning)
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	endHead, err := gitOutput(ctx, rootPath, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(endHead) != snap.Head {
		return nil, fmt.Errorf("HEAD changed during source capture; retry")
	}
	snap.Digest = hex.EncodeToString(hash.Sum(nil))
	return snap, nil
}

type archiveWriter struct {
	writer       io.Writer
	count, limit int64
}

func (w *archiveWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.limit-w.count {
		return 0, fmt.Errorf("source archive exceeds %d bytes (512 MiB or half the PVC capacity)", w.limit)
	}
	n, err := w.writer.Write(b)
	w.count += int64(n)
	return n, err
}

func captureFile(tw *tar.Writer, w *archiveWriter, f *os.File, name string, before os.FileInfo, root *os.Root) (SourceFile, string, error) {
	entry := SourceFile{Path: name, Size: before.Size(), OriginalMode: int64(before.Mode().Perm())}
	entry.UploadedMode = entry.OriginalMode | 0o060
	if entry.OriginalMode&0o111 != 0 {
		entry.UploadedMode |= 0o010
	}
	opened, err := f.Stat()
	if err != nil {
		return entry, "", err
	}
	if !os.SameFile(before, opened) || !opened.Mode().IsRegular() {
		return entry, "", fmt.Errorf("%s changed during capture; retry", name)
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: entry.UploadedMode, Size: entry.Size, Typeflag: tar.TypeReg}); err != nil {
		return entry, "", err
	}
	entry.offset = w.count
	if _, err := io.CopyN(tw, f, entry.Size); err != nil {
		return entry, "", fmt.Errorf("capture %s: %w", name, err)
	}
	after, err := f.Stat()
	if err != nil {
		return entry, "", err
	}
	current, err := root.Lstat(name)
	if err != nil || !os.SameFile(before, current) || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || before.Mode() != after.Mode() {
		return entry, "", fmt.Errorf("%s changed during capture; retry", name)
	}
	var prefix [128]byte
	n, _ := f.ReadAt(prefix[:], 0)
	warning := ""
	if strings.HasPrefix(string(prefix[:n]), "version https://git-lfs.github.com/spec/v1\n") {
		warning = name + ": uploading a Git LFS pointer; materialize it before retrying if the object is needed"
	}
	return entry, warning, nil
}

func hasGitComponent(name string) bool { return slices.Contains(strings.Split(name, "/"), ".git") }
func validPath(name string) bool {
	return name != "." && filepath.IsLocal(name) && path.Clean(name) == name && !strings.Contains(name, "\\")
}

func (s *Snapshot) ReadFile(name string) ([]byte, error) {
	name = strings.TrimPrefix(name, "./")
	if !validPath(name) || hasGitComponent(name) {
		return nil, fmt.Errorf("unsafe repository path %q", name)
	}
	for _, f := range s.Files {
		if f.Path == name {
			return io.ReadAll(io.NewSectionReader(s.archive, f.offset, f.Size))
		}
	}
	return nil, fmt.Errorf("%s is absent from the captured source (check git check-ignore)", name)
}

func (s *Snapshot) Archive() (io.Reader, error) {
	if _, err := s.archive.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return s.archive, nil
}

func (s *Snapshot) Close() error {
	name := s.archive.Name()
	returnErr := s.archive.Close()
	if err := os.Remove(name); returnErr == nil {
		returnErr = err
	}
	return returnErr
}
