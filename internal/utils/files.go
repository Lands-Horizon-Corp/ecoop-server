package utils

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

var (
	ErrFileNotExist = errors.New("file does not exist")
	ErrFileIsDir    = errors.New("path is a directory, not a file")
)

func IsValidFilePath(p string) error {
	info, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %s", ErrFileNotExist, p)
	}
	if err != nil {
		return fmt.Errorf("failed to stat path %s: %w", p, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%w: %s", ErrFileIsDir, p)
	}
	return nil
}

// WriteNew creates file with content and fails if the file already exists. The
// file is 0644 on purpose: it is a migration or source file that is committed and
// read by other users and tools, not a secret.
func WriteNew(file, content string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // see above: not a secret
	if err != nil {
		return err
	}
	_, err = f.WriteString(content)
	return errors.Join(err, f.Close())
}
