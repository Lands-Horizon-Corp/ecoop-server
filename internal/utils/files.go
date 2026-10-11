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
