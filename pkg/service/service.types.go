package service

import "io"

type SMTPRequest struct {
	Name     string
	To       string
	Subject  string
	Body     string
	Vars     map[string]string
	FromName string
}

type SMSRequest struct {
	To   string
	Body string
	Vars map[string]string
}

type Storage struct {
	FileName   string
	FileSize   int64
	FileType   string
	StorageKey string
	URL        string
	BucketName string
	Status     string
	Progress   int64
}

type ProgressCallback func(progress int64, total int64, storage *Storage)

type progressReader struct {
	reader    io.Reader
	callback  ProgressCallback
	total     int64
	readSoFar int64
	storage   *Storage
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.readSoFar += int64(n)
		percent := pr.readSoFar * 100 / pr.total
		percent = min(percent, 100)
		pr.storage.Progress = percent
		if pr.callback != nil {
			pr.callback(percent, 100, pr.storage)
		}
	}
	return n, err
}
