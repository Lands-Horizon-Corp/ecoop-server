package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

var _ StorageServices = (*StorageService)(nil)

// StorageService stores files in an S3-compatible bucket: AWS S3 (driver
// "s3"), MinIO, RustFS or any other server speaking the S3 API.
type StorageService struct {
	driver         string
	endpoint       string // host:port the service talks to
	publicEndpoint string // host:port presigned URLs are signed for ("" = endpoint)
	accessKey      string
	secretKey      string
	bucket         string
	region         string
	ssl            bool

	maxFileSize  int64
	urlTimeout   time.Duration
	allowPrivate bool // UploadFromURL may fetch private and local addresses

	client       atomic.Pointer[minio.Client]
	publicClient atomic.Pointer[minio.Client]
	lifecycle    sync.Mutex // serialises Run and Stop
	gate         *utils.Gate
}

// MaxFileSize caps every upload (DefaultMaxFileSize otherwise).
func MaxFileSize(n int64) Option { return func(s *StorageService) { s.maxFileSize = n } }

// URLTimeout bounds a whole UploadFromURL download (DefaultURLTimeout
// otherwise).
func URLTimeout(d time.Duration) Option { return func(s *StorageService) { s.urlTimeout = d } }

// AllowPrivateURLs lets UploadFromURL fetch private and local addresses.
// Off by default: a URL from a user could otherwise reach internal services.
func AllowPrivateURLs() Option { return func(s *StorageService) { s.allowPrivate = true } }

// NewStorageService creates the service. endpoint and publicEndpoint are
// host:port; publicEndpoint is where browsers reach the server, for
// presigned URLs ("" means endpoint). inflight may be shared with other
// services; nil gives the service its own tracker.
func NewStorageService(
	driver, endpoint, publicEndpoint, accessKey, secretKey, bucket, region string,
	ssl bool,
	inflight *utils.Inflight,
	opts ...Option,
) StorageServices {
	if region == "" {
		region = DefaultRegion
	}
	s := &StorageService{
		driver:         strings.ToLower(driver),
		endpoint:       endpoint,
		publicEndpoint: publicEndpoint,
		accessKey:      accessKey,
		secretKey:      secretKey,
		bucket:         bucket,
		region:         region,
		ssl:            ssl,
		maxFileSize:    DefaultMaxFileSize,
		urlTimeout:     DefaultURLTimeout,
		gate:           utils.NewGate(inflight),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Lifecycle

// Run connects to the server and creates the bucket if it does not exist.
// Calling it while running does nothing.
func (s *StorageService) Run(ctx context.Context) error {
	if s.gate == nil {
		return ErrNoTracker
	}
	var errs []error
	for _, missing := range []struct {
		v   string
		err error
	}{{s.endpoint, ErrNoEndpoint}, {s.accessKey, ErrNoAccessKey}, {s.secretKey, ErrNoSecretKey}, {s.bucket, ErrNoBucket}} {
		if strings.TrimSpace(missing.v) == "" {
			errs = append(errs, missing.err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.gate.IsOpen() {
		return nil
	}
	client, err := s.newClient(s.endpoint, s.ssl)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	public := client
	if s.publicEndpoint != "" {
		if public, err = s.newClient(s.publicEndpoint, true); err != nil {
			return errors.Join(ErrConnect, err)
		}
	}
	exists, err := client.BucketExists(ctx, s.bucket)
	if err != nil {
		return errors.Join(ErrConnect, err)
	}
	if !exists {
		err := client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: s.region})
		if err != nil && minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
			return errors.Join(ErrBucket, fmt.Errorf("creating %s: %w", s.bucket, err))
		}
	}
	s.client.Store(client)
	s.publicClient.Store(public)
	s.gate.Open()
	return nil
}

func (s *StorageService) newClient(endpoint string, secure bool) (*minio.Client, error) {
	lookup := minio.BucketLookupPath
	if s.driver == DriverS3 {
		lookup = minio.BucketLookupDNS
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(s.accessKey, s.secretKey, ""),
		Secure:       secure,
		Region:       s.region, // set, so presigning never asks the server
		BucketLookup: lookup,
	})
}

// Stop refuses new work and waits for running uploads and deletes, or
// returns ctx.Err() if ctx ends first.
func (s *StorageService) Stop(ctx context.Context) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	return s.gate.Close(ctx)
}

func (s *StorageService) Ping(ctx context.Context) error {
	client, done, err := s.ready()
	if err != nil {
		return err
	}
	defer done()
	if _, err := client.BucketExists(ctx, s.bucket); err != nil {
		return errors.Join(ErrConnect, err)
	}
	return nil
}

// ready enters the gate; the caller must call done when it finishes.
func (s *StorageService) ready() (*minio.Client, func(), error) {
	done, ok := s.gate.Enter()
	if !ok {
		return nil, nil, ErrNotStarted
	}
	client := s.client.Load()
	if client == nil {
		done()
		return nil, nil, ErrNotStarted
	}
	return client, done, nil
}

// Uploads

func (s *StorageService) Upload(ctx context.Context, file any, cb ProgressCallback) (*Storage, error) {
	switch f := file.(type) {
	case string:
		switch {
		case strings.HasPrefix(f, "http://"), strings.HasPrefix(f, "https://"):
			return s.UploadFromURL(ctx, f, cb)
		case strings.HasPrefix(f, "data:"):
			return s.UploadFromBase64(ctx, f, cb)
		default:
			return s.UploadFromPath(ctx, f, cb)
		}
	case []byte:
		return s.UploadFromBinary(ctx, f, cb)
	case *multipart.FileHeader:
		return s.UploadFromHeader(ctx, f, cb)
	case io.Reader:
		if utils.IsNil(f) {
			return nil, ErrNilReader
		}
		return s.UploadFromStream(ctx, f, -1, "", "", cb)
	}
	return nil, fmt.Errorf("%w (got %T)", ErrUnsupportedType, file)
}

func (s *StorageService) UploadFromPath(ctx context.Context, p string, cb ProgressCallback) (*Storage, error) {
	if err := utils.IsValidFilePath(p); err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return s.UploadFromStream(ctx, f, info.Size(), filepath.Base(p), mime.TypeByExtension(filepath.Ext(p)), cb)
}

var dataURI = regexp.MustCompile(`^data:([\w.+-]+/[\w.+-]+)?(;[\w-]+=[\w-]+)*;base64,`)

// UploadFromBase64 accepts a data URI ("data:image/png;base64,...") or bare
// base64 (standard or URL alphabet, padded or not).
func (s *StorageService) UploadFromBase64(ctx context.Context, data string, cb ProgressCallback) (*Storage, error) {
	contentType := ""
	if m := dataURI.FindStringSubmatch(data); m != nil {
		contentType, data = m[1], data[len(m[0]):]
	} else if strings.HasPrefix(data, "data:") {
		return nil, ErrInvalidBase64
	}
	data = strings.Join(strings.Fields(data), "")
	if data == "" {
		return nil, ErrEmptyFile
	}
	if int64(len(data))/4*3 > s.maxFileSize+3 {
		return nil, ErrTooLarge
	}
	var raw []byte
	var err error
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if raw, err = enc.DecodeString(data); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrInvalidBase64
	}
	return s.upload(ctx, bytes.NewReader(raw), int64(len(raw)), "", contentType, cb)
}

func (s *StorageService) UploadFromBinary(ctx context.Context, data []byte, cb ProgressCallback) (*Storage, error) {
	return s.upload(ctx, bytes.NewReader(data), int64(len(data)), "", "", cb)
}

func (s *StorageService) UploadFromHeader(ctx context.Context, header *multipart.FileHeader, cb ProgressCallback) (*Storage, error) {
	if header == nil {
		return nil, ErrNilHeader
	}
	f, err := header.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return s.UploadFromStream(ctx, f, header.Size, header.Filename, header.Header.Get("Content-Type"), cb)
}

// UploadFromURL downloads url and stores it. Only http and https are
// allowed, private and local addresses are refused (redirects included)
// unless AllowPrivateURLs, and the download is bounded by the size limit
// and URLTimeout.
func (s *StorageService) UploadFromURL(ctx context.Context, raw string, cb ProgressCallback) (*Storage, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, ErrInvalidURL
	}
	ctx, cancel := context.WithTimeout(ctx, s.urlTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, ErrInvalidURL
	}
	resp, err := s.httpClient().Do(req)
	if err != nil {
		if errors.Is(err, ErrBlockedAddress) {
			return nil, ErrBlockedAddress
		}
		return nil, errors.Join(ErrDownload, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%w: %s", ErrDownload, resp.Status)
	}
	if resp.ContentLength > s.maxFileSize {
		return nil, ErrTooLarge
	}
	name := path.Base(u.Path)
	if name == "/" || name == "." {
		name = ""
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	// The declared length is not trusted: the body is counted as it streams.
	return s.UploadFromStream(ctx, resp.Body, -1, name, contentType, cb)
}

// httpClient refuses connections to private and local addresses at dial
// time, after DNS, so a hostname or a redirect cannot smuggle one in.
func (s *StorageService) httpClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if !s.allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return ErrBlockedAddress
			}
			ip := net.ParseIP(host)
			if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
				ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
				return ErrBlockedAddress
			}
			return nil
		}
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:                 nil, // a proxy would dial for us, past the check
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}}
}

func (s *StorageService) UploadFromStream(ctx context.Context, r io.Reader, size int64, fileName, contentType string, cb ProgressCallback) (*Storage, error) {
	if utils.IsNil(r) {
		return nil, ErrNilReader
	}
	return s.upload(ctx, r, size, fileName, contentType, cb)
}

// upload stores r under a new unique key and reports progress to cb.
func (s *StorageService) upload(ctx context.Context, r io.Reader, size int64, fileName, contentType string, cb ProgressCallback) (*Storage, error) {
	client, done, err := s.ready()
	if err != nil {
		return nil, err
	}
	defer done()
	if size == 0 {
		return nil, ErrEmptyFile
	}
	if size > s.maxFileSize {
		return nil, ErrTooLarge
	}
	br := bufio.NewReaderSize(r, 512)
	head, err := br.Peek(512)
	if len(head) == 0 {
		if err == nil || errors.Is(err, io.EOF) {
			return nil, ErrEmptyFile
		}
		return nil, errors.Join(ErrUpload, err)
	}
	if contentType == "" {
		contentType = http.DetectContentType(head)
	}
	key, err := s.GenerateUniqueName(fileName, contentType)
	if err != nil {
		return nil, err
	}
	if fileName == "" {
		fileName = path.Base(key)
	}
	st := &Storage{
		FileName:   fileName,
		FileSize:   size,
		FileType:   contentType,
		StorageKey: key,
		URL:        s.objectURL(key),
		BucketName: s.bucket,
		Status:     StatusUploading,
	}
	var body io.Reader = br
	opts := minio.PutObjectOptions{ContentType: contentType, Progress: &progress{st: st, total: size, cb: cb}}
	if size < 0 {
		body = &limited{r: br, left: s.maxFileSize}
		opts.PartSize = UnknownPartSize
	}
	info, err := client.PutObject(ctx, s.bucket, key, body, size, opts)
	if err != nil {
		for _, known := range []error{ErrTooLarge, ErrCallback} {
			if errors.Is(err, known) {
				return nil, known
			}
		}
		return nil, errors.Join(ErrUpload, err)
	}
	st.FileSize, st.Progress, st.Status = info.Size, info.Size, StatusCompleted
	if err := report(cb, st.Progress, st.FileSize, st); err != nil {
		return nil, err
	}
	return st, nil
}

// progress receives what minio has sent, and reports it.
type progress struct {
	st    *Storage
	total int64
	cb    ProgressCallback
}

func (p *progress) Read(b []byte) (int, error) {
	p.st.Progress += int64(len(b))
	if err := report(p.cb, p.st.Progress, p.total, p.st); err != nil {
		return 0, err // stops the upload
	}
	return len(b), nil
}

// report calls cb. minio calls the progress reader from its own
// goroutines, where a panic cannot be recovered by the caller and would
// crash the process, so a panicking callback becomes ErrCallback instead.
func report(cb ProgressCallback, done, total int64, st *Storage) (err error) {
	if cb == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrCallback, r)
		}
	}()
	cb(done, total, st)
	return nil
}

// limited fails once more than left bytes have been read. The read that
// crosses the limit passes on no bytes at all: minio's part reader drops an
// error that comes with a full buffer, so an error with data could be lost
// and the upload would go on.
type limited struct {
	r    io.Reader
	left int64
}

func (l *limited) Read(b []byte) (int, error) {
	if l.left < 0 {
		return 0, ErrTooLarge
	}
	if int64(len(b)) > l.left+1 {
		b = b[:l.left+1]
	}
	n, err := l.r.Read(b)
	if l.left -= int64(n); l.left < 0 {
		return 0, ErrTooLarge
	}
	return n, err
}

// objectURL is the file's address on the public endpoint (or the endpoint).
// The bucket is private unless configured otherwise; GeneratePresignedURL
// gives a link that works.
func (s *StorageService) objectURL(key string) string {
	host, scheme := s.endpoint, "http"
	if s.ssl {
		scheme = "https"
	}
	if s.publicEndpoint != "" {
		host, scheme = s.publicEndpoint, "https"
	}
	u := url.URL{Scheme: scheme, Host: host, Path: "/" + s.bucket + "/" + key}
	if s.driver == DriverS3 {
		u.Host, u.Path = s.bucket+"."+host, "/"+key
	}
	return u.String()
}

// Files

func (s *StorageService) DeleteFile(ctx context.Context, st *Storage) error {
	if st == nil {
		return ErrNilStorage
	}
	if st.StorageKey == "" {
		return ErrNoKey
	}
	if st.BucketName != "" && st.BucketName != s.bucket {
		return ErrWrongBucket
	}
	client, done, err := s.ready()
	if err != nil {
		return err
	}
	defer done()
	if err := client.RemoveObject(ctx, s.bucket, st.StorageKey, minio.RemoveObjectOptions{}); err != nil {
		return errors.Join(ErrDelete, err)
	}
	return nil
}

func (s *StorageService) GeneratePresignedURL(ctx context.Context, st *Storage, expiry time.Duration) (string, error) {
	if st == nil {
		return "", ErrNilStorage
	}
	if st.StorageKey == "" {
		return "", ErrNoKey
	}
	if st.BucketName != "" && st.BucketName != s.bucket {
		return "", ErrWrongBucket
	}
	if expiry < time.Second || expiry > MaxPresignExpiry {
		return "", ErrInvalidExpiry
	}
	_, done, err := s.ready()
	if err != nil {
		return "", err
	}
	defer done()
	u, err := s.publicClient.Load().PresignedGetObject(ctx, s.bucket, st.StorageKey, expiry, nil)
	if err != nil {
		return "", errors.Join(ErrConnect, err)
	}
	return u.String(), nil
}

var (
	unsafeName = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
	plainExt   = regexp.MustCompile(`^\.[a-z0-9]{1,10}$`)
)

// GenerateUniqueName makes a storage key: a date folder, a random id and
// a cleaned-up version of the original name, with an extension from the
// name or, failing that, the content type. Path parts in original are
// dropped, so a name cannot climb out of its folder.
func (s *StorageService) GenerateUniqueName(original, contentType string) (string, error) {
	id := make([]byte, 12)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	base := path.Base(strings.ReplaceAll(original, `\`, "/"))
	ext := strings.ToLower(path.Ext(base))
	base = strings.TrimSuffix(base, path.Ext(base))
	base = strings.Trim(unsafeName.ReplaceAllString(base, "-"), "-.")
	if len(base) > 64 {
		base = base[:64]
	}
	if !plainExt.MatchString(ext) {
		ext = ""
		if mt, _, err := mime.ParseMediaType(contentType); err == nil {
			if exts, _ := mime.ExtensionsByType(mt); len(exts) > 0 {
				ext = exts[0]
			}
		}
	}
	name := hex.EncodeToString(id)
	if base != "" {
		name += "-" + base
	}
	return time.Now().UTC().Format("2006/01/02") + "/" + name + ext, nil
}

// RemoveAllFiles deletes every object in the service's bucket.
func (s *StorageService) RemoveAllFiles(ctx context.Context) error {
	client, done, err := s.ready()
	if err != nil {
		return err
	}
	defer done()
	objects := client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Recursive: true})
	var errs []error
	for e := range client.RemoveObjects(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
		errs = append(errs, e.Err)
	}
	if err := errors.Join(errs...); err != nil {
		return errors.Join(ErrDelete, err)
	}
	return nil
}
