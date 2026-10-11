package qr

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

// QRService is safe for concurrent use. Every Encode and Decode is tracked on
// inflight while it runs; Stop refuses new calls, waits for the tracked ones
// to finish, and only then releases the codecs.
type QRService struct {
	codecs   atomic.Pointer[codecs]
	inflight *utils.Inflight
	secret   string
}

// codecs is everything Start creates and Stop releases.
type codecs struct {
	aead    cipher.AEAD
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func (c *codecs) close() {
	_ = c.encoder.Close()
	c.decoder.Close()
}

// NewQRServices creates the service. inflight may be shared with other
// services; nil gives the service its own tracker.
func NewQRServices(secret string, inflight *utils.Inflight) QRServices {
	if inflight == nil {
		inflight = utils.NewInflight()
	}
	return &QRService{
		secret:   secret,
		inflight: inflight,
	}
}

func (q *QRService) Start(ctx context.Context) error {
	if q.inflight == nil {
		return ErrNoTracker
	}
	key := sha256.Sum256([]byte(q.secret))
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		return fmt.Errorf("qr: creating cipher: %w", err)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.WithDecoderMaxMemory(maxDecodedSize),
	)
	if err != nil {
		return fmt.Errorf("qr: creating decoder: %w", err)
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		decoder.Close()
		return fmt.Errorf("qr: creating encoder: %w", err)
	}
	if old := q.codecs.Swap(&codecs{aead: aead, encoder: encoder, decoder: decoder}); old != nil {
		// Calls still using the old codecs finish before they are released.
		go q.releaseWhenDrained(old)
	}
	return nil
}

// Stop rejects new calls with ErrNotStarted and waits for in-flight ones to
// finish before releasing the codecs. If ctx ends first it returns an error;
// the codecs are then released in the background once those calls finish,
// never out from under them.
func (q *QRService) Stop(ctx context.Context) error {
	old := q.codecs.Swap(nil)
	if old == nil {
		return nil
	}
	if err := q.inflight.Wait(ctx); err != nil {
		go q.releaseWhenDrained(old)
		return fmt.Errorf("qr: stop: in-flight calls still running: %w", err)
	}
	old.close()
	return nil
}

func (q *QRService) releaseWhenDrained(c *codecs) {
	_ = q.inflight.Wait(context.Background())
	c.close()
}

// acquire tracks a call on the current codecs. Callers must call done when
// the call finishes.
//
// It registers before loading the codecs: any call that sees live codecs is
// already counted, so Stop (which swaps them out, then waits) cannot release
// them underneath it.
func (q *QRService) acquire() (c *codecs, done func(), err error) {
	if q.codecs.Load() == nil {
		return nil, nil, ErrNotStarted // fast path: never touches the tracker
	}
	done = q.inflight.Track()
	if c = q.codecs.Load(); c == nil {
		done()
		return nil, nil, ErrNotStarted
	}
	return c, done, nil
}

func (q *QRService) Decode(ctx context.Context, encodedStr string) (QRData, error) {
	c, done, err := q.acquire()
	if err != nil {
		return QRData{}, err
	}
	defer done()
	decodedStr, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		return QRData{}, err
	}
	nonceSize := c.aead.NonceSize()
	if len(decodedStr) < nonceSize {
		return QRData{}, ErrTooShort
	}
	nonce, cipherText := decodedStr[:nonceSize], decodedStr[nonceSize:]
	decryptedData, err := c.aead.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return QRData{}, err
	}
	decompressedStr, err := c.decoder.DecodeAll(decryptedData, nil)
	if err != nil {
		return QRData{}, err
	}
	var decodedQRData QRData
	if err = sonic.Unmarshal(decompressedStr, &decodedQRData); err != nil {
		return QRData{}, err
	}
	return decodedQRData, nil
}

func (q *QRService) Encode(ctx context.Context, data *QRData) (string, error) {
	if data == nil {
		return "", ErrNilData
	}
	if !utf8.ValidString(data.Data) || !utf8.ValidString(data.Type) {
		return "", ErrInvalidUTF8
	}
	c, done, err := q.acquire()
	if err != nil {
		return "", err
	}
	defer done()
	marshalled, err := sonic.Marshal(data)
	if err != nil {
		return "", err
	}
	if len(marshalled) > maxDecodedSize {
		return "", ErrTooLarge
	}
	compressed := c.encoder.EncodeAll(marshalled, make([]byte, 0, len(marshalled)))
	nonceSize := c.aead.NonceSize()
	output := make([]byte, nonceSize, nonceSize+len(compressed)+c.aead.Overhead())
	if _, err := io.ReadFull(rand.Reader, output); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(
		c.aead.Seal(output, output, compressed, nil),
	), nil
}
