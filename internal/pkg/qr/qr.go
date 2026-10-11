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
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
)

type QRService struct {
	decoder *zstd.Decoder
	encoder *zstd.Encoder
	aead    cipher.AEAD
	secret  string
}

func NewQRServices(secret string) QRServices {
	return &QRService{
		secret: secret,
	}
}

func (q *QRService) Start(ctx context.Context) error {
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
	q.closeCodecs()
	q.aead = aead
	q.decoder = decoder
	q.encoder = encoder
	return nil
}

func (q *QRService) Stop(ctx context.Context) error {
	q.closeCodecs()
	q.aead = nil
	return nil
}

// closeCodecs releases the current codecs.
func (q *QRService) closeCodecs() {
	if q.encoder != nil {
		_ = q.encoder.Close()
		q.encoder = nil
	}
	if q.decoder != nil {
		q.decoder.Close()
		q.decoder = nil
	}
}

func (q *QRService) Decode(ctx context.Context, encodedStr string) (QRData, error) {
	if q.aead == nil {
		return QRData{}, ErrNotStarted
	}
	decodedStr, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		return QRData{}, err
	}
	nonceSize := q.aead.NonceSize()
	if len(decodedStr) < nonceSize {
		return QRData{}, ErrTooShort
	}
	nonce, cipherText := decodedStr[:nonceSize], decodedStr[nonceSize:]
	decryptedData, err := q.aead.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return QRData{}, err
	}
	decompressedStr, err := q.decoder.DecodeAll(decryptedData, nil)
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
	if q.aead == nil {
		return "", ErrNotStarted
	}
	marshalled, err := sonic.Marshal(data)
	if err != nil {
		return "", err
	}
	if len(marshalled) > maxDecodedSize {
		return "", ErrTooLarge
	}
	compressed := q.encoder.EncodeAll(marshalled, make([]byte, 0, len(marshalled)))
	nonceSize := q.aead.NonceSize()
	output := make([]byte, nonceSize, nonceSize+len(compressed)+q.aead.Overhead())
	if _, err := io.ReadFull(rand.Reader, output); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(
		q.aead.Seal(output, output, compressed, nil),
	), nil
}
