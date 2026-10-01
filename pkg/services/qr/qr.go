package qr

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"runtime"

	"github.com/bytedance/sonic"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
)

type QRService struct {
	decoder *zstd.Decoder
	encoder *zstd.Encoder
	aead    cipher.AEAD
}

func NewQRService(secret string) QRServices {
	key := sha256.Sum256([]byte(secret))
	aead, err := chacha20poly1305.NewX(key[:])
	if err != nil {
		panic(err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		panic(err)
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		decoder.Close()
		encoder.Close()
		panic(err)
	}
	return &QRService{
		decoder: decoder,
		encoder: encoder,
		aead:    aead,
	}
}

// Close releases the zstd encoder/decoder. Call it on shutdown.
func (q *QRService) Close() {
	q.encoder.Close()
	q.decoder.Close()
}

func (q *QRService) Encode(ctx context.Context, data *QRData) (string, error) {
	marshalled, err := sonic.Marshal(data)
	if err != nil {
		return "", err
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

func (q *QRService) Decode(ctx context.Context, encodedStr string) (QRData, error) {
	decodedStr, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		return QRData{}, err
	}
	nonceSize := q.aead.NonceSize()
	if len(decodedStr) < nonceSize {
		return QRData{}, errors.New("data is too short")
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
	return decodedQRData, err
}
