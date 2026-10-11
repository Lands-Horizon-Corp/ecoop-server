package otp

import "context"

type OTPService struct {
	secret string
}

func NewOTPService(secret string) OTPServices {
	return &OTPService{
		secret: secret,
	}
}

// Generate implements [OTPServices].
func (o *OTPService) Generate(ctx context.Context, key string) (string, error) {
	panic("unimplemented")
}

// Revoke implements [OTPServices].
func (o *OTPService) Revoke(ctx context.Context, key string) error {
	panic("unimplemented")
}

// Verify implements [OTPServices].
func (o *OTPService) Verify(ctx context.Context, key string, code string) (bool, error) {
	panic("unimplemented")
}
