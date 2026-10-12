package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/cache"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/pkg/security"
	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

type AuthService[T ClaimWithID] struct {
	name       string
	csrfHeader string
	ssl        bool

	cache    cache.CacheServices
	security security.SecurityServices
}

func NewAuthService[T ClaimWithID](
	name string,
	csrfHeader string,
	ssl bool,
	cache cache.CacheServices,
	security security.SecurityServices,
) AUTHServices[T] {
	return &AuthService[T]{
		name:       name,
		csrfHeader: csrfHeader,
		ssl:        ssl,
		cache:      cache,
		security:   security,
	}
}

type session struct {
	UserID string `json:"u"`
	Claim  []byte `json:"c"` // encrypted JSON of T
}

func (h *AuthService[T]) SetCSRF(ctx context.Context, c *app.RequestContext, claim T, expiry time.Duration) error {
	if err := h.ready(); err != nil {
		return err
	}
	if c == nil {
		return ErrNoRequest
	}
	if utils.IsNil(claim) || claim.GetID() == "" {
		return ErrNoUserID
	}
	if expiry < time.Second || expiry > MaxSessionTTL {
		return ErrInvalidExpiry
	}
	token := utils.RandomToken(TokenBytes)
	data, err := json.Marshal(claim)
	if err != nil {
		return errors.Join(ErrSession, err)
	}
	sealed, err := h.security.EncryptData(ctx, data)
	if err != nil {
		return errors.Join(ErrSession, err)
	}
	record, _ := json.Marshal(session{UserID: claim.GetID(), Claim: sealed})
	id := utils.HashHex(token)
	if err := h.cache.Set(ctx, h.sessionKey(id), record, expiry); err != nil {
		return errors.Join(ErrSession, err)
	}
	userKey := h.userKey(claim.GetID())
	if err := h.cache.ZAdd(ctx, userKey, float64(time.Now().Add(expiry).Unix()), id); err != nil {
		_, _ = h.cache.Delete(ctx, h.sessionKey(id))
		return errors.Join(ErrSession, err)
	}
	_, _ = h.cache.Expire(ctx, userKey, MaxSessionTTL) // the index outlives any one session
	c.Response.Header.Set(h.csrfHeader, token)
	h.setCookie(c, h.cookieName(c), token, int(expiry/time.Second))
	return nil
}

func (h *AuthService[T]) GetCSRF(ctx context.Context, c *app.RequestContext) (T, error) {
	claim, _, err := h.current(ctx, c)
	return claim, err
}

func (h *AuthService[T]) VerifyCSRF(ctx context.Context, token string) (T, error) {
	claim, _, err := h.load(ctx, token)
	return claim, err
}

// ClearCSRF signs out this device. Without a session there is nothing to
// do; the cookies are cleared either way.
func (h *AuthService[T]) ClearCSRF(ctx context.Context, c *app.RequestContext) error {
	defer h.clearCookies(c)
	if err := h.ready(); err != nil {
		return err
	}
	token := h.token(c)
	if token == "" {
		return nil
	}
	id := utils.HashHex(token)
	_, s, err := h.load(ctx, token)
	if err != nil {
		if errors.Is(err, ErrInvalidToken) {
			return nil
		}
		return err
	}
	return h.remove(ctx, s.UserID, id)
}

// Devices

func (h *AuthService[T]) HasActiveSessionOnOtherDevice(ctx context.Context, c *app.RequestContext) (bool, error) {
	claim, id, err := h.current(ctx, c)
	if err != nil {
		return false, err
	}
	others, err := h.others(ctx, claim.GetID(), id)
	return len(others) > 0, err
}

func (h *AuthService[T]) GetActiveSessionClaims(ctx context.Context, c *app.RequestContext) ([]T, error) {
	claim, id, err := h.current(ctx, c)
	if err != nil {
		return nil, err
	}
	others, err := h.others(ctx, claim.GetID(), id)
	if err != nil {
		return nil, err
	}
	claims := make([]T, 0, len(others))
	for _, s := range others {
		if t, err := h.open(ctx, s); err == nil {
			claims = append(claims, t)
		}
	}
	return claims, nil
}

func (h *AuthService[T]) RevokeUserSessions(ctx context.Context, userID string) error {
	if err := h.ready(); err != nil {
		return err
	}
	if userID == "" {
		return ErrNoUserID
	}
	return h.revoke(ctx, userID, "")
}

func (h *AuthService[T]) RevokeAllUserSessions(ctx context.Context, c *app.RequestContext) error {
	claim, _, err := h.current(ctx, c)
	if err != nil {
		return err
	}
	defer h.clearCookies(c)
	return h.revoke(ctx, claim.GetID(), "")
}

// RevokeOtherDeviceSessions signs the user out everywhere else; this device
// stays signed in.
func (h *AuthService[T]) RevokeOtherDeviceSessions(ctx context.Context, c *app.RequestContext) error {
	claim, id, err := h.current(ctx, c)
	if err != nil {
		return err
	}
	return h.revoke(ctx, claim.GetID(), id)
}

// Helpers

func (h *AuthService[T]) ready() error {
	var errs []error
	if utils.IsNil(h.cache) {
		errs = append(errs, ErrNoCache)
	}
	if utils.IsNil(h.security) {
		errs = append(errs, ErrNoSecurity)
	}
	return errors.Join(errs...)
}

func (h *AuthService[T]) sessionKey(id string) string {
	return h.name + ":auth:session:" + id
}
func (h *AuthService[T]) userKey(userID string) string {
	return h.name + ":auth:user:" + userID
}

func (h *AuthService[T]) current(ctx context.Context, c *app.RequestContext) (T, string, error) {
	var zero T
	token := h.token(c)
	if token == "" {
		if err := h.ready(); err != nil {
			return zero, "", err
		}
		return zero, "", ErrNoToken
	}
	claim, _, err := h.load(ctx, token)
	return claim, utils.HashHex(token), err
}

func (h *AuthService[T]) load(ctx context.Context, token string) (T, session, error) {
	var zero T
	if err := h.ready(); err != nil {
		return zero, session{}, err
	}
	if token == "" {
		return zero, session{}, ErrNoToken
	}
	s, err := h.read(ctx, utils.HashHex(token))
	if err != nil {
		return zero, session{}, err
	}
	claim, err := h.open(ctx, s)
	return claim, s, err
}

func (h *AuthService[T]) read(ctx context.Context, id string) (session, error) {
	raw, err := h.cache.Get(ctx, h.sessionKey(id))
	if errors.Is(err, cache.ErrNotFound) {
		return session{}, ErrInvalidToken
	}
	if err != nil {
		return session{}, errors.Join(ErrSession, err)
	}
	var s session
	if err := json.Unmarshal(raw, &s); err != nil || s.UserID == "" {
		return session{}, ErrInvalidToken
	}
	return s, nil
}

func (h *AuthService[T]) open(ctx context.Context, s session) (T, error) {
	var claim T
	data, err := h.security.DecryptData(ctx, s.Claim)
	if err != nil {
		return claim, ErrInvalidToken
	}
	if err := json.Unmarshal(data, &claim); err != nil {
		return claim, ErrInvalidToken
	}
	return claim, nil
}
func (h *AuthService[T]) others(ctx context.Context, userID, except string) ([]session, error) {
	ids, err := h.ids(ctx, userID)
	if err != nil {
		return nil, err
	}
	var out []session
	for _, id := range ids {
		if id == except {
			continue
		}
		s, err := h.read(ctx, id)
		if errors.Is(err, ErrInvalidToken) {
			_, _ = h.cache.ZRem(ctx, h.userKey(userID), id)
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (h *AuthService[T]) ids(ctx context.Context, userID string) ([]string, error) {
	key := h.userKey(userID)
	if _, err := h.cache.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return nil, errors.Join(ErrSession, err)
	}
	ids, err := h.cache.ZRange(ctx, key, 0, -1)
	if err != nil {
		return nil, errors.Join(ErrSession, err)
	}
	return ids, nil
}

func (h *AuthService[T]) revoke(ctx context.Context, userID, except string) error {
	ids, err := h.ids(ctx, userID)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		if id != except {
			errs = append(errs, h.remove(ctx, userID, id))
		}
	}
	return errors.Join(errs...)
}

func (h *AuthService[T]) remove(ctx context.Context, userID, id string) error {
	if _, err := h.cache.Delete(ctx, h.sessionKey(id)); err != nil {
		return errors.Join(ErrSession, err)
	}
	if _, err := h.cache.ZRem(ctx, h.userKey(userID), id); err != nil {
		return errors.Join(ErrSession, err)
	}
	return nil
}

func (h *AuthService[T]) token(c *app.RequestContext) string {
	if c == nil {
		return ""
	}
	if t := strings.TrimSpace(string(c.GetHeader(h.csrfHeader))); t != "" {
		return t
	}
	if t := string(c.Cookie(h.cookieName(c))); t != "" {
		return t
	}
	return string(c.Cookie(h.csrfHeader))
}

// cookieName is the session cookie for the site calling, so one API can
// serve several front ends without their sessions overwriting each other.
func (h *AuthService[T]) cookieName(c *app.RequestContext) string {
	host := utils.RequestHost(string(c.GetHeader("Origin")), string(c.GetHeader("Referer")), string(c.Host()))
	if scope := utils.Slug(host, '-'); scope != "" {
		return h.csrfHeader + "-" + scope
	}
	return h.csrfHeader
}

func (h *AuthService[T]) setCookie(c *app.RequestContext, name, value string, maxAge int) {
	sameSite := protocol.CookieSameSiteLaxMode
	if h.ssl {
		sameSite = protocol.CookieSameSiteNoneMode
	}
	c.SetCookie(name, value, maxAge, "/", "", sameSite, h.ssl, true)
}

func (h *AuthService[T]) clearCookies(c *app.RequestContext) {
	if c == nil {
		return
	}
	h.setCookie(c, h.cookieName(c), "", -1)
	if name := h.cookieName(c); name != h.csrfHeader {
		h.setCookie(c, h.csrfHeader, "", -1)
	}
}
