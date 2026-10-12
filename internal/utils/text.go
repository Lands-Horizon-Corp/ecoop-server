package utils

import (
	"net/url"
	"strings"
)

// Slug lower-cases s and keeps its ASCII letters and digits, turning every
// run of anything else into one sep, trimmed from both ends: "Add Members"
// -> "add_members" with '_', "app.ecoop.test" -> "app-ecoop-test" with
// '-'. It suits names built from user input: files, cookies, keys.
func Slug(s string, sep byte) string {
	var b strings.Builder
	pending := false
	for _, r := range strings.ToLower(s) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			if pending && b.Len() > 0 {
				b.WriteByte(sep)
			}
			pending = false
			b.WriteRune(r)
			continue
		}
		pending = true
	}
	return b.String()
}

// RequestHost is the host name of the site making a request: from its
// Origin header, else its Referer, else the Host it called (without the
// port). It tells apart front ends that share one API.
func RequestHost(origin, referer, host string) string {
	for _, raw := range []string{origin, referer} {
		if u, err := url.Parse(strings.TrimSpace(raw)); err == nil && u.Hostname() != "" {
			return u.Hostname()
		}
	}
	if u, err := url.Parse("//" + strings.TrimSpace(host)); err == nil {
		return u.Hostname()
	}
	return ""
}
