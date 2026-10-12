package text_test

import (
	"testing"

	"github.com/Lands-Horizon-Corp/ecoop-server/internal/utils"
)

func TestSlug(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"Add Members":                 "add_members",
		"  --Loan  Terms (v2)!!  ":    "loan_terms_v2",
		"../../etc/passwd":            "etc_passwd",
		"app.ecoop.test":              "app_ecoop_test",
		"日本語":                         "",
		"":                            "",
		"<script>alert(1)</script>":   "script_alert_1_script",
		"evil.test/;domain=bank.test": "evil_test_domain_bank_test",
	} {
		if got := utils.Slug(in, '_'); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
	if got := utils.Slug("app.ecoop.test", '-'); got != "app-ecoop-test" {
		t.Errorf("with '-': %q", got)
	}
}

func TestRequestHost(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ origin, referer, host, want string }{
		{"https://app.ecoop.test", "https://other.test/page", "api.ecoop.test", "app.ecoop.test"},
		{"", "https://admin.ecoop.test:8443/x", "api.ecoop.test", "admin.ecoop.test"},
		{"", "", "api.ecoop.test:6767", "api.ecoop.test"},
		{"", "", "[::1]:6767", "::1"},
		{"null", "", "api.ecoop.test", "api.ecoop.test"}, // sandboxed iframes send Origin: null
		{"", "", "", ""},
	} {
		if got := utils.RequestHost(c.origin, c.referer, c.host); got != c.want {
			t.Errorf("RequestHost(%q, %q, %q) = %q, want %q", c.origin, c.referer, c.host, got, c.want)
		}
	}
}
