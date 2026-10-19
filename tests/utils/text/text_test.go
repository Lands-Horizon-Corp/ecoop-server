package text_test

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

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

func TestTruncate(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"exactly10!", 10, "exactly10!"},
		{"longer than ten", 10, "longer tha..."},
		{"", 5, ""},
		{"abc", 0, "..."},
		{"abc", -3, "..."},
		{"日本語です", 4, "日..."},                  // a cut inside a rune drops the split rune
		{"bad\xffbyte", 20, "badbyte"},        // invalid UTF-8 is dropped even when short
		{"\xff\xfe\xfdlong enough", 3, "..."}, // nothing valid is left in the first 3 bytes
	} {
		if got := utils.Truncate(c.in, c.n); got != c.want {
			t.Errorf("Truncate(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
	long := strings.Repeat("x", 10_000)
	if got := utils.Truncate(long, 64); len(got) != 67 {
		t.Errorf("a long value was cut to %d bytes, want 64 plus the ellipsis", len(got))
	}
}

func TestStripControl(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"plain":                 "plain",
		"line\r\nbreak":         "linebreak",
		"tab\there":             "tabhere",
		"nul\x00byte":           "nulbyte",
		"esc\x1b[31mred":        "esc[31mred",
		"del\x7fete":            "delete",
		"next\u0085line":        "nextline", // NEL is a control character too
		"日本語 and spaces stay":   "日本語 and spaces stay",
		"":                      "",
		"\r\nSet-Cookie: pwn=1": "Set-Cookie: pwn=1",
	} {
		if got := utils.StripControl(in); got != want {
			t.Errorf("StripControl(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSplitList(t *testing.T) {
	t.Parallel()
	for in, want := range map[string][]string{
		"a,b,c":             {"a", "b", "c"},
		" a , b ,, c ,":     {"a", "b", "c"},
		"single":            {"single"},
		"":                  nil,
		" , ,, ":            nil,
		"https://a.test, b": {"https://a.test", "b"},
		"10.0.0.0/8,::1":    {"10.0.0.0/8", "::1"},
	} {
		got := utils.SplitList(in)
		if len(got) != len(want) {
			t.Errorf("SplitList(%q) = %q, want %q", in, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("SplitList(%q) = %q, want %q", in, got, want)
			}
		}
	}
}

func TestASCIILower(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"API.Ecoop.TEST": "api.ecoop.test",
		"already lower":  "already lower",
		"":               "",
		"MiXeD-123_[]@":  "mixed-123_[]@",
		"\u212Aapi":      "\u212Aapi", // the Kelvin sign is not K, and must not become k
		"İSTANBUL":       "İstanbul",  // non-ASCII letters are left alone
	} {
		if got := utils.ASCIILower(in); got != want {
			t.Errorf("ASCIILower(%q) = %q, want %q", in, got, want)
		}
	}
	// The reason this exists: strings.ToLower folds the Kelvin sign onto k.
	if strings.ToLower("\u212Aapi") != "kapi" {
		t.Skip("this Go version no longer folds the Kelvin sign; the guard is moot but harmless")
	}
	if utils.ASCIILower("\u212Aapi") == "kapi" {
		t.Error("ASCIILower folded the Kelvin sign onto k")
	}
}

func FuzzTextHelpers(f *testing.F) {
	for _, s := range []string{"", "a,b", "\r\n", "\xff", "日本語", "\u212A", strings.Repeat("é", 100)} {
		f.Add(s, 7)
	}
	f.Fuzz(func(t *testing.T, s string, n int) {
		got := utils.Truncate(s, n)
		if !utf8.ValidString(got) {
			t.Fatalf("Truncate(%q, %d) = %q is not valid UTF-8", s, n, got)
		}
		if limit := max(n, 0); len(got) > limit+3 && len(got) > len(s) {
			t.Fatalf("Truncate(%q, %d) grew the value to %d bytes", s, n, len(got))
		}
		for _, r := range utils.StripControl(s) {
			if unicode.IsControl(r) {
				t.Fatalf("StripControl left %U in %q", r, s)
			}
		}
		for _, item := range utils.SplitList(s) {
			if item == "" || item != strings.TrimSpace(item) || strings.Contains(item, ",") {
				t.Fatalf("SplitList(%q) produced a bad item %q", s, item)
			}
		}
		low := utils.ASCIILower(s)
		if len(low) != len(s) {
			t.Fatalf("ASCIILower changed the length of %q", s)
		}
		for i := range len(s) {
			if s[i] >= 0x80 && low[i] != s[i] {
				t.Fatalf("ASCIILower altered a non-ASCII byte of %q", s)
			}
		}
	})
}
