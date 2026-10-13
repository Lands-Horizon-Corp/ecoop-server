package utils

import (
	"regexp"
	"strings"
	"sync"
	"unicode"

	"github.com/microcosm-cc/bluemonday"
)

var (
	columnNameInvalidChars = regexp.MustCompile(`[^a-z_]+`)
	columnNameRepeatedSep  = regexp.MustCompile(`_+`)
	pascalCaseWordSep      = regexp.MustCompile(`[^A-Za-z0-9]+`)

	cssURL = regexp.MustCompile(`(?i)url\(\s*['"]?\s*([^'")\s]*)`)

	emailPolicy = sync.OnceValue(func() *bluemonday.Policy {
		p := bluemonday.NewPolicy()
		p.AllowElements("style")
		p.AllowElements("html", "head", "body", "div", "span", "p", "br", "h1", "h2", "h3", "h4", "h5", "h6")
		p.AllowElements("table", "thead", "tbody", "tfoot", "tr", "th", "td")
		p.AllowAttrs("style").OnElements("div", "span", "p", "table", "td", "th", "tr", "body")
		for _, prop := range []string{
			"color", "background", "background-color", "font-family", "font-size", "font-weight",
			"text-align", "width", "height", "margin", "padding", "border", "border-collapse",
			"display", "flex", "grid-template-columns", "gap", "justify-content", "align-items",
		} {
			p.AllowStyles(prop).MatchingHandler(safeCSSValue).Globally()
		}
		return p
	})
)

func safeCSSValue(value string) bool {
	low := strings.ToLower(value)
	for _, bad := range []string{"expression", "javascript:", "vbscript:", "behavior", "-moz-binding", "@import", `\`, "/*", "<", ">"} {
		if strings.Contains(low, bad) {
			return false
		}
	}
	for _, m := range cssURL.FindAllStringSubmatch(low, -1) {
		if !strings.HasPrefix(m[1], "https://") && !strings.HasPrefix(m[1], "http://") {
			return false
		}
	}
	return true
}

func Sanitize(input string) string {
	return emailPolicy().Sanitize(strings.TrimSpace(input))
}

func NormalizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = CamelToSnake(name)
	name = strings.ToLower(name)
	name = columnNameInvalidChars.ReplaceAllString(name, "_")
	name = columnNameRepeatedSep.ReplaceAllString(name, "_")
	return strings.Trim(name, "_")
}

func ToPascalCase(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	words := pascalCaseWordSep.Split(s, -1)
	var b strings.Builder
	b.Grow(len(s))
	for _, w := range words {
		if w == "" {
			continue
		}
		r := []rune(w)
		b.WriteRune(unicode.ToUpper(r[0]))
		b.WriteString(string(r[1:]))
	}
	return b.String()
}

func CamelToSnake(s string) string {
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(runes) + len(runes)/3)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextIsLower) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
