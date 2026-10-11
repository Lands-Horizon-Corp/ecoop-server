package utils

import (
	"regexp"
	"strings"
	"sync"

	"github.com/microcosm-cc/bluemonday"
)

var emailPolicy = sync.OnceValue(func() *bluemonday.Policy {
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

var cssURL = regexp.MustCompile(`(?i)url\(\s*['"]?\s*([^'")\s]*)`)

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
