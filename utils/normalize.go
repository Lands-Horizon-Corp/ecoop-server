package utils

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	columnNameInvalidChars = regexp.MustCompile(`[^a-z_]+`)
	columnNameRepeatedSep  = regexp.MustCompile(`_+`)
	pascalCaseWordSep      = regexp.MustCompile(`[^A-Za-z0-9]+`)
)

func NormalizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = camelToSnake(name)
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

func camelToSnake(s string) string {
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
