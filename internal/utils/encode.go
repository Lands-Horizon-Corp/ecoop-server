package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/microcosm-cc/bluemonday"
)

var ErrNilValue = errors.New("value is nil")

func Encode(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case bool:
		return strconv.FormatBool(v), nil
	}
	if IsNil(value) {
		return "", ErrNilValue
	}
	b, err := sonic.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encoding value: %w", err)
	}
	return string(b), nil
}

func Sanitize(input string) string {
	p := bluemonday.NewPolicy()
	p.AllowElements("style")
	p.AllowElements("html", "head", "body", "div", "span", "p", "br", "h1", "h2", "h3", "h4", "h5", "h6")
	p.AllowElements("table", "thead", "tbody", "tfoot", "tr", "th", "td")
	p.AllowAttrs("style").OnElements("div", "span", "p", "table", "td", "th", "tr", "body")
	anyValue := func(value string) bool { return true }
	p.AllowStyles("color").MatchingHandler(anyValue).Globally()
	p.AllowStyles("background").MatchingHandler(anyValue).Globally()
	p.AllowStyles("background-color").MatchingHandler(anyValue).Globally()
	p.AllowStyles("font-family").MatchingHandler(anyValue).Globally()
	p.AllowStyles("font-size").MatchingHandler(anyValue).Globally()
	p.AllowStyles("font-weight").MatchingHandler(anyValue).Globally()
	p.AllowStyles("text-align").MatchingHandler(anyValue).Globally()
	p.AllowStyles("width").MatchingHandler(anyValue).Globally()
	p.AllowStyles("height").MatchingHandler(anyValue).Globally()
	p.AllowStyles("margin").MatchingHandler(anyValue).Globally()
	p.AllowStyles("padding").MatchingHandler(anyValue).Globally()
	p.AllowStyles("border").MatchingHandler(anyValue).Globally()
	p.AllowStyles("border-collapse").MatchingHandler(anyValue).Globally()
	p.AllowStyles("display").MatchingHandler(anyValue).Globally()
	p.AllowStyles("flex").MatchingHandler(anyValue).Globally()
	p.AllowStyles("grid-template-columns").MatchingHandler(anyValue).Globally()
	p.AllowStyles("gap").MatchingHandler(anyValue).Globally()
	p.AllowStyles("justify-content").MatchingHandler(anyValue).Globally()
	p.AllowStyles("align-items").MatchingHandler(anyValue).Globally()
	return p.Sanitize(strings.TrimSpace(input))
}
