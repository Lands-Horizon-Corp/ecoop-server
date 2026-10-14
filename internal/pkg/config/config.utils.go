package config

import (
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

func fillStruct(v reflect.Value, prefix string, lookup func(string) (string, bool)) ([]error, bool) {
	t := v.Type()
	var errs []error
	anySet := false

	for i := range t.NumField() {
		field := t.Field(i)
		fv := v.Field(i)

		if !field.IsExported() {
			continue
		}

		tag, hasTag := field.Tag.Lookup("env")
		if !hasTag {
			if isNestedStruct(field.Type) {
				subPrefix := prefix + field.Tag.Get("envPrefix")

				if field.Type.Kind() == reflect.Pointer {
					elem := reflect.New(field.Type.Elem()).Elem()
					subErrs, populated := fillStruct(elem, subPrefix, lookup)
					errs = append(errs, subErrs...)
					if populated {
						ptr := reflect.New(field.Type.Elem())
						ptr.Elem().Set(elem)
						fv.Set(ptr)
						anySet = true
					}
				} else {
					subErrs, populated := fillStruct(fv, subPrefix, lookup)
					errs = append(errs, subErrs...)
					if populated {
						anySet = true
					}
				}
			}
			continue
		}

		name, flags, _ := strings.Cut(tag, ",")
		key := prefix + name
		raw, ok := lookup(key)

		if !ok {
			raw, ok = field.Tag.Lookup("envDefault")
		}

		if !ok {
			if strings.Contains(flags, "required") {
				errs = append(errs, fmt.Errorf("%w: %s", ErrMissing, key))
			}
			continue
		}

		if err := setValue(fv, raw); err != nil {
			errs = append(errs, fmt.Errorf("%w: %s (%s): %v", ErrInvalidValue, key, field.Type, err))
		} else {
			anySet = true
		}
	}

	return errs, anySet
}

func isNestedStruct(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct && t != timeType && !reflect.PointerTo(t).Implements(textUnmarshaler)
}

func setValue(v reflect.Value, raw string) error {
	if v.Kind() == reflect.Pointer {
		p := reflect.New(v.Type().Elem())
		if err := setValue(p.Elem(), raw); err != nil {
			return err
		}
		v.Set(p)
		return nil
	}

	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshaler) {
		if err := v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(raw)); err != nil {
			return errors.New("invalid text unmarshaler format")
		}
		return nil
	}

	switch v.Type() {
	case durationType:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return errors.New("invalid duration (expected e.g. 30s, 5m)")
		}
		v.SetInt(int64(d))
		return nil
	case timeType:
		tm, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return errors.New("invalid RFC3339 timestamp")
		}
		v.Set(reflect.ValueOf(tm))
		return nil
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)

	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return errors.New("invalid boolean")
		}
		v.SetBool(b)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, v.Type().Bits())
		if err != nil {
			return errors.New("integer out of range or invalid")
		}
		v.SetInt(n)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, v.Type().Bits())
		if err != nil {
			return errors.New("unsigned integer out of range or invalid")
		}
		v.SetUint(n)

	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, v.Type().Bits())
		if err != nil {
			return errors.New("invalid floating point number")
		}
		v.SetFloat(f)

	case reflect.Slice:
		parts := strings.Split(raw, ",")
		out := reflect.MakeSlice(v.Type(), 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			elem := reflect.New(v.Type().Elem()).Elem()
			if err := setValue(elem, p); err != nil {
				return fmt.Errorf("slice item %d: %w", out.Len(), err)
			}
			out = reflect.Append(out, elem)
		}
		v.Set(out)

	default:
		return fmt.Errorf("unsupported field type %s", v.Type())
	}

	return nil
}

func readYAML(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(map[string]string)
	dec := yaml.NewDecoder(f)

	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if err := flattenDoc(doc, out); err != nil {
			return nil, err
		}
	}
}

func flattenDoc(doc map[string]any, out map[string]string) error {
	if doc == nil {
		return nil
	}
	if doc["kind"] == "Secret" {
		if sd, ok := doc["stringData"].(map[string]any); ok {
			for k, v := range sd {
				out[envKey(k)] = scalar(v)
			}
		}
		if d, ok := doc["data"].(map[string]any); ok {
			for k, v := range d {
				b, err := base64.StdEncoding.DecodeString(scalar(v))
				if err != nil {
					return fmt.Errorf("secret data %q is not valid base64: %w", k, err)
				}
				out[envKey(k)] = string(b)
			}
		}
		return nil
	}
	for k, v := range doc {
		if k == "env" {
			switch env := v.(type) {
			case []any:
				for _, item := range env {
					if m, ok := item.(map[string]any); ok && m["name"] != nil && m["value"] != nil {
						out[envKey(scalar(m["name"]))] = scalar(m["value"])
					}
				}
				continue
			case map[string]any:
				for ek, ev := range env {
					out[envKey(ek)] = scalar(ev)
				}
				continue
			}
		}
		flatten(envKey(k), v, out)
	}

	return nil
}

func flatten(key string, v any, out map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			flatten(key+"_"+envKey(k), sub, out)
		}
	case []any:
		parts := make([]string, 0, len(x))
		for _, e := range x {
			parts = append(parts, scalar(e))
		}
		out[key] = strings.Join(parts, ",")
	default:
		out[key] = scalar(v)
	}
}

func envKey(k string) string {
	var b strings.Builder
	runes := []rune(k)

	for i := range runes {
		r := runes[i]
		switch {
		case r == '-' || r == '.' || r == ' ':
			b.WriteByte('_')
		case r >= 'A' && r <= 'Z':
			if i > 0 {
				prev := runes[i-1]
				next := rune(0)
				if i+1 < len(runes) {
					next = runes[i+1]
				}
				if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') ||
					(next >= 'a' && next <= 'z' && prev >= 'A' && prev <= 'Z') {
					b.WriteByte('_')
				}
			}
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}

	return strings.ToUpper(b.String())
}

func scalar(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
