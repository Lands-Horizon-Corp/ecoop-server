package utils

import "maps"

// CloneWith returns a copy of m with key set to value, leaving m unchanged.
// A nil m gives a new map.
func CloneWith[K comparable, V any](m map[K]V, key K, value V) map[K]V {
	out := maps.Clone(m)
	if out == nil {
		out = make(map[K]V, 1)
	}
	out[key] = value
	return out
}
