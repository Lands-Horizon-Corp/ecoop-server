package utils

import "sync"

type MapPool[K comparable, V any] struct {
	pool sync.Pool
}

func NewMapPool[K comparable, V any]() *MapPool[K, V] {
	return &MapPool[K, V]{
		pool: sync.Pool{
			New: func() any {
				return make(map[K]V, 128)
			},
		},
	}
}

func (p *MapPool[K, V]) Get() map[K]V {
	return p.pool.Get().(map[K]V)
}

func (p *MapPool[K, V]) Put(m map[K]V) {
	if m == nil {
		return
	}
	if len(m) > 10000 {
		return
	}
	clear(m)
	p.pool.Put(m)
}
