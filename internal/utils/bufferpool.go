package utils

import "sync"

type BufferPool[T any] struct {
	pool sync.Pool
}

func NewBufferPool[T any]() *BufferPool[T] {
	return &BufferPool[T]{
		pool: sync.Pool{
			New: func() any {
				b := make([]T, 0, 128)
				return &b
			},
		},
	}
}

func (p *BufferPool[T]) Get() *[]T {
	buf := p.pool.Get().(*[]T)
	*buf = (*buf)[:0]
	return buf
}

func (p *BufferPool[T]) Put(buf *[]T) {
	if buf == nil {
		return
	}
	if cap(*buf) > 10000 {
		return
	}
	clear(*buf)
	*buf = (*buf)[:0]
	p.pool.Put(buf)
}
