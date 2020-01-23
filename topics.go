package datalive

import (
	"sync"
)

type topics struct {
	w  map[string]*watchers
	mu sync.RWMutex
}

func (t *topics) Watch(key string, c chan interface{}) chan interface{} {
	if c == nil {
		c = make(chan interface{}, 5)
	}
	t.mu.RLock()
	w, ok := t.w[key]
	t.mu.RUnlock()
	if !ok {
		t.mu.Lock()
		w, ok = t.w[key]
		if !ok {
			w = &watchers{}
			t.w[key] = w
		}
		t.mu.Unlock()
	}
	w.watch(c)
	return c
}

func (t *topics) Send(key string, b interface{}) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	w, ok := t.w[key]
	if !ok {
		return
	}
	w.send(b)
}

func (t *topics) Close(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	w, ok := t.w[key]
	if !ok {
		return
	}
	w.close()
	delete(t.w, key)
}

func (t *topics) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range t.w {
		v.close()
		delete(t.w, k)
	}
}

type watchers struct {
	c  []chan interface{}
	mu sync.Mutex
}

func (w *watchers) watch(c chan interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.c = append(w.c, c)
}

func (w *watchers) send(b interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, c := range w.c {
		select {
		case c <- b:
		default:
			close(c)
			w.c[i] = w.c[len(w.c)-1]
			w.c = w.c[:len(w.c)-1]
		}
	}
}

func (w *watchers) close() {
	for _, c := range w.c {
		close(c)
	}
	w.c = w.c[:0]
}
