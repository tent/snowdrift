package snowdrift

import (
	"sync"
	"sync/atomic"
)

type MemoryBackend struct {
	urlCodes map[string]string
	codeURLs map[string]string
	id       uint64
	mtx      sync.RWMutex
}

func NewMemoryBackend() Backend {
	return &MemoryBackend{urlCodes: make(map[string]string), codeURLs: make(map[string]string)}
}

func (b *MemoryBackend) Add(url, digest, code string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if _, exists := b.urlCodes[digest]; exists {
		return ErrURLExists
	}
	if _, exists := b.codeURLs[code]; exists {
		return ErrCodeExists
	}
	b.urlCodes[digest] = code
	b.codeURLs[code] = url
	return nil
}

func (b *MemoryBackend) GetCode(digest string) (string, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	code, ok := b.urlCodes[digest]
	if !ok {
		return "", ErrNotFound
	}
	return code, nil
}

func (b *MemoryBackend) GetURL(code string) (string, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	url, ok := b.codeURLs[code]
	if !ok {
		return "", ErrNotFound
	}
	return url, nil
}

func (b *MemoryBackend) NextID() (int, error) {
	return int(atomic.AddUint64(&b.id, 1)), nil
}
