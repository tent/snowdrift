package snowdrift

import (
	"github.com/cupcake/go-riak"
)

type IDBackend interface {
	NextID() (int, error)
}

type RiakBackend struct {
	prefix string
	riak   *riak.Client
	IDBackend
}

type riakURL struct {
	URL string `riak:"string"`
	riak.Model
}

type riakCode struct {
	Code string `riak:"code"`
	riak.Model
}

func NewRiakBackend(client *riak.Client, prefix string, idb IDBackend) Backend {
	return &RiakBackend{prefix: prefix, riak: client, IDBackend: idb}
}

func (b *RiakBackend) Add(url, digest, code string) error {
	codeData := &riakCode{Code: code}
	if err := b.riak.New(b.prefix+"codes", digest, codeData); err != nil {
		return err
	}
	if err := codeData.Save(); err != nil {
		return err
	}
	urlData := &riakURL{URL: url}
	if err := b.riak.New(b.prefix+"urls", code, urlData); err != nil {
		return err
	}
	return urlData.Save()
}

func (b *RiakBackend) GetCode(digest string) (string, error) {
	code := &riakCode{}
	err := b.riak.Load(b.prefix+"codes", digest, code)
	if err == riak.NotFound {
		err = ErrNotFound
	}
	return code.Code, err
}

func (b *RiakBackend) GetURL(code string) (string, error) {
	url := &riakURL{}
	err := b.riak.Load(b.prefix+"urls", code, url)
	if err == riak.NotFound {
		err = ErrNotFound
	}
	return url.URL, err
}
