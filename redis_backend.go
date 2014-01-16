package snowdrift

import (
	"github.com/garyburd/redigo/redis"
)

type RedisBackend struct {
	prefix string
	conn   redis.Conn
}

func NewRedisBackend(conn redis.Conn, prefix string) Backend {
	return &RedisBackend{prefix, conn}
}

func (b *RedisBackend) codeKey(code string) string {
	return b.prefix + "code:" + code
}

func (b *RedisBackend) urlKey(digest string) string {
	return b.prefix + "url:" + digest
}

func (b *RedisBackend) Add(url, digest, code string) error {
	res, err := b.conn.Do("SET", b.urlKey(digest), code, "NX")
	if err != nil {
		return err
	}
	if res == nil {
		return ErrURLExists
	}

	res, err = b.conn.Do("SET", b.codeKey(code), url, "NX")
	if err != nil {
		return err
	}
	if res == nil {
		return ErrCodeExists
	}

	return nil
}

func (b *RedisBackend) GetCode(digest string) (string, error) {
	res, err := redis.String(b.conn.Do("GET", b.urlKey(digest)))
	if err == redis.ErrNil {
		err = ErrNotFound
	}
	return res, err
}

func (b *RedisBackend) GetURL(code string) (string, error) {
	res, err := redis.String(b.conn.Do("GET", b.codeKey(code)))
	if err == redis.ErrNil {
		err = ErrNotFound
	}
	return res, err
}

func (b *RedisBackend) NextID() (int, error) {
	return redis.Int(b.conn.Do("INCR", b.prefix+"id"))
}
