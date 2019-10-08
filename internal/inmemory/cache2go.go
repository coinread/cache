package inmemory

import (
	"fmt"
	"github.com/muesli/cache2go"
	"time"
)

type CacheProxy struct {
	instance      *cache2go.CacheTable
	defaultExpiry time.Duration
}

func New(name string) *CacheProxy {
	return &CacheProxy{
		instance: cache2go.Cache(name),
	}
}

func (c *CacheProxy) Get(key string) ([]byte, error) {
	tmp, err := c.instance.Value(key)
	if err != nil {
		return nil, err
	}
	b, ok := tmp.Data().([]byte)
	if !ok {
		return nil, fmt.Errorf("cache2go: could not cast data to []byte for key " + key)
	}

	return b, nil
}

func (c *CacheProxy) Set(key string, value []byte, expiresIn time.Duration) {
	if expiresIn == 0 {
		expiresIn = c.defaultExpiry
	}

	c.instance.Add(key, expiresIn, value)
}

func (c *CacheProxy) Delete(key string) error {
	_, err := c.instance.Delete(key)
	return err
}

func (c *CacheProxy) Len() int {
	return c.instance.Count()
}

func (c *CacheProxy) Flush() error {
	c.instance.Flush()
	return nil
}
