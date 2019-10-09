package inmemory

import (
	"fmt"
	"github.com/muesli/cache2go"
	"time"
)

type Cache2Go struct {
	instance      *cache2go.CacheTable
	defaultExpiry time.Duration
}

func New(name string, expiry time.Duration) *Cache2Go {
	if expiry == 0 {
		expiry = 15 * time.Second
	}

	return &Cache2Go{
		instance:      cache2go.Cache(name),
		defaultExpiry: expiry,
	}
}

func (c *Cache2Go) Get(key string) ([]byte, error) {
	//internal.Log.Printf("cache2go: attempting to lookup %s", key)

	if c.instance.Exists(key) {
		tmp, err := c.instance.Value(key)
		if err != nil {
			return nil, err
		}
		b, ok := tmp.Data().([]byte)
		if !ok {
			return nil, fmt.Errorf("cache2go: could not cast data to []byte for key " + key)
		}

		//internal.Log.Printf("cache2go: successfully looked up %s, value %s", key, string(b))
		return b, nil
	}

	//internal.Log.Printf("cache2go: failed to lookup %s", key)
	return nil, cache2go.ErrKeyNotFound
}

func (c *Cache2Go) Exists(key string) bool {
	return c.instance.Exists(key)
}

func (c *Cache2Go) Set(key string, value []byte, expiresIn time.Duration) {
	if expiresIn == 0 && c.defaultExpiry != 0 {
		expiresIn = c.defaultExpiry
	}

	c.instance.Add(key, expiresIn, value)
}

func (c *Cache2Go) Delete(key string) error {
	_, err := c.instance.Delete(key)
	return err
}

func (c *Cache2Go) Len() int {
	return c.instance.Count()
}

func (c *Cache2Go) Flush() error {
	c.instance.Flush()
	return nil
}
