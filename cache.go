package cache

import (
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v4"
	"sync/atomic"
	"time"

	"github.com/coinread/twotier-cache/v8/internal/inmemory"
	"github.com/coinread/twotier-cache/v8/internal/singleflight"

	"github.com/go-redis/redis/v7"
	"github.com/hashicorp/go-multierror"
)

var ErrCacheMiss = errors.New("cache: key is missing")
var ErrNilValueProvided = errors.New("cache: nil value(s) can NOT be stored")

type Redis interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
	PTTL(key string) *redis.DurationCmd
}

type TwoTier struct {
	R Redis
	L *inmemory.Cache2Go

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group singleflight.Group

	hits        uint64
	misses      uint64
	localHits   uint64
	localMisses uint64
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

func (tt *TwoTier) Get(key string, target interface{}) error {
	var found bool
	var b []byte
	var err error

	if tt.L != nil {
		b, err = tt.L.Get(key)
		if err == nil {
			found = true
			atomic.AddUint64(&tt.localHits, 1)
		}
	}

	if !found {
		atomic.AddUint64(&tt.localMisses, 1)

		if tt.R != nil {
			b, err = tt.R.Get(key).Bytes()
			if err == nil {
				found = true
				atomic.AddUint64(&tt.hits, 1)

				// Set this back into the local cache since we somehow found an out of sync record.
				tt.syncLocalFromRedis(key, b)
			} else {
				atomic.AddUint64(&tt.misses, 1)
			}
		}
	}

	if !found {
		return ErrCacheMiss
	}

	// This is to facilitate existence checking
	if target == nil {
		return nil
	}

	// In here, we are guaranteed to have it, one way or another
	// Just unmarshal and hand it over
	err = tt.Unmarshal(b, target)
	if err != nil {
		fmt.Printf("cache: key=%q Unmarshal(%T) failed: %s", key, target, err)
		_ = tt.Delete(key)
		return err
	}

	return nil
}

func (tt *TwoTier) syncLocalFromRedis(key string, value []byte) {
	ttl, err := tt.R.PTTL(key).Result()
	if err != nil {
		return
	}

	if tt.L != nil {
		tt.L.Set(key, value, ttl)
	}
}

func (tt *TwoTier) Exists(key string) bool {
	return tt.Get(key, nil) == nil
}

func (tt *TwoTier) Set(key string, expiresIn time.Duration, cb func() (interface{}, error)) (interface{}, error) {
	return tt.group.Do(key, func() (interface{}, error) {
		generated, err := cb()
		if err != nil {
			return nil, err
		}

		if generated == nil {
			return nil, ErrNilValueProvided
		}

		b, err := tt.Marshal(generated)
		if err != nil {
			return nil, err
		}

		// Let's set it to both the local cache and redis
		if tt.L != nil {
			tt.L.Set(key, b, expiresIn)
		}

		if tt.R != nil {
			err = tt.R.Set(key, b, expiresIn).Err()
			if err != nil {
				return nil, err
			}
		}

		return generated, nil
	})
}

// Funny implementation
func (tt *TwoTier) SetStatic(key string, expiresIn time.Duration, value interface{}) error {
	_, err := tt.Set(key, expiresIn, func() (interface{}, error) {
		return value, nil
	})

	return err
}

func (tt *TwoTier) Delete(key string) error {
	var out error

	if tt.L != nil {
		err := tt.L.Delete(key)
		if err != nil {
			out = multierror.Append(out, err)
		}
	}

	if tt.R != nil {
		err := tt.R.Del(key).Err()
		if err != nil {
			out = multierror.Append(out, err)
		}
	}

	return out
}

func (tt *TwoTier) Stats() Stats {
	return Stats{
		Hits:        atomic.LoadUint64(&tt.hits),
		Misses:      atomic.LoadUint64(&tt.misses),
		LocalHits:   atomic.LoadUint64(&tt.localHits),
		LocalMisses: atomic.LoadUint64(&tt.localMisses),
	}
}

func (tt *TwoTier) UseLocalCache(name string, defaultExpiry time.Duration) {
	tt.L = inmemory.New(name, defaultExpiry)
}

func (tt *TwoTier) UseRedisCache(redis Redis) {
	tt.R = redis
}

func New() *TwoTier {
	return &TwoTier{
		Marshal: func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		},
	}
}
