package cache_test

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/coinread/twotier-cache/v8"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cache")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer wg.Done()
				defer GinkgoRecover()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

var _ = Describe("Codec", func() {
	const key = "mykey"
	var obj *Object

	var codec *cache.TwoTier

	testCodec := func() {
		It("Gets and Sets nil", func() {
			key := "GaSn"
			err := codec.SetStatic(key, time.Hour, nil)
			Expect(err).To(HaveOccurred())

			err = codec.Get(key, nil)
			Expect(err).To(HaveOccurred())

			Expect(codec.Exists(key)).To(BeFalse())
		})

		It("Deletes key", func() {
			key := "Dk"
			err := codec.SetStatic(key, time.Hour, true)

			Expect(err).NotTo(HaveOccurred())
			Expect(codec.Exists(key)).To(BeTrue())

			err = codec.Delete(key)
			Expect(err).NotTo(HaveOccurred())

			err = codec.Get(key, nil)
			Expect(err).To(Equal(cache.ErrCacheMiss))

			Expect(codec.Exists(key)).To(BeFalse())
		})

		It("Gets and Sets data", func() {
			key := "GaSd"
			err := codec.SetStatic(key, time.Hour, obj)

			Expect(err).NotTo(HaveOccurred())

			wanted := new(Object)
			err = codec.Get(key, wanted)
			Expect(err).NotTo(HaveOccurred())
			Expect(wanted).To(Equal(obj))

			Expect(codec.Exists(key)).To(BeTrue())
		})

		Describe("Once func", func() {
			It("calls Func when cache fails", func() {
				key := "cFwcf"
				err := codec.SetStatic(key, time.Hour, "*")

				Expect(err).NotTo(HaveOccurred())

				var got bool
				err = codec.Get(key, &got)
				Expect(err).To(MatchError("msgpack: invalid code=a1 decoding bool"))

				val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
					return true, nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(BeTrue())

				got = false
				err = codec.Get(key, &got)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(BeTrue())
			})

			It("does not cache when Func fails", func() {
				key := "dncwFf"
				perform(100, func(int) {
					val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						return nil, io.EOF
					})

					Expect(err).To(Equal(io.EOF))
					Expect(val).To(BeNil())
				})

				var got bool
				err := codec.Get(key, &got)
				Expect(err).To(Equal(cache.ErrCacheMiss))

				val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
					return true, nil
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(BeTrue())
			})

			It("works with Object", func() {
				key := "wwO"
				var callCount int64
				perform(100, func(int) {
					val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return obj, nil
					})

					Expect(err).NotTo(HaveOccurred())
					Expect(val).To(Equal(obj))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works with ptr and non-ptr", func() {
				key := "wwpanp"
				var callCount int64
				perform(100, func(int) {
					val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return *obj, nil
					})

					Expect(err).NotTo(HaveOccurred())
					Expect(val).To(Equal(*obj))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works with bool", func() {
				key := "wwb"
				var callCount int64
				perform(100, func(int) {
					val, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						atomic.AddInt64(&callCount, 1)
						return true, nil
					})

					Expect(err).NotTo(HaveOccurred())
					Expect(val).To(BeTrue())
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("works without Object and error result", func() {
				key := "wwOaer"
				var callCount int64
				perform(100, func(int) {
					_, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						time.Sleep(100 * time.Millisecond)
						atomic.AddInt64(&callCount, 1)
						return nil, errors.New("error stub")
					})
					Expect(err).To(MatchError("error stub"))
				})
				Expect(callCount).To(Equal(int64(1)))
			})

			It("does not cache error result", func() {
				key := "dncer"
				var callCount int64
				do := func(sleep time.Duration) (int, error) {
					n, err := codec.Set(key, time.Hour, func() (interface{}, error) {
						time.Sleep(sleep)

						n := atomic.AddInt64(&callCount, 1)
						if n == 1 {
							return nil, errors.New("error stub")
						}
						return 42, nil
					})

					if err != nil {
						return 0, err
					}
					return n.(int), nil
				}

				perform(100, func(int) {
					n, err := do(100 * time.Millisecond)
					Expect(err).To(MatchError("error stub"))
					Expect(n).To(Equal(0))
				})

				perform(100, func(int) {
					n, err := do(0)
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(42))
				})

				Expect(callCount).To(Equal(int64(2)))
			})
		})
	}

	BeforeEach(func() {
		obj = &Object{
			Str: "mystring",
			Num: 42,
		}
	})

	Context("L + R", func() {
		BeforeEach(func() {
			codec = newTieredCache()
		})

		testCodec()
	})
})

func newRing() *redis.Ring {
	return redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"master": "127.0.0.1:6379",
		},
	})
}

func newTieredCache() *cache.TwoTier {
	ring := newRing()
	_ = ring.ForEachShard(func(client *redis.Client) error {
		return client.FlushDB().Err()
	})

	return cache.New(ring)
}
