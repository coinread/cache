package cache_test

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkOnce(b *testing.B) {
	tier := newTieredCache()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			val, err := tier.Set("bench-once", time.Minute, func() (i interface{}, e error) {
				i = 42
				e = nil
				return
			})
			if err != nil {
				panic(err)
			}
			if val != 42 {
				panic(fmt.Sprintf("%d != 42", val))
			}
		}
	})
}
