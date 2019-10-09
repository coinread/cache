package cache_test

import (
	"fmt"
	"time"
)

type Object struct {
	Str string
	Num int
}

func Example_basicUsage() {

	codec := newTieredCache()

	key := "basicUsage"
	obj := &Object{
		Str: "mystring",
		Num: 42,
	}

	_ = codec.SetStatic(key, time.Hour, obj)

	var wanted Object
	if err := codec.Get(key, &wanted); err == nil {
		fmt.Println(wanted)
	}

	// Output: {mystring 42}
}

func Example_advancedUsage() {
	codec := newTieredCache()
	key := "advancedUsage"

	obj := new(Object)
	err := codec.SetStatic(key, time.Hour, &Object{
		Str: "mystring",
		Num: 42,
	})

	if err != nil {
		panic(err)
	}

	fmt.Println(obj)
	// Output: &{mystring 42}
}
