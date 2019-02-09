package cache

import (
	"github.com/coocood/freecache"
)

var kash *freecache.Cache

func init() {
	cacheSize := 100 * 1024 * 1024
	kash = freecache.NewCache(cacheSize)
}

//Set save val in cache
func Set(key string, val []byte, expire int) {
	kash.Set([]byte(key), val, expire)
}

//Get value from cache
func Get(key string) ([]byte, error) {
	return kash.Get([]byte(key))
}

//Del delete from cache
func Del(key string) bool {
	return kash.Del([]byte(key))
}
