package boltdb

import (
	"testing"

	"github.com/docker/libkv/store"
	"github.com/stretchr/testify/assert"
)

func TestBoltDBStore(t *testing.T) {
	var succ bool
	var pair *store.KVPair

	kv, err := New([]string{"/tmp/__boltdbtest"}, &store.Config{Bucket: "boltDBTest"})
	assert.NoError(t, err)

	key := "foo"
	err = kv.Put(key, []byte("bar"), nil)
	assert.NoError(t, err)

	pair, err = kv.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(pair.Value))

	succ, pair, err = kv.AtomicPut("foo", []byte("oof"), &store.KVPair{Key: "foo", LastIndex: 1}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "oof", string(pair.Value))
	assert.Equal(t, 2, int(pair.LastIndex))

	succ, pair, err = kv.AtomicPut("foo", []byte("bar"), &store.KVPair{Key: "foo", LastIndex: 1}, nil)
	assert.Equal(t, false, succ)

	key = "foo1"
	err = kv.Put(key, []byte("bar1"), nil)

	key = "foo2"
	err = kv.Put(key, []byte("bar2"), nil)

	key = "fizz"
	err = kv.Put(key, []byte("buzz"), nil)

	kvpair, _ := kv.List("foo")
	assert.Equal(t, 3, len(kvpair))

	key = "fizz"
	succ, err = kv.Exists(key)
	assert.Equal(t, true, succ)

	key = "foo1"
	err = kv.Delete(key)
	assert.NoError(t, err)

	kv.Close()
}
