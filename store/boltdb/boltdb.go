package boltdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/docker/libkv/store"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for
	// BoltDB. Endpoint has to be a local file path
	ErrMultipleEndpointsUnsupported = errors.New("boltdb supports one endpoint and should be a file path")
	// specified BoltBD bucker doesn't exist in the DB
	ErrBoltBucketNotFound = errors.New("boltdb bucket doesn't exist")
	// For BoltBD Bucket is a mandatory parameter
	ErrBoltBucketOptionMissing = errors.New("boltdb bucket parameter not passed")
	// Error retruned for APIs unsupported by BoltBD
	ErrBoltAPIUnsupported = errors.New("API not supported by BoltDB backend")
)

type BoltDB struct {
	client      *bolt.DB
	bolt_bucket []byte
	dbIndex     uint64
}

const (
	libkvmetadatalen = 8
)

func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	if (options == nil) || (len(options.Bucket) == 0) {
		return nil, ErrBoltBucketOptionMissing
	}

	db, err := bolt.Open(endpoints[0], 0644, nil)
	if err != nil {
		return nil, err
	}

	b := &BoltDB{}

	b.client = db
	b.bolt_bucket = []byte(options.Bucket)
	return b, nil
}

// Get the value at "key". BoltDB doesn't provide an inbuilt last modified index with ever kv pair. Its implemented by
// by a atomic counter maintained by the libkv  and appened to the value passed by the client.
func (b *BoltDB) Get(key string) (*store.KVPair, error) {
	var val []byte

	db := b.client
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			return (ErrBoltBucketNotFound)
		}

		val = bucket.Get([]byte(key))

		return nil
	})

	if err != nil {
		return nil, err
	}

	dbIndex := binary.LittleEndian.Uint64(val[:libkvmetadatalen])
	val = val[libkvmetadatalen:]

	return &store.KVPair{Key: key, Value: val, LastIndex: (dbIndex)}, nil
}

//Insert the key, value pair. index number metadata is prepended to the value
func (b *BoltDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	db := b.client
	dbval := make([]byte, libkvmetadatalen)

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(b.bolt_bucket)
		if err != nil {
			return err
		}

		atomic.AddUint64(&b.dbIndex, 1)

		binary.LittleEndian.PutUint64(dbval, b.dbIndex)
		dbval = append(dbval, value...)

		err = bucket.Put([]byte(key), dbval)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

//Delete the value for the given key.
func (b *BoltDB) Delete(key string) error {
	db := b.client

	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			return (ErrBoltBucketNotFound)
		}
		err := bucket.Delete([]byte(key))
		return err
	})
	return err
}

// Exists checks if the key exists inside the store
func (b *BoltDB) Exists(key string) (bool, error) {
	var val []byte

	db := b.client
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			return (ErrBoltBucketNotFound)
		}

		val = bucket.Get([]byte(key))

		return nil
	})

	if len(val) == 0 {
		return false, err
	} else {
		return true, err
	}
}

func (b *BoltDB) List(directory string) ([]*store.KVPair, error) {
	kv := []*store.KVPair{}

	db := b.client
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			return (ErrBoltBucketNotFound)
		}

		cursor := bucket.Cursor()
		prefix := []byte(directory)

		for key, val := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, val = cursor.Next() {

			dbIndex := binary.LittleEndian.Uint64(val[:libkvmetadatalen])
			val = val[libkvmetadatalen:]

			kv = append(kv, &store.KVPair{
				Key:       string(key),
				Value:     val,
				LastIndex: dbIndex,
			})
		}
		return nil
	})
	return kv, err
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (b *BoltDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	var val []byte
	var dbIndex uint64

	db := b.client

	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			return ErrBoltBucketNotFound
		}

		val = bucket.Get([]byte(key))
		dbIndex = binary.LittleEndian.Uint64(val[:libkvmetadatalen])
		if dbIndex != previous.LastIndex {
			return store.ErrKeyModified
		}
		err := bucket.Delete([]byte(key))
		return err
	})
	if err != nil {
		return false, err
	} else {
		return true, err
	}
}

// AtomicPut puts a value at "key" if the key has not been
// modified since the last Put, throws an error if this is the case
func (b *BoltDB) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	var val []byte
	var dbIndex uint64
	dbval := make([]byte, libkvmetadatalen)

	db := b.client

	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket(b.bolt_bucket)
		if bucket == nil {
			if previous != nil {
				return ErrBoltBucketNotFound
			} else {
				bucket, err = tx.CreateBucket(b.bolt_bucket)
				if err != nil {
					return err
				}
			}
		}
		// check the current value of last modified index with what is passed
		val = bucket.Get([]byte(key))
		dbIndex = binary.LittleEndian.Uint64(val[:libkvmetadatalen])
		if dbIndex != previous.LastIndex {
			return store.ErrKeyModified
		}
		atomic.AddUint64(&b.dbIndex, 1)
		binary.LittleEndian.PutUint64(dbval, b.dbIndex)
		dbval = append(dbval, value...)
		return (bucket.Put([]byte(key), dbval))
	})
	if err != nil {
		return false, nil, err
	} else {
		return true, &store.KVPair{key, value, dbIndex}, nil
	}
}

func (b *BoltDB) Close() {
	db := b.client

	db.Close()
}

// TBD
func (s *BoltDB) DeleteTree(directory string) error {
	return nil
}

func (s *BoltDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, ErrBoltAPIUnsupported
}

func (s *BoltDB) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, ErrBoltAPIUnsupported
}

func (s *BoltDB) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, ErrBoltAPIUnsupported
}
