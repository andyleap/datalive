package datalive

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/etcd-io/bbolt"
)

var (
	dataBucket    = []byte("data")
	indexesBucket = []byte("indexes")
	configKey     = []byte("config")
)

var (
	ErrInternal           = errors.New("internal error occured")
	ErrUniqueIndexOverlap = errors.New("unique index value already exists")
	ErrIndexDoesNotExist  = errors.New("index does not exist")
)

type DataLive struct {
	db *bbolt.DB

	typs map[string]*Typ
	mu   sync.RWMutex
}

func Open(file string) (*DataLive, error) {
	db, err := bbolt.Open(file, 0600, &bbolt.Options{})
	if err != nil {
		return nil, err
	}
	return &DataLive{
		db: db,

		typs: map[string]*Typ{},
	}, nil
}

func (dl *DataLive) getType(tx *bbolt.Tx, typ string) (*Typ, error) {
	dl.mu.RLock()
	t, ok := dl.typs[typ]
	dl.mu.RUnlock()
	if ok {
		return t, nil
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()
	t, ok = dl.typs[typ]
	if ok {
		return t, nil
	}

	c := &Config{}
	b := tx.Bucket([]byte(typ))

	if b != nil {
		data := b.Get(configKey)
		if len(data) > 0 {
			err := json.Unmarshal(data, &c)
			if err != nil {
				return nil, err
			}
		}
	}
	t = &Typ{
		c: c,
		objects: &topics{
			w: map[string]*watchers{},
		},
		indexes: map[string]*topics{},
	}
	dl.typs[typ] = t
	return t, nil
}

type Typ struct {
	c   *Config
	cmu sync.RWMutex

	objects *topics

	indexes map[string]*topics
	iwmu    sync.RWMutex
}

func (t *Typ) getIndexTopics(index string) *topics {
	t.iwmu.RLock()
	it, ok := t.indexes[index]
	t.iwmu.RUnlock()
	if ok {
		return it
	}
	t.iwmu.Lock()
	defer t.iwmu.Unlock()
	it, ok = t.indexes[index]
	if ok {
		return it
	}
	it = &topics{
		w: map[string]*watchers{},
	}
	t.indexes[index] = it
	return it
}

type Config struct {
	Indexes Indexes
}

func (dl *DataLive) Set(typ string, key string, value interface{}) error {
	return dl.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(typ))
		if err != nil {
			return err
		}
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.RLock()
		defer t.cmu.RUnlock()

		datab, err := b.CreateBucketIfNotExists(dataBucket)
		if err != nil {
			return err
		}
		if len(t.c.Indexes) > 0 {
			old := datab.Get([]byte(key))
			if len(old) > 0 {
				var i interface{}
				err = json.Unmarshal(old, &i)
				if err != nil {
					return err
				}
				err = t.c.Indexes.Clear(b, t, key, i)
				if err != nil {
					return err
				}
			}
			err = t.c.Indexes.Set(b, t, key, value)
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		t.objects.Send(key, value)
		return datab.Put([]byte(key), raw)
	})
}

func (dl *DataLive) Delete(typ string, key string) error {
	return dl.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(typ))
		if err != nil {
			return err
		}
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.RLock()
		defer t.cmu.RUnlock()

		datab, err := b.CreateBucketIfNotExists(dataBucket)
		if err != nil {
			return err
		}
		if len(t.c.Indexes) > 0 {
			old := datab.Get([]byte(key))
			if len(old) > 0 {
				var i interface{}
				err = json.Unmarshal(old, &i)
				if err != nil {
					return err
				}
				err = t.c.Indexes.Clear(b, t, key, i)
				if err != nil {
					return err
				}
			}
		}
		t.objects.Close(key)
		return datab.Delete([]byte(key))
	})
}

func (dl *DataLive) Get(typ string, key string) (val interface{}, err error) {
	err = dl.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(typ))
		if b == nil {
			return nil
		}
		datab := b.Bucket(dataBucket)
		if datab == nil {
			return nil
		}
		data := datab.Get([]byte(key))
		if len(data) == 0 {
			return nil
		}
		return json.Unmarshal(data, &val)
	})
	return val, err
}

func (dl *DataLive) Index(typ string, index string, val interface{}) (keys []string, err error) {
	err = dl.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(typ))
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.RLock()
		defer t.cmu.RUnlock()
		if err != nil {
			return err
		}
		for _, i := range t.c.Indexes {
			if i.Name == index {
				keys, err = i.Get(b, val)
				return err
			}
		}
		return ErrIndexDoesNotExist
	})
	return keys, err
}

func (dl *DataLive) CreateIndex(typ string, index string, config IndexConfig) error {
	return dl.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(typ))
		if err != nil {
			return err
		}
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.Lock()
		defer t.cmu.Unlock()
		for i := range t.c.Indexes {
			if t.c.Indexes[i].Name == index {
				return nil
			}
		}
		i := Index{
			Name:        index,
			Split:       JSONPathSplit(index),
			IndexConfig: config,
		}
		t.c.Indexes = append(t.c.Indexes, i)
		newc, err := json.Marshal(t.c)
		if err != nil {
			return err
		}
		err = b.Put(configKey, newc)
		if err != nil {
			return err
		}

		datab, err := b.CreateBucketIfNotExists(dataBucket)
		if err != nil {
			return err
		}

		return datab.ForEach(func(k, v []byte) error {
			var val interface{}
			if len(v) == 0 {
				return nil
			}
			err := json.Unmarshal(v, &val)
			if err != nil {
				return err
			}
			return i.Set(b, t, string(k), val)
		})
	})
}

func (dl *DataLive) DeleteIndex(typ string, index string) error {
	return dl.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(typ))
		if err != nil {
			return err
		}
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.Lock()
		defer t.cmu.Unlock()
		for i := range t.c.Indexes {
			if t.c.Indexes[i].Name == index {
				t.c.Indexes[i] = t.c.Indexes[len(t.c.Indexes)-1]
				t.c.Indexes = t.c.Indexes[:len(t.c.Indexes)-1]
			}
		}
		newc, err := json.Marshal(t.c)
		if err != nil {
			return err
		}
		err = b.Put(configKey, newc)
		if err != nil {
			return err
		}
		indexb, err := b.CreateBucketIfNotExists(indexesBucket)
		if err != nil {
			return err
		}
		return indexb.DeleteBucket([]byte(index))
	})
}

func (dl *DataLive) Watch(typ string, key string) (c chan interface{}, err error) {
	err = dl.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(typ))
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		datab := b.Bucket(dataBucket)
		c = t.objects.Watch(key, make(chan interface{}, 5))
		var i interface{}
		json.Unmarshal(datab.Get([]byte(key)), &i)
		c <- i
		return nil
	})
	return c, err
}

func (dl *DataLive) WatchIndex(typ string, index string, value interface{}) (c chan interface{}, err error) {
	err = dl.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(typ))
		t, err := dl.getType(tx, typ)
		if err != nil {
			return err
		}
		t.cmu.RLock()
		defer t.cmu.RUnlock()
		if err != nil {
			return err
		}
		val, err := json.Marshal(value)
		if err != nil {
			return err
		}
		keys := []string{}
		for _, i := range t.c.Indexes {
			if i.Name == index {
				keys, err = i.Get(b, value)
				if err != nil {
					return err
				}
			}
		}

		c = t.getIndexTopics(index).Watch(string(val), make(chan interface{}, 5))

		c <- keys
		return nil
	})
	return c, err
}

func (dl *DataLive) Close() {
	dl.db.Close()
	for _, typ := range dl.typs {
		typ.objects.CloseAll()
		for _, index := range typ.indexes {
			index.CloseAll()
		}
	}
}
