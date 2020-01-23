package datalive

import (
	"bytes"
	"encoding/json"

	"github.com/etcd-io/bbolt"
)

type Indexes []Index

func (is Indexes) Set(b *bbolt.Bucket, typ *Typ, key string, data interface{}) error {
	for _, i := range is {
		err := i.Set(b, typ, key, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (is Indexes) Clear(b *bbolt.Bucket, typ *Typ, key string, data interface{}) error {
	for _, i := range is {
		err := i.Clear(b, typ, key, data)
		if err != nil {
			return err
		}
	}
	return nil
}

type Index struct {
	Name  string
	Split []string
	IndexConfig
}

type IndexConfig struct {
	Unique  bool
	Exclude []interface{}
}

func (i Index) Set(b *bbolt.Bucket, typ *Typ, key string, data interface{}) error {
	indexesb, err := b.CreateBucketIfNotExists(indexesBucket)
	if err != nil {
		return err
	}
	ib, err := indexesb.CreateBucketIfNotExists([]byte(i.Name))
	if err != nil {
		return err
	}
	indexVal := jsonPath(data, i.Split)

	indexValRaw, err := json.Marshal(indexVal)
	if err != nil {
		return err
	}

	for _, exclude := range i.Exclude {
		excludeRaw, err := json.Marshal(exclude)
		if err != nil {
			return err
		}
		if bytes.Equal(excludeRaw, indexValRaw) {
			return nil
		}
	}

	if i.Unique {
		key := ib.Get(indexValRaw)
		if !bytes.Equal(key, indexValRaw) {
			return ErrUniqueIndexOverlap
		}
		typ.getIndexTopics(i.Name).Send(string(indexValRaw), []string{string(key)})
		return ib.Put(indexValRaw, []byte(key))
	}

	keys := ib.Get(indexValRaw)
	keySet := Set{}
	if len(keys) > 0 {
		err = json.Unmarshal(keys, &keySet)
		if err != nil {
			return err
		}
	}
	keySet.Set((key))
	keys, err = json.Marshal(keySet)
	if err != nil {
		return err
	}
	typ.getIndexTopics(i.Name).Send(string(indexValRaw), []string(keySet))
	return ib.Put(indexValRaw, keys)
}

func (i Index) Clear(b *bbolt.Bucket, typ *Typ, key string, data interface{}) error {
	indexesb, err := b.CreateBucketIfNotExists(indexesBucket)
	if err != nil {
		return err
	}
	ib, err := indexesb.CreateBucketIfNotExists([]byte(i.Name))
	if err != nil {
		return err
	}
	indexVal := jsonPath(data, i.Split)

	indexValRaw, err := json.Marshal(indexVal)
	if err != nil {
		return err
	}

	if i.Unique {
		typ.getIndexTopics(i.Name).Send(string(indexValRaw), []string{})
		return ib.Delete(indexValRaw)
	}

	keys := ib.Get(indexValRaw)
	keySet := Set{}
	if len(keys) > 0 {
		err = json.Unmarshal(keys, &keySet)
		if err != nil {
			return err
		}
	}
	keySet.Clear(key)
	if len(keySet) == 0 {
		typ.getIndexTopics(i.Name).Send(string(indexValRaw), []string{})
		return ib.Delete(indexValRaw)
	}
	keys, err = json.Marshal(keySet)
	if err != nil {
		return err
	}
	typ.getIndexTopics(i.Name).Send(string(indexValRaw), []string(keySet))
	return ib.Put(indexValRaw, keys)
}

func (i Index) Get(b *bbolt.Bucket, data interface{}) (keys []string, err error) {
	indexesb := b.Bucket([]byte(indexesBucket))
	if indexesb == nil {
		return []string{}, nil
	}
	ib := indexesb.Bucket([]byte(i.Name))
	if ib == nil {
		return []string{}, nil
	}
	val, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if i.Unique {
		key := ib.Get(val)
		if key != nil {
			return []string{string(key)}, nil
		}
		return nil, nil
	}
	keysRaw := ib.Get(val)
	if len(keysRaw) == 0 {
		return []string{}, nil
	}
	err = json.Unmarshal(keysRaw, &keys)
	if err != nil {
		return nil, err
	}
	return keys, nil
}
