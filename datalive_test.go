package datalive

import (
	"os"
	"reflect"
	"testing"
)

func testFrame(t *testing.T, f func(t *testing.T, dl *DataLive) error) {
	dl, err := Open("test.db")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		dl.Close()
		os.Remove("test.db")
	}()
	err = f(t, dl)
	if err != nil {
		t.Error(err)
	}
}

func TestSimple(t *testing.T) {
	testFrame(t, func(t *testing.T, dl *DataLive) error {
		data := map[string]interface{}{"b": "c"}
		err := dl.Set("test", "a", data)
		if err != nil {
			return err
		}

		ret, err := dl.Get("test", "a")
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(data, ret) {
			t.Error(ret, "!=", data)
		}
		return nil
	})
}

func TestIndex(t *testing.T) {
	testFrame(t, func(t *testing.T, dl *DataLive) error {
		data := map[string]interface{}{"b": "c"}
		err := dl.Set("test", "a", data)
		if err != nil {
			return err
		}

		err = dl.CreateIndex("test", "b", IndexConfig{})
		if err != nil {
			return err
		}

		err = dl.Set("test", "b", data)
		if err != nil {
			return err
		}

		err = dl.Delete("test", "a")
		if err != nil {
			return err
		}

		err = dl.Set("test", "c", data)
		if err != nil {
			return err
		}

		data["b"] = "d"
		err = dl.Set("test", "c", data)
		if err != nil {
			return err
		}

		keys, err := dl.Index("test", "b", "c")
		if err != nil {
			return err
		}

		if len(keys) != 1 || keys[0] != "b" {
			t.Error("invalid index response")
		}

		err = dl.DeleteIndex("test", "b")
		if err != nil {
			return err
		}

		return nil
	})
}

func TestWatch(t *testing.T) {
	testFrame(t, func(t *testing.T, dl *DataLive) error {
		data := map[string]interface{}{"b": "c"}
		err := dl.Set("test", "a", data)
		if err != nil {
			return err
		}

		c, err := dl.Watch("test", "a")

		if err != nil {
			return err
		}

		data["d"] = "e"

		dl.Set("test", "a", data)

		a := <-c
		b := <-c

		if reflect.DeepEqual(a, data) {
			t.Error(a, "==", data)
		}

		if !reflect.DeepEqual(b, data) {
			t.Error(b, "!=", data)
		}

		return nil
	})
}

func TestIndexWatch(t *testing.T) {
	testFrame(t, func(t *testing.T, dl *DataLive) error {
		err := dl.CreateIndex("test", "b", IndexConfig{})
		if err != nil {
			return err
		}

		c, err := dl.WatchIndex("test", "b", "c")
		if err != nil {
			return err
		}

		data := map[string]interface{}{"b": "c"}
		err = dl.Set("test", "a", data)
		if err != nil {
			return err
		}

		dl.Delete("test", "a")
		if err != nil {
			return err
		}

		ret := <-c

		if reflect.DeepEqual(ret, []string{}) {
			t.Error("got", ret, "expected", "[]")
		}

		ret = <-c

		if reflect.DeepEqual(ret, []string{"a"}) {
			t.Error("got", ret, "expected", "[\"a\"]")
		}

		ret = <-c

		if reflect.DeepEqual(ret, []string{}) {
			t.Error("got", ret, "expected", "[]")
		}

		return nil
	})
}

func TestIndexExclude(t *testing.T) {
	testFrame(t, func(t *testing.T, dl *DataLive) error {
		data := map[string]interface{}{"b": "c"}
		err := dl.Set("test", "a", data)
		if err != nil {
			return err
		}

		err = dl.CreateIndex("test", "b", IndexConfig{Exclude: []interface{}{"c"}})
		if err != nil {
			return err
		}

		err = dl.Set("test", "b", data)
		if err != nil {
			return err
		}

		keys, err := dl.Index("test", "b", "c")
		if err != nil {
			return err
		}

		if len(keys) != 0 {
			t.Error("invalid index response")
		}

		return nil
	})
}
