package runlevel

import (
	"testing"
	"github.com/jmhodges/levigo"
	"strings"
	"encoding/json"
	"github.com/weistn/sublevel"
	"time"
)

func TestRunlevel(t *testing.T) {
	opts := levigo.NewOptions()
	levigo.DestroyDatabase("test.ldb", opts)
	// opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("test.ldb", opts)
	if err != nil {
		t.Fatal(err)
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	sub1 := sublevel.Sublevel(db, "input")
	index := sublevel.Sublevel(db, "index")
	job := sublevel.Sublevel(db, "job")

	sub1.Post(func(key, value []byte) {
		time.Sleep(1000 * time.Millisecond)
	})

	var delay = false

	task := TriggerBefore(sub1, job, func(key, value []byte) []byte {
		if strings.HasPrefix(string(key), "Doc_") || strings.HasPrefix(string(key), "PostDoc_") {
			return key
		}
		return nil
	}, func(key, value []byte, hook *sublevel.Hook) bool {
		if delay {
			return false
		}
		doc := make(map[string]string)
		err := json.Unmarshal(value, &doc)
		if err != nil {
			t.Fatal(err)
		}
		hook.Put([]byte(doc["id"]), []byte(doc["number"]), index)
		return true
	})

	sub1.Put(wo, []byte("foobar"), []byte("do-not-process"))

	// Two concurrent put operations
	go sub1.Put(wo, []byte("Doc_1"), []byte("{\"id\":\"01234\", \"number\": \"42\"}"))
	time.Sleep(500 * time.Millisecond)
	go sub1.Put(wo, []byte("Doc_1"), []byte("{\"id\":\"01234\", \"number\": \"43\"}"))
	time.Sleep(600 * time.Millisecond)

	val, err := index.Get(ro, []byte("01234"))
	if err != nil || string(val) != "42" {
		t.Fatal(err)
	}

	time.Sleep(800 * time.Millisecond)

	val, err = index.Get(ro, []byte("01234"))
	if err != nil || string(val) != "43" {
		t.Fatal(err, string(val))
	}

	delay = true

	sub1.Put(wo, []byte("PostDoc_2"), []byte("{\"id\":\"03134\", \"number\": \"48\"}"))
	val, err = index.Get(ro, []byte("03134"))
	if err != nil || val != nil {
		t.Fatal(err)
	}

	delay = false

	task.WorkOff()

	val, err = index.Get(ro, []byte("03134"))
	if err != nil || string(val) != "48" {
		t.Fatal(err, string(val))
	}

	task.Close()

	ro.Close()
	wo.Close()
	db.Close()
}

func TestRunlevel2(t *testing.T) {
	opts := levigo.NewOptions()
	levigo.DestroyDatabase("test.ldb", opts)
	// opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("test.ldb", opts)
	if err != nil {
		t.Fatal(err)
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	sub1 := sublevel.Sublevel(db, "input")
	index := sublevel.Sublevel(db, "index")
	job := sublevel.Sublevel(db, "job")

	task := TriggerAfter(sub1, job, func(key, value []byte) []byte {
		if strings.HasPrefix(string(key), "Doc_") {
			return key
		}
		return nil
	}, func(key, value []byte) {
		doc := make(map[string]string)
		err := json.Unmarshal(value, &doc)
		if err != nil {
			t.Fatal(err)
		}
		index.Put(wo, []byte(doc["id"]), []byte(doc["number"]))
		// Make sure that the next task invocation comes in concurrently to this one
		time.Sleep(300 * time.Millisecond)
	})

	sub1.Put(wo, []byte("foobar"), []byte("do-not-process"))

	// Two put operations which will both trigger the task for the same taskKey.
	sub1.Put(wo, []byte("Doc_1"), []byte("{\"id\":\"01234\", \"number\": \"42\"}"))
	sub1.Put(wo, []byte("Doc_1"), []byte("{\"id\":\"01234\", \"number\": \"43\"}"))

	val, err := sub1.Get(ro, []byte("Doc_1"))
	if err != nil || string(val) != "{\"id\":\"01234\", \"number\": \"43\"}" {
		t.Fatal(err, string(val))
	}

	time.Sleep(800 * time.Millisecond)


	val, err = index.Get(ro, []byte("01234"))
	if err != nil || string(val) != "43" {
		t.Fatal(err, string(val))
	}

	task.Close()

	ro.Close()
	wo.Close()
	db.Close()
}
