package runlevel

import (
	"testing"
	"github.com/jmhodges/levigo"
	"strings"
	"encoding/json"
	"github.com/weistn/sublevel"
	"time"
)

func TestTrigger(t *testing.T) {
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

	task := Trigger(sub1, job, func(key, value []byte) []byte {
		if strings.HasPrefix(string(key), "Doc_") {
			return key
		}
		return nil
	}, func(key, value []byte) bool {
		doc := make(map[string]string)
		err := json.Unmarshal(value, &doc)
		if err != nil {
			t.Fatal(err)
		}
		index.Put(wo, []byte(doc["id"]), []byte(doc["number"]))
		// Make sure that the next task invocation comes in concurrently to this one
		time.Sleep(300 * time.Millisecond)
		return true
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
