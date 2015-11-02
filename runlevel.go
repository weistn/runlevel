package runlevel

import (
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"github.com/weistn/uniclock"
	"fmt"
	"sync"
	"bytes"
)

type FilterFunc func(key, value []byte) []byte
type ProcessFunc func(key, value []byte) bool

type Task struct {
	db *sublevel.DB
	filter FilterFunc
	processFunc ProcessFunc
	taskDb *sublevel.DB
	wo *levigo.WriteOptions
	ro *levigo.ReadOptions
	running map[string]bool
	runningMutex sync.Mutex
	closeMutex sync.Mutex
	pre sublevel.PreFunc
	post sublevel.PostFunc
}

func Trigger(db *sublevel.DB, taskDb *sublevel.DB, filter FilterFunc, taskfunc ProcessFunc) *Task {
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	task := &Task{db: db, filter: filter, processFunc: taskfunc, taskDb: taskDb, wo: wo, ro: ro, running: make(map[string]bool)}

	var run func(taskKey []byte)
	run = func(taskKey []byte) {
		taskKeyStr := string(taskKey)
		var ok bool
//		println("RUN", string(key), string(value), string(taskKey))
		hookfunc := func(key, value []byte, hook *sublevel.Hook) {
			ok = taskfunc(key, value)
		}

		for {
			it := task.taskDb.NewIterator(task.ro)
			for it.Seek(taskKey); it.Valid(); it.Next() {
				if !bytes.HasPrefix(it.Key(), taskKey) || len(taskKey) + 16 != len(it.Key()) {
					break
				}
				key := it.Value()
				val, err := task.db.Get(task.ro, it.Value())
				if err != nil {
					continue
				}
				if len(val) == 0 {
					val = nil
				}
//				println("RUN", string(key), string(val), string(taskKey))
				// This lock avoids that the processing is interrupted by a call to Close()
				task.closeMutex.Lock()
				// Execute taskfunc in the context of a new hook, commit to disk, then call after
				ok = true
				db.RunHook(wo, hookfunc, nil, key, val)
				if ok {
					taskDb.Delete(wo, it.Key())
				}
				task.closeMutex.Unlock()
			}
			it.Close()

			task.runningMutex.Lock()
			state := task.running[taskKeyStr]
			if !state {
				delete(task.running, taskKeyStr)
				task.runningMutex.Unlock()
				break
			}
			task.running[taskKeyStr] = false
			task.runningMutex.Unlock()
		}
	}

	// Hook into the db to watch for changes
	task.pre = func(key, value []byte, hook *sublevel.Hook) {
//		println("PRE", string(key), string(value))
		// Is this change relevant?
		taskKey := filter(key, value)
		if taskKey == nil {
			return
		}

		// Write a DB row so the task is not forgotten if the system is terminated now
		now := uniclock.Next()
		nowBytes := []byte(fmt.Sprintf("%016x", now))
		hook.Put(append(taskKey, nowBytes...), key, taskDb)
	}

	task.post = func(key, value []byte) {
//		println("POST", string(key), string(value))
		// Is this change relevant?
		taskKey := filter(key, value)
		if taskKey == nil {
			return
		}
		taskKeyStr := string(taskKey)

//		println("POSTtask", "'" + taskKeyStr + "'")
		task.runningMutex.Lock()
		defer task.runningMutex.Unlock()
		if _, ok := task.running[taskKeyStr]; ok {
			task.running[taskKeyStr] = true
		} else {
			task.running[taskKeyStr] = false
			go run(taskKey)
		}
	}

	db.Pre(task.pre)
	db.Post(task.post)

	return task
}

func (this *Task) WorkOff() (err error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	it := this.taskDb.NewIterator(ro)
	var val []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		val, err = this.db.Get(ro, it.Value())
		if err != nil {
			continue
		}
		err = this.db.RunHook(wo, nil, this.post, it.Value(), val)
		if err != nil {
			return
		}
		err = this.taskDb.Delete(wo, it.Key())
		if err != nil {
			return
		}
	}
	return
}

func (this *Task) Close() {
	this.closeMutex.Lock()
	this.wo.Close()
	this.ro.Close()
	this.wo = nil
	this.closeMutex.Unlock()
}
