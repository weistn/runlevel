package runlevel

import (
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"fmt"
	"time"
	"sync"
)

type FilterFunc func(key, value []byte) []byte

// Returns true if the task has been completed, or false otherwise.
// In the case of false, an entry is written to the DB to note that this task still needs doing.
type TaskPreFunc func(key, value []byte, hook *sublevel.Hook) bool
type TaskPostFunc func(key, value []byte)

type taskState struct {
	// A negative value means that a computation is running and has not yet committed to disk.
	n int
	// Used by TriggerAfter only
	tainted bool
	time int64
}

type Task struct {
	db *sublevel.DB
	filter FilterFunc
	taskPreFunc TaskPreFunc
	taskPostFunc TaskPostFunc
	taskDb *sublevel.DB
	wo *levigo.WriteOptions
	ro *levigo.ReadOptions
	running map[string]taskState
	runningMutex sync.Mutex
	closeMutex sync.Mutex
	pre sublevel.PreFunc
	post sublevel.PostFunc
}

func TriggerBefore(db *sublevel.DB, taskDb *sublevel.DB, filter FilterFunc, taskfunc TaskPreFunc) *Task {
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	task := &Task{db: db, filter: filter, taskPreFunc: taskfunc, taskDb: taskDb, wo: wo, ro: ro, running: make(map[string]taskState)}

	// Hook into the db to watch for changes
	task.pre = func(key, value []byte, hook *sublevel.Hook) {
//		println("PRE", string(key), string(value))
		// Is this change relevant?
		taskKey := filter(key, value)
		if taskKey == nil {
			return
		}
		// Mark that this task is currently working on 'key'.
		// This keeps 'WorkOff' from munching on the same key.
		// However, it is fine if this function is executed in multiple go-routines, since the output of the task is written
		// atomically with the data is has been working on. So 'last-write-wins' is perfectly ok.
		task.runningMutex.Lock()
		now := time.Now().Unix()
		if state, ok := task.running[string(taskKey)]; ok {
			if state.n == -1 {
				// Write a DB row so the task is not forgotten if the system is terminated now
				nowBytes := []byte(fmt.Sprintf("%d", now))
				taskDb.Put(wo, append(taskKey, nowBytes...), key)
				task.running[string(taskKey)] = taskState{2, false, now}
			} else {
				task.running[string(taskKey)] = taskState{state.n + 1, false, state.time}
			}
			task.runningMutex.Unlock()
			return
		} else {
			task.running[string(taskKey)] = taskState{-1, false, now}
		}
		task.runningMutex.Unlock()

		// Compute
		done := taskfunc(key, value, hook)
		if !done {
			// Add a row to the DB that marks this task as 'needs-to-be-executed-later'.
			now := time.Now().Unix()
			nowBytes := []byte(fmt.Sprintf("%d", now))
			hook.Put(append(taskKey, nowBytes...), key, taskDb)
		}
	}

	task.post = func(key, value []byte) {
//		println("POST", string(key), string(value))
		// Is this change relevant?
		taskKey := filter(key, value)
		if taskKey == nil {
			return
		}

		task.runningMutex.Lock()
		state := task.running[string(taskKey)]
		if state.n == -1 || state.n == 1 {
			delete(task.running, string(taskKey))
		} else {
			task.running[string(taskKey)] = taskState{state.n - 1, false, state.time}				
		}
		task.runningMutex.Unlock()

		if state.n == 1 {
			// There have been multiple interleaving invokations of the task from concurrent transactions.
			// The first invokation caused the TaskFunc to execute, but most likely on old data.
			// This is the last of these transactions that has completed.
			// The task has to be re-run for the key to compute it based on the latest data.
			val, err := db.Get(ro, key)
			if err != nil {
				return
			}
			db.RunHook(wo, task.pre, task.post, key, val)
			// Delete the DB row, because all pending tasks for this key have been executed.
			nowBytes := []byte(fmt.Sprintf("%d", state.time))
			taskDb.Delete(wo, append(taskKey, nowBytes...))
		}
	}

	db.Pre(task.pre)
	db.Post(task.post)

	return task
}

func TriggerAfter(db *sublevel.DB, taskDb *sublevel.DB, filter FilterFunc, taskfunc TaskPostFunc) *Task {
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	task := &Task{db: db, filter: filter, taskPostFunc: taskfunc, taskDb: taskDb, wo: wo, ro: ro, running: make(map[string]taskState)}

	var run func(key, value, taskKey []byte)
	run = func(key, value, taskKey []byte) {
		hookfunc := func(key, value []byte, hook *sublevel.Hook) {
			taskfunc(key, value)
		}
		// Execute taskfunc in the context of a new hook, commit to disk, then call after
		db.RunHook(wo, hookfunc, nil, key, value)

		task.runningMutex.Lock()
		state := task.running[string(taskKey)]
		nowBytes := []byte(fmt.Sprintf("%d", state.time))
		if state.tainted {
			task.running[string(taskKey)] = taskState{state.n, false, state.time}
			val, err := db.Get(ro, key)
			if err != nil {
				return
			}
//			println("Running tainted", string(key), string(val))
			task.runningMutex.Unlock()
			go run(key, val, taskKey)
			return
		} else if state.n == -1 {
			delete(task.running, string(taskKey))
		} else {
			task.running[string(taskKey)] = taskState{state.n, false, state.time}	
		}
		task.runningMutex.Unlock()

		if state.n == -1 {
			taskDb.Delete(wo, append(taskKey, nowBytes...))
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
		task.runningMutex.Lock()
		defer task.runningMutex.Unlock()
		now := time.Now().Unix()
		if state, ok := task.running[string(taskKey)]; ok {
			if state.n < 0 {
				task.running[string(taskKey)] = taskState{state.n - 1, false, state.time}
			} else {
				task.running[string(taskKey)] = taskState{state.n + 1, false, state.time}					
			}
		} else {
			// Write a DB row so the task is not forgotten if the system is terminated now
			nowBytes := []byte(fmt.Sprintf("%d", now))
			taskDb.Put(wo, append(taskKey, nowBytes...), key)
			task.running[string(taskKey)] = taskState{2, false, now}
		}
	}

	task.post = func(key, value []byte) {
//		println("POST", string(key), string(value))
		// Is this change relevant?
		taskKey := filter(key, value)
		if taskKey == nil {
			return
		}

		task.runningMutex.Lock()
		state := task.running[string(taskKey)]
		if state.n == 2 {
			task.running[string(taskKey)] = taskState{-1, state.tainted, state.time}
			go run(key, value, taskKey)
		} else if state.n < 0 {
			task.running[string(taskKey)] = taskState{state.n + 1, true, state.time}				
		} else {
			task.running[string(taskKey)] = taskState{state.n - 1, state.tainted, state.time}
		}
		task.runningMutex.Unlock()
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
		err = this.db.RunHook(wo, this.pre, this.post, it.Value(), val)
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
