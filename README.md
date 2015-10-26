# Taskrunner for LevelDB

Runlevel builds on the sublevel library, which in turn is a thin wrapper around LevelDB.
Using Runlevel, you can install triggers on a sublevel.
That is, whenever a row is inserted or deleted, the trigger function is invoked.
It is possible to trigger a function before and after the rows are committed to disk (to be precise: handed over to the operating system).

Triggers are ensured to eventually complete.
That means, the system could crash after committing to disk but before executing the trigger.
When the system is restarted, all missing triggers can be executed.
This is achieved by writing log rows to LevelDB, telling that some trigger has to run.
Upon completion of the trigger, this log rows is removed.

Furthermore, runlevel serialized triggers related to the same key.
If a key is changed multiple times concurrently or in short succession, runlevel ensures that the triggers are never executed in parallel for the same key.
This is very helpful to avoid races.
However, runlevel does not lock the entire DB.
Locks are really just on a per-key basis.
As a result, concurrency is still possible, which allows for better performance on multiple cores under high load. 