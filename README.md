#Task Store
by Chris Monson

A simple, no-frills place to put tasks with safe coordination semantics.

```
  go install http://code.google.com/p/entrogo/keyheap
  go install http://code.google.com/p/entrogo/taskstore
```

If you want the client and server code:

```
  go install http://code.google.com/p/entrogo/taskstore/service/protocol
  go install http://code.google.com/p/entrogo/taskstore/service/client
  go install http://code.google.com/p/entrogo/taskstore/service/server
```

Of course, you can always clone the entire repository (and perhaps get more than you bargained for - reorganization is badly needed and will hopefully come soon - today is May 19, 2014).

The TaskStore is fault-tolerant transactional task maintenance software. Basically, it allows you to have multiple workers creating and consuming small chunks of data with some hard guarantees on consistency. This enables you, for example, to create things like a MapReduce framework: input data is transformed into map tasks, map workers consume tasks so long as there are any to consume, they then produce tasks corresponding to their map output, and reduce workers pick up those tasks and emit output data. Central to all of this is a process flow that is based on the production and consumption of tasks, all done in a fault-tolerant way that maintains consistency through multiple parallel accessors, crashes, and evictions.

It has been designed to be as modular and simple as possible, only implementing what is needed to fulfill the needed guarantees. This simplicity not only makes it easy to reason about so that it can be used correctly, but also makes it easier to maintain while preserving correctness. It is just a transactional task store, and does not implement anything else like a message service, nor does it depend on one. A sample server process has been included, as has a sample client library. These are by no means the only way in which the TaskStore can be used, but they do give an idea of how one might use it, and they can be used as is for many needs.

#Introduction

The TaskStore is divided into three basic pieces:

  * TaskStore: basic in-process task manipulation.
  * Service: exposes an API over the network for task manipulation - uses the TaskStore.
  * Client: library that exposes Go calls to use the service.

The TaskStore is further divided into task management and journaling. The journaler is an interface that you can implement and pass into the TaskStore upon creation, allowing you to implement something different than what comes with the TaskStore. A basic append-only logging journaler is included.

#The TaskStore Library

The TaskStore library can be used to open a persistent store of tasks and then manipulate them safely *in a single process*. The basic design is single-threaded to keep it very simple and easy to reason about and work with. There is only ever one reader/writer, with one exception that will be discussed in the section on journaling.

The basic idea of the TaskStore is that you can have multiple task "groups". These groups are simply names, and can be used in any way you choose. You might choose to use a group as a way of specifying task "type", or as a way of specifying a particular part of a pipeline. It's up to you. An example of how to use the TaskStore to make a MapReduce is given in taskstore_test.go (and can be viewed in godoc as an "Example").

Within these groups, tasks can be manipulated in a way that has some guarantees:

  * No currently-owned task can be updated or deleted by any other process.
  * No operation succeeds if any of the depended-upon tasks are missing.

The way these characteristics are enforced is through immutability and atomicity. Every Task is immutable. An update to an existing task atomically deletes the old Task (and thus its globally-unique ID) and creates a new one with the new specifications (new data, new ownership, or new "available time" when ownership expires). Thus the existence of a task ID is a guarantee that it is exactly the task that you think it is, and a missing task ID means that another process was responsible for doing something with it. A task is either there or it is not. It is never in an unexpected state.

Because a change in "available time" actually creates a new task, this means that "Claim"ing a task creates a new task ID with an available time in the future and deletes the old ID. Thus the old task is gone and therefore cannot be claimed by any other process, and the new task has a future available time and an assigned owner, also rendering it unclaimable by another process.

Some failure scenarios can help demonstrate the utility of this simple approach.

##Task Abandoned

Frequently in parallel processing environments, a process busy doing a task will die. This can happen for various reasons, including eviction, data corruption, or process bugs. When it happens to a process that owns a task, that task is now orphaned for a period of time. Eventually, however, the ownership will expire on that task, and another process will be able to claim it.

Because claiming a task actually creates a new task with a new availability time, the old process will not be able to claim the same task.

##Task Delayed

If a process is working, but ends up being delayed or is simply very slow, the task might expire while it is holding it. If this occurs, that task is now available to be picked up by another process, which may decide to start working on it. This means that two processes are working on the same task, but *only the second will be able to commit the work* because the task held by the first is no longer in the store (it disappeared when the second process claimed it). Thus, the first process, when attempting to delete the task or otherwise modify it to indicate that it is finished with it, will get an error that the task is no longer available and can abandon the work that it has completed.

##Task Store Restarted

There are two approaches to running a task store: opportunistic journaling and strict journaling.

In the opportunistic scenario, requests involving tasks are fulfilled immediately from RAM, and the corresponding update is flushed to disk as soon as possible afterward. If a task store dies between sending out the task and flushing the update to disk, a worker will have confirmation that it, e.g., owns a task, but the task store will have no record of that task when it starts up again. If the work done for a task is idempotent (there is no harm in repeating work), then this is safe: the client can finish what it is doing and fail when it attempts to update or delete the corresponding task: it won't be there (or if a task with a similar ID *is* there because another process created one, it won't be owned by this worker).

With strict journaling things are even more straightforward: no task requests are fulfilled until *after* the task update is flushed to disk, so no work will be done on a task that the task store does not durably remember. If the task store crashes between flushing to disk and fulfilling the request, that simply means that a task exists that nobody will be able to claim until its expiry: an annoying but safe situation.

##More on Journaling

When the TaskStore is opened, it is given a journal to which it can send transactions and through which it can initiate a snapshot. Commonly a journal just accesses a filesystem, but other approaches are also allowed, e.g., a database.

As mentioned above, the TaskStore has two journaling modes: strict and opportunistic. The opportunistic variant works by spawning a concurrent routine that listens for transactions on a channel and sends them to the journal when they arrive. The strict method simply writes transactions in the main thread before returning a result to the caller.

Even in the case of opportunistic journaling, all access to the in-memory store is serialized, with the exception of producing periodic snapshots.

A snapshot is produced when the number of recent transactions reaches some threshold, enabling start-up times to be reduced in the event of a failure. During a snapshot, the operation of the task store can continue normally, as careful attention has been given to allowing this exception to serialized access.

#Anatomy of a Task

What is a Task? A Task is a piece of data with

  * A unique identifier,
  * A group name,
  * An available time,
  * An owner, and
  * User-specified data.

##Task ID

The unique ID is always globally unique and new tasks always have a larger ID number than old tasks. There are never any other tasks in the store with the same ID, and once used, it will never be used again.

##Group Name

Tasks are organized into "groups" with free-form names. Groups serve to partition tasks in useful ways. For instance, one can claim a new, random task from a particular group knowing only the name of that group. One can also list the tasks, owned or not, within a group. This can serve as a way of determining whether there is any of a particular kind of work left to be done. For example, in a mapreduce scenario, a "map" group might contain all of the map tasks that are not yet completed, and a "reduce" group might contain all of the unfinished reduce tasks.

Groups spring into existence when tasks are created within them, and they disappear when they are empty.

##Available Time

The "available time" (AT) of a task is the time in milliseconds since the Epoch when a task can be claimed or owned by anyone. If the time is in the future, then the task is not available: some process owns it and is ostensbily busy working on it if the time is in the future. In fact, "claim"ing a task involves setting its owner ID and advancing its AT to a future time (and, of course, creating a new ID for it, as these operations create a new task).

##Owner ID

Each client of the task store is required to provide a unique ID that represents it. This is often achieved by obtaining a random number. The client library, for example, works in precisely this way: when the library loads, it creates a unique ID, and any uses of that library will share it. This gives each process its own (probably) unique identifier. The task store does not assign identifiers to clients.

##Data

Each task can optionally contain a slice of bytes, the data that describes the task. Sometimes a task requires no explanation; it's presence is sufficient. Most of the time, tasks will require some data. A map task, for example, will probably want to descirbe the file from which it is to read the data, and perhaps the offsets within that file. Because it is only a byte slice, the data is not interpreted in any way by the task store. It is simply passed around as an opaque entity.

#Task Service

Users of the task store are of course welcome to create their own service using the taskstore library as the underlying mechanism for storage and maintenance of tasks. Even so, a taskstore service is included with this distribution that exposes the operations of the taskstore over HTTP using a simple RESTful API. Among other things, this approach assumes that all task data is a *string*, not a slice of bytes.

All communication with the HTTP service is done via JSON.

The retrieval entry points of the API are described here:

  * `/groups`: GET a list of non-empty groups
  * `/task/<ID>`: GET the given task, specified by numeric ID
  * `/tasks/<ID>[,<ID>,...]`: GET a list of tasks, by comma-separated ID list
  * `/group/<Group>?limit=1&owned=true`: GET the tasks for the specified group, optionally limiting the number returned and allowing owned tasks to be included with unowned tasks. Default is all unowned tasks.

In addition to retrieving, the store can be updated (including task claiming) by POSTing to

  * `/update`: add, update, or delete tasks
  * `/claim`: claim a task from a group

All of the above are described in their own section below.

##`/groups`

Issue a GET request to `/groups` to return a JSON list of all non-empty group names in the task store. It may not be perfectly current.

##`/task/<ID>`

Issue a GET request to `/task/<ID>` to obtain information about the task of the given ID. The ID is numeric (only digits), e.g., `/task/523623`. The returned JSON has the following format:

```JSON
  {"id": 523623,
   "group": "map",
   "data": "/path/to/file:1024:2048",
   "timespec": 1388552400000,
   "ownerid": 2624}
```

The time is in milliseconds since the Epoch. The data is free-form, whatever was put into the data field when the task was last updated or created.

##`/tasks/<ID>[,<ID>,...]`

This is like `/task/<ID>` except that you can specify a comma-separated list of IDs and a list of task JSON structures will be returned, e.g., issuing a GET to `/tasks/523623,523624` would produce the following (provided that the tasks exist, otherwise nonexistent tasks return `null`):

```JSON
  [{"id": 523623,
    "group": "map",
    "data": "/path/to/file:1024:2048",
    "timespec": 1388552400000,
    "ownerid": 2354},
   {"id": 523624,
    "group": "map",
    "data": "/path/to/file:2048:3096",
    "timespec": 1388552401000,
    "ownerid": 2354}]
```

##`/group/<Group>`

To obtain a list of tasks from a specified group, issue a GET request to `/group/<Group>` where `<Group>` is the name of the group of interest.

Additionally the parameters `limit` and `owned` may be specified:

  * `limit`: numeric, return no more than the amount specified.
  * `owned`: boolean (any of 0, 1, "yes", "no", "true", "false), determines whether to allow owned tasks to be returned.

The default is no limit and only unowned tasks.

The result is the same as for `/tasks/...`: a list of tasks from the task store.

##`/update`

To update tasks (including add, modify, or delete), issue a POST request to `/update` with the request information in a JSON object, the structure of which is exemplified here:

```JSON
  {"clientid": 1643,
   "adds": [{"group": "map",
             "data": "/path/to/file:1024:2048"},
            {"group": "map",
             "data": "/path/to/file:2048:3096"}],
   "updates": [{"id": 14,
                "data": "some data for this task",
                "timespec": -60000}],
   "deletes": [16, 18, 20],
   "depends": [12, 13]}
```

The `clientid` should be a unique numeric ID for this particular requesting process. The included client library that uses this service assigns the ID randomly using the `cypto/rand` library. This is the suggested approach. Alternatively, a service could be designed to assign client IDs, but cryptographically strong random number generators are decentralized and convenient.

The `adds` list contains partial task information (the ID is omitted, for example, because it will be assigned by the task store, and is ignored if included). The group is the only thing that must be specified. All other fields have sensible defaults:

  * `data` defaults to the empty string
  * `timespec` defaults to "right now" in milliseconds since the Epoch (making the task unowned, since its time is already in the past).

The `updates` list contains the ID of the task and need not contain any other fields. If it does not, then the task will be changed (deleted and a new one created) with no data and the timespec set to the present time. The group is never changed. If you want to move a task from one group to another, you must delete the old one and add a new one with the same data.

You can renew ownership of a task by specifying a future timespec. Alternatively, if you specify a negative timespec, the new timespec will be the present time plus the absolute value of what you specify.(e.g., -60000 means "set to one minute from right now", where 60000 means "set to 60 seconds after the Epoch", which was a long time ago). This also works for adds.

The `deletes` list is nothing more than a list of integer task IDs to delete.

The `depends` list allows you to specify that the transaction cannot complete unless the given task IDs are *present*. None of those task IDs will be changed in any way, but the other tasks cannot be changed, added, or deleted unless the `depends` list is all currently in the task store.

This does not only mean that the conceptual tasks *exist*, but also that whatever tasks you specify are *unmodified*, since a task ID changes when the task is updated by any process.

An update either completely succeeds or completely fails. If any of the tasks specified by ID are not present (`udpates`, `deletes`, or `depends`), then the transaction will return an error indicating what happened.

If it succeeds, then it returns all of the new tasks in a single JSON list in the order specified: adds first, followed by updates. This way you can track the way that the ID changes across updates, and can do things like renew your claim on a changed task later on.

The JSON returned is an object with two fields: `tasks` and `errors`. They will never be simultaneously present. The `tasks` member is a list of tasks just like that returned by a GET request on `/tasks`, and the `error` member is a list of error strings indicating all of the reasons that a transaction may have failed.

##`/claim`

To claim a task, POST a JSON object to `/claim` with the following layout:

```JSON
  {"clientid": 1532,
   "group": "map",
   "duration": 60000,
   "depends": [14, 10]}
```

Like an update, a claim cannot succeed unless the dependent task IDs are present in the task store. When a claim does succeed, you will be given a random task from the specified group name, and will have ownership that extends `duration` milliseconds into the future.

The result is the same as with `/update`: either a list containing the claimed task, or a list of errors indicating why it failed.

It is good practice to claim a task for a relatively short time and refresh the claim periodically if work is progressing properly. This is done by issuing an update to the task with a future (or negative) timespec as described in the section for `/update`.
