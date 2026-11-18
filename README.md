# BatchEngine

BatchEngine runs database-submitted workflows across .NET 10 worker processes. SQL Server 2022 or later holds the submission queue, the execution tree, task leases and outcomes, and run-scoped shared data. Workers can run on separate machines that can reach the same database.

## Run two workers

The demo uses Docker Compose and SQL Server Developer Edition on x86-64. Allocate at least 4 GB of memory to SQL Server.

~~~sh
docker compose up -d --build --scale worker=2
docker compose run --rm --no-deps worker submit examples/workflow.json examples/input.json demo-1
docker compose run --rm --no-deps worker status <run-id>
~~~

The submission command returns the run ID. The example ends as Invalid because two records exceed the shared limit. Its serial and parallel branches still complete. Change the submission key to run the same example again.

To use an existing SQL Server, provision a database at compatibility level 160 or higher and run the following from the repository. Deploy the same build and business plugin versions on each worker and set its connection string to the same database.

~~~sh
export BATCH_SQL_CONNECTION='<connection-string>'
dotnet build BatchEngine.slnx
dotnet src/BatchEngine/bin/Debug/net10.0/BatchEngine.dll init-db
dotnet src/BatchEngine/bin/Debug/net10.0/BatchEngine.dll worker
~~~

Use the demo credentials and certificate settings for local Compose runs. For a shared deployment, give the submitter permission to execute batch.Submit, the front end read access to status views or queries, and the worker permission to execute the dispatch procedures and read the queued submissions. Use an account with DDL permissions for init-db and a TLS-verified connection string for the workers.

## Submit and inspect workflows

A workflow is a JSON tree with a group at its root. Groups can contain groups and processes. Processes can have child processes. Give each node a unique key within the submission. The group or process that owns the children sets their execution order with children_mode.

| Field | Behavior |
| --- | --- |
| children_mode | serial runs each child subtree to completion in array order. parallel makes all child subtrees eligible together. The default is serial. |
| handler | Registered, versioned business handler name on a process. |
| max_parallel_tasks | Simultaneous task leases for this process instance across all workers. The default is 1. |
| max_attempts | Maximum attempts per task, lifecycle stage or fetch page. The default is 3. |
| timeout_seconds | Deadline for each attempt, including hooks and shared-data calls. The default is 60. |
| options | JSON object passed to the process and its tasks. |

A process finishes its own work and its after-process hook before releasing children. Invalid advances the tree and rolls up to the ancestors. An exhausted technical failure skips that process's descendants and any later children on the same serial branch. Eligible parallel branches continue. Failed takes precedence over Invalid in the final subtree status. Node OwnStatus shows the process's own result. Node Status includes its descendants.

Submit an immutable snapshot using the CLI, a stored procedure, or an insert. A repeated non-null submission key returns the original run ID when you call batch.Submit.

~~~sql
EXEC batch.Submit
    @DefinitionJson = N'{"type":"group","key":"root","children":[{"type":"process","key":"configure","handler":"demo.configure.v1"}]}',
    @InputJson = N'{"limit":100}',
    @SubmissionKey = N'daily-audit-001';

-- To enqueue directly, supply DefinitionJson and optionally InputJson and SubmissionKey.
-- INSERT batch.Submission(DefinitionJson, InputJson) OUTPUT inserted.Id VALUES (@Definition, @Input);

SELECT Id, Status, Error, StartedAt, FinishedAt FROM batch.Submission WHERE Id = @RunId;
SELECT NodeKey, Status, OwnStatus, TotalTasks, RunningTasks, SucceededTasks, InvalidTasks, FailedTasks
FROM batch.Node WHERE RunId = @RunId;
SELECT TOP (100) Id, ProcessKey, RecordKey, Status, Attempts, WorkerId, Error
FROM batch.TaskStatus WHERE RunId = @RunId AND Id > @AfterId ORDER BY Id;
~~~

## Add business logic

Reference src/BatchEngine.Contracts from a .NET 10 class library. Create a public IBatchProcess implementation with a public parameterless constructor and a ProcessHandler attribute such as ProcessHandler("orders.validate.v1"). Deploy the library and its dependencies to each worker, and include the DLL in BATCH_PLUGIN_PATHS. Separate multiple paths with the platform path separator. Register a new handler name when changing logic for new submissions while older runs still need the previous implementation.

| Method | When it runs |
| --- | --- |
| BeforeProcessAsync | Before the first fetch page. |
| FetchRecordsAsync | Returns RecordPage values until NextCursor is null. The worker commits each page and its checkpoint together. Tasks are released after the final page. |
| BeforeTaskAsync and RunTaskAsync | Run per record on any available worker. Return TaskResult.Success or TaskResult.Invalid with a reason. |
| AfterTaskAsync | Receives the outcome of each attempt that returns or throws in the runner. An attempt killed by a crash or a hard timeout cannot call this hook. |
| AfterProcessAsync | Receives the final outcome and counts for the process's own work, including a start or fetch failure. The engine can retry this hook independently. |

Use stable record keys and stable keyset pagination over the source records. The engine keeps one task per process and record key, preserving the first accepted payload. A repeated cursor fails the fetch. A business rejection can also be signaled by BusinessInvalidException in start, fetch, or task work. Exceptions retry with exponential backoff up to the attempt limit. An after-task hook failure retries the task attempt. PermanentProcessException fails immediately.

Handlers receive an IdempotencyKey that is stable across retries. Delivery is at least once. Use this key when writing external effects so a crash between the effect and its SQL settlement can be replayed safely. Store inter-stage state in context.Shared. GetManyAsync batches reads, and PutAsync supports version checks for concurrent updates. Writes commit independently, so make them replay-safe when they affect business logic. Business methods can land on different workers or subprocesses.

## Operating limits

| Environment variable | Default | Effect |
| --- | ---: | --- |
| BATCH_SLOTS | 8 | Maximum concurrent attempts on one worker. Compose sets 4. |
| BATCH_POOL_SIZE | 3 | Maximum SqlClient connections per worker for the engine and shared store. |
| BATCH_PAGE_SIZE | 200 | Maximum records requested in one fetch page. |
| BATCH_MIN_POLL_MS / BATCH_MAX_POLL_MS | 200 / 5000 | Adaptive, jittered polling window. |
| BATCH_LEASE_SECONDS | 45 | Lease duration, renewed in one batch at roughly one-third of this value. |
| BATCH_SWEEP_SECONDS | 6 | Interval for checking expired leases. |
| BATCH_GRACE_SECONDS | 15 | Time to finish active work on shutdown before terminating the runners. |

The engine batches claims, renewals, and settlements, uses filtered queue indexes, and holds locks only during metadata transactions. On a dedicated batch database, READ_COMMITTED_SNAPSHOT lets dashboard readers use row versions while workers update status. Keep business fetches paged and budget their own connections separately. Small run-scoped values fit in the shared store. A single record, output, or shared value is limited to 256 KiB, with a 4 MiB fetch page limit. Store references to larger payloads.

## Verification

The integration executable starts two independent workers, loads a separate business plugin assembly, and inserts submissions directly into SQL Server. It checks tree order, cross-worker task execution, staged fetches, both business outcomes, retries, task hooks, permanent failures, hard timeouts and recovery after a worker crash.

~~~sh
export BATCH_TEST_SQL_CONNECTION='<disposable-sql-server-database-connection-string>'
dotnet build BatchEngine.slnx
dotnet tests/Integration/bin/Debug/net10.0/Integration.dll
~~~
