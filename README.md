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

Use the demo credentials and certificate settings for local Compose runs. For a shared deployment, give the submitter permission to execute batch.Submit and batch.Resume, the front end read access to status views or queries, and the worker permission to execute the dispatch procedures and read the queued submissions. Use an account with DDL permissions for init-db and a TLS-verified connection string for the workers.

## Worker service and recovery

The `worker` command runs a .NET 10 hosted background service. An unexpected exception in the scheduling loop starts another worker loop after five seconds. The host creates a fresh business runner when a child exits. Configure a service manager on each machine so a crash of the host executable also restarts it. Docker Compose sets `restart: unless-stopped` on the worker service.

**Linux with systemd.** Publish the worker to the target machine's `/opt/batch-engine` directory. Put `BATCH_SQL_CONNECTION` and any plugin settings in `/etc/batch-engine.env`. Install the supplied unit under `/etc/systemd/system/` and enable it. [Systemd unit](deploy/batch-engine.service)

~~~sh
sudo systemctl daemon-reload
sudo systemctl enable --now batch-engine
~~~

**Windows Service.** Publish for the machine's architecture and set the service account's connection string and plugin paths in its service environment. For an installation under `C:\BatchEngine`, run these commands from an elevated command prompt.

~~~bat
sc.exe create BatchEngine binPath= "C:\BatchEngine\BatchEngine.exe worker" start= auto
sc.exe failure BatchEngine reset= 86400 actions= restart/5000/restart/5000/restart/5000
sc.exe failureflag BatchEngine 1
sc.exe start BatchEngine
~~~

On a service stop the worker stops claiming jobs and signals cancellation to active business handlers. They have `BATCH_GRACE_SECONDS` (default 15) to return or unwind. The worker saves completed results, terminates any remaining runners after the grace period and returns interrupted jobs to SQL. If the machine or database is unavailable, another worker recovers outstanding leases after expiry. An intentional service stop leaves work eligible on other nodes. Set the service manager's shutdown allowance above the configured grace period to allow time for SQL settlement.

## Resume a stopped group

A permanent exception in a process handler, exhausted transient retries, or a task or process timeout sets the submitted run and its active nodes to `Stopped`. The stop transaction fences active job leases across the group and preserves waiting descendants and sibling branches. Other submitted groups remain eligible under their configured concurrency policies. The status endpoint shows the cause in `Error`, the time in `StoppedAt` and the execution phase on each node.

Re-execute the **original run ID** after addressing the cause. The API, CLI and SQL procedure all use the same resume operation.

~~~text
POST /api/runs/{runId}/resume?expectedResumeCount=0
dotnet BatchEngine.dll resume <run-id>
EXEC batch.Resume @RunId = @RunId, @ExpectedResumeCount = 0;
~~~

The optional count comes from the status response. Including it protects against a delayed or repeated request resuming a later stop and returns HTTP 409 if the count changed. A fresh submission key creates a separate run. Resuming preserves the original configuration, selection, tree, shared data and job IDs.

| Stage at stop | Work performed on resume |
| --- | --- |
| Start or finish hook | Retry the unfinished hook. Previously completed stages stay complete. |
| Fetch in progress | Continue from the last committed cursor. The unique process and volume key prevents duplicate engine task rows across overlapping pages. |
| Fetch complete | Reuse the staged volume. Retry failed tasks with a fresh attempt allowance and dispatch pending tasks. Success and business Invalid results are retained. |
| Process deadline expired | Re-enter normal cross-run admission with a fresh process time budget. The hierarchy continues after the process completes. |

Task attempts keep their identity across a resume. Business tables and external services can still see a repeated write if the handler committed before its lease was lost. Use the task's `IdempotencyKey` or a unique business key for those writes.

For an upgrade, stop the old workers, run `init-db` once, then deploy the new workers and API. Existing terminal runs retain their recorded outcomes. Grant the caller of the resume API or CLI execute permission on `batch.Resume`.

## Submit and inspect workflows

A workflow is a JSON tree with a group at its root. Groups can contain groups and processes. Processes can have child processes. Give each node a unique key within the submission. The group or process that owns the children sets their execution order with children_mode.

| Field | Behavior |
| --- | --- |
| children_mode | serial runs each child subtree to completion in array order. parallel makes all child subtrees eligible together. The default is serial. |
| configuration | Key of a saved SQL Server process configuration. A referenced configuration supplies the settings for that process. |
| handler | Registered, versioned business handler name on a process with inline settings. |
| max_parallel_tasks | Simultaneous task leases for this process instance across all workers. The default is 1. |
| max_attempts | Maximum attempts per task, lifecycle stage or fetch page within a retry window. The default is 3. |
| timeout_seconds | Deadline for each attempt, including hooks and shared-data calls. The default is 60. |
| options | JSON object passed to the process and its tasks. |

A process finishes its own work and its after-process hook before releasing children. Invalid advances the tree and rolls up to the ancestors. An unrecoverable process error pauses the run at its current hierarchy position. Resuming the run releases later children after their prerequisites complete. Node OwnStatus shows the process's own result. Node Status includes its descendants.

Submit an immutable snapshot using the CLI, a stored procedure, or an insert. A repeated non-null submission key returns the original run ID when you call batch.Submit.

~~~sql
EXEC batch.Submit
    @DefinitionJson = N'{"type":"group","key":"root","children":[{"type":"process","key":"configure","handler":"demo.configure.v1"}]}',
    @InputJson = N'{"limit":100}',
    @SubmissionKey = N'daily-audit-001';

-- To enqueue directly, supply DefinitionJson and optionally InputJson and SubmissionKey.
-- INSERT batch.Submission(DefinitionJson, InputJson) OUTPUT inserted.Id VALUES (@Definition, @Input);
~~~

## Save process settings in SQL Server

Run init-db with the updated executable to install batch.ProcessConfiguration and batch.SaveProcessConfiguration. A frontend backend can read the configuration table and call the procedure to save a whole configuration. The procedure checks the revision to detect concurrent edits.

~~~sql
EXEC batch.SaveProcessConfiguration
    @ConfigurationKey = N'orders-audit',
    @Handler = N'orders.audit.v1',
    @ExpectedRevision = 0,
    @MaxParallelTasks = 4,
    @MaxAttempts = 3,
    @TimeoutSeconds = 60,
    @ChildrenMode = 'serial',
    @OptionsJson = N'{"rowsPerVolume":100}',
    @TaskExecutionMode = 'parallel',
    @TaskTimeoutEnabled = 1,
    @TaskTimeoutSeconds = 30,
    @ProcessTimeoutEnabled = 1,
    @ProcessTimeoutSeconds = 900,
    @AllowSameProcessAcrossRuns = 0,
    @AllowOtherProcessesAcrossRuns = 1;

SELECT * FROM batch.ProcessConfiguration WHERE ConfigurationKey = N'orders-audit';
~~~

Pass revision zero on creation. For an update, pass the revision from the row you read and use the new revision returned by the procedure. The backend identity can read the table and execute the save procedure.

Reference the configuration in a workflow after deploying the named handler to all workers.

~~~json
{
  "key": "audit",
  "type": "group",
  "children": [
    {"key": "orders", "type": "process", "configuration": "orders-audit"}
  ]
}
~~~

Use key, type, configuration and optional children on a configured process node. The worker reads the referenced settings when it accepts a queued workflow and persists them in its execution plan. Edits apply to workflows accepted after the save. A parallel-task limit of one runs tasks serially, and a larger limit allows that many concurrent task leases across the worker cluster for the process instance.

| Configuration setting | Execution behavior |
| --- | --- |
| MaxParallelTasks | Cluster-wide task limit for one process instance in parallel mode. |
| TaskExecutionMode | parallel uses the task limit. serial runs volumes in fetch order, completing a retry before starting the next volume. |
| TaskTimeoutEnabled and TaskTimeoutSeconds | Per-attempt deadline for a task. The duration inherits TimeoutSeconds until set explicitly. |
| ProcessTimeoutEnabled and ProcessTimeoutSeconds | Total deadline for start, fetch, tasks and their retries, measured from the first admitted start. TimeoutSeconds still controls individual lifecycle hook attempts, including after-process cleanup. |
| AllowSameProcessAcrossRuns | Set to zero to serialize instances of this configuration across separate submissions. |
| AllowOtherProcessesAcrossRuns | Set to zero for exclusive process execution across runs. The process waits for existing work in other runs to finish. New processes in those runs wait until it completes its own work and finish hook. |

Existing configurations default to parallel task mode, an enabled task timeout inherited from TimeoutSeconds, a disabled process deadline and both overlap policies enabled. A process deadline stops the run. Interrupted running tasks are recorded as failed and unstarted tasks remain pending.

## Group management API

The ASP.NET Core API gives a frontend backend access to saved groups and run progress. Start it separately from the workers after running init-db with the current build.

~~~sh
export BATCH_SQL_CONNECTION='<connection-string>'
export BATCH_API_KEY='<service-secret>'
dotnet run --project src/BatchEngine.Api -- --urls http://127.0.0.1:5080
~~~

Send Authorization with the Bearer scheme and the service key from your frontend backend. The API exposes these routes.

| Method and route | Result |
| --- | --- |
| GET /api/groups | List saved group keys, display names and revisions. |
| GET /api/groups/{key} | Read the complete ordered group hierarchy. |
| GET /api/process-configurations | List saved configuration keys for the process picker. |
| PUT /api/groups/{key} | Save a group and its full hierarchy. |
| POST /api/groups/{key}/runs | Queue the selected saved group revision. Returns HTTP 202, a run ID and a status URL. |
| POST /api/runs/{id}/resume | Resume the saved run. Optionally supply expectedResumeCount from its status response. |
| GET /api/runs/{id} | Read run status, process states and progress. Set taskLimit from 0 to 100 to include individual tasks. |

The two lists accept limit from 1 to 100 and afterKey for paging by key. A group save takes displayName, expectedRevision and children. Use revision 0 to create a group. To edit or reorder a hierarchy, send the new children array with the currently saved revision. A child group can contain groups and processes. A process references a saved configuration and may have process children.

~~~json
{
  "displayName": "Daily order audit",
  "expectedRevision": 0,
  "children": [
    {
      "key": "validation",
      "type": "group",
      "children": [
        {"key": "orders", "type": "process", "configuration": "orders-audit"}
      ]
    }
  ]
}
~~~

Submit the group by key with its revision, a submissionKey for safe retries, and optional input. For example, send the following to POST /api/groups/daily-order-audit/runs after saving that group at revision 1.

~~~json
{"revision":1,"submissionKey":"daily-order-audit-request-1","processKeys":["orders"],"input":{"region":"north"}}
~~~

Use processKeys to submit a nonempty selection of process node keys. Omitting it submits the full group. The saved tree determines execution order. When a process is omitted, selected descendants are promoted to the nearest retained parent. Groups with selected descendants keep their lifecycle hooks.

A stale group revision returns HTTP 409. Repeating a submission with the same key, group revision, process selection and input returns the original run ID. SQL Server stores the group definition snapshot on submission. Workers accept that snapshot from the queue and execute its processes one at a time in tree order. Each process still uses its own parallel task limit across workers. The process overlap settings control whether processes in separate runs can execute concurrently.

## Group lifecycle hooks

Implement IBatchGroup in a business plugin and give the class a GroupHandler attribute with its registered name. Deploy the plugin to all workers. Add lifecycle to a group node in a workflow or to the root group body in PUT /api/groups/{key}. Nested groups can each have their own lifecycle settings.

~~~json
{
  "handler": "orders.batch-group.v1",
  "max_attempts": 3,
  "timeout_seconds": 60,
  "options": {"notify": true}
}
~~~

BeforeGroupAsync runs before any child starts. A paused group defers its finish hook until the resumed children finish. AfterGroupAsync runs after the group’s children finish and receives their combined Success, Invalid, or Failed outcome. When the start hook fails permanently or exhausts retries, the engine skips the children and still calls the finish hook with Failed. A finish hook that exhausts its retries marks the group Failed.

Each hook is a leased job. Its attempt limit and deadline apply independently, and another node can recover an expired lease. Use GroupContext.IdempotencyKey for external effects that need deduplication across retries. Run init-db and deploy the updated workers before enabling group hooks.

## Monitor a running workflow

Run the updated executable with init-db on an existing batch database to install the progress views. The views also cover earlier submissions. While a workflow runs, use status with a task limit of zero to see its run totals and each process's counts.

~~~sh
dotnet src/BatchEngine/bin/Debug/net10.0/BatchEngine.dll status <run-id> 0
~~~

The counts come from SQL Server and reflect committed worker batches. Task counts measure task volumes. The matching record counts, such as ProcessedRecords, PendingRecords and FailedRecords, measure source rows. An ordinary task contributes one record. Every row in a result-set volume receives that task's outcome.

| Count | Includes |
| --- | --- |
| TotalTasks | Tasks discovered so far. More tasks can appear as processes fetch pages. |
| ProcessedTasks | Tasks with a Success or business Invalid outcome. |
| PendingTasks | Tasks waiting to run, including staged volumes and tasks waiting to retry. |
| RunningTasks | Tasks reserved by a worker. Expired leases count here until recovery requeues or fails them. |
| FailedTasks | Tasks with a permanent error or exhausted retries. |
| SkippedTasks | Discovered tasks left undispatched because preparation ended early. |

SucceededTasks and InvalidTasks break down ProcessedTasks. A process can also fail in a lifecycle hook or during fetching. Its Status and Error show that failure separately from FailedTasks.

A front end can query the views by run ID. Progress queries read per-process counters. For error details, page through the task status view.

~~~sql
SELECT * FROM batch.RunProgress WHERE RunId = @RunId;
SELECT * FROM batch.ProcessProgress WHERE RunId = @RunId ORDER BY ProcessKey;
SELECT TOP (100) Id, ProcessKey, RecordKey, [RowCount], Status, Attempts, Error
FROM batch.TaskStatus
WHERE RunId = @RunId AND Status = 'Failed' AND Id > @AfterId
ORDER BY Id;
~~~

## Add business logic

Reference src/BatchEngine.Contracts from a .NET 10 class library. Create a public IBatchProcess implementation with a public parameterless constructor and a ProcessHandler attribute such as ProcessHandler("orders.validate.v1"). For plugins with NuGet dependencies, set EnableDynamicLoading to true in the plugin project and deploy its publish output, including the dependency manifest and runtime assets. Deploy the library to each worker and include its DLL in BATCH_PLUGIN_PATHS. Separate multiple paths with the platform path separator. Register a new handler name when changing logic for new submissions while older runs still need the previous implementation.

| Method | When it runs |
| --- | --- |
| BeforeProcessAsync | Before the first fetch page. |
| FetchRecordsAsync | Returns RecordPage values until NextCursor is null. The worker commits each page and its checkpoint together. Tasks are released after the final page. |
| BeforeTaskAsync and RunTaskAsync | Run per task volume on any available worker. Return TaskResult.Success or TaskResult.Invalid with a reason. |
| AfterTaskAsync | Receives the outcome of each attempt that returns or throws in the runner. An attempt killed by a crash or a hard timeout cannot call this hook. |
| AfterProcessAsync | Receives the final outcome and counts for the process's own work, once the process reaches its finish stage. A stopped process defers this hook until execution resumes. A failure in the hook resumes at that same hook. |

### Query business tables and create task volumes

Use FetchRecordsAsync to query business tables in bounded pages and project each row into a serializable model. Group rows into task volumes using stable keys. Return Record.FromResultSet for each volume in the RecordPage. The limit parameter caps the number of volumes in the page. A cursor should identify where the next business query resumes.

~~~csharp
var volumes = rows.Chunk(rowsPerVolume)
    .Select(chunk => Record.FromResultSet(chunk[0].Id.ToString(), chunk))
    .ToArray();
return new RecordPage(volumes, nextCursor);
~~~

SQL Server saves each volume with its result rows. The worker that leases the task passes those rows as a JSON array to BeforeTaskAsync, RunTaskAsync and AfterTaskAsync. Deserialize the array with resultSet.Deserialize<OrderRow[]>() or iterate it with EnumerateArray(). A retry receives the same saved result set. The integration fixture DatabaseVolumes.cs contains a working SQL query, cursor, and RunTaskAsync implementation.

SQL Server commits the page’s task rows, progress counters, and next cursor together. If the save rolls back, the previous cursor stays in place. An expired fetch lease lets a worker fetch the same page again. Tasks are released after the last page commits. Use stable source pagination and unique volume keys. The engine deduplicates reused keys and can only persist rows the fetch method returned.

Size each volume within the 256 KiB payload limit and each fetch page within 4 MiB. One task outcome applies to its entire volume. When rows need independent retry or outcome tracking, return a separate task per row.

Use stable record keys and stable keyset pagination over the source records. The engine keeps one task per process and record key, preserving the first accepted payload. A repeated cursor fails the fetch. A business rejection can also be signaled by BusinessInvalidException in start, fetch, or task work. Transient exceptions retry with exponential backoff up to the attempt limit. An after-task hook failure retries the task attempt. PermanentProcessException and timeouts stop the process immediately.

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

The integration executable starts two independent workers, loads a separate business plugin assembly, and submits work both directly to SQL Server and through the group API. It checks selected-process hierarchy, cross-run scheduling and timeouts, hierarchy editing and snapshots, group hooks, fetch rollback and recovery, editable process configurations, parallel result-set volumes, live task and record counts, tree order, cross-worker task execution, staged fetches, both business outcomes, retries, task hooks, permanent failures, hard timeouts and recovery after a worker crash.

~~~sh
export BATCH_TEST_SQL_CONNECTION='<disposable-sql-server-database-connection-string>'
dotnet build BatchEngine.slnx
dotnet tests/Integration/bin/Debug/net10.0/Integration.dll
~~~
