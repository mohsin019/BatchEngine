using System.Data;
using System.Diagnostics;
using System.Text.Json;
using FixturePlugin;
using Microsoft.Data.SqlClient;

var dsn = Environment.GetEnvironmentVariable("BATCH_TEST_SQL_CONNECTION")
    ?? throw new InvalidOperationException("Set BATCH_TEST_SQL_CONNECTION to a disposable SQL Server test database.");
var engine = args.Length == 1 ? Path.GetFullPath(args[0]) : Path.GetFullPath(
    Path.Combine(AppContext.BaseDirectory, "../../../../../src/BatchEngine/bin/Debug/net10.0/BatchEngine.dll"));
var dotnet = Environment.GetEnvironmentVariable("BATCH_TEST_DOTNET") ?? "dotnet";

await RunCliAsync("init-db");
var workers = new List<Process>();
try
{
    workers.Add(Worker("integration-a"));
    workers.Add(Worker("integration-b"));
    await ConfiguredVolumesAsync();
    await HierarchyAsync();
    await FailuresAsync();

    await StopWorkersAsync();
    await CrashRecoveryAsync();
    Console.WriteLine("SQL Server integration checks passed.");
}
finally
{
    await StopWorkersAsync();
}

async Task HierarchyAsync()
{
    var tree = Group("root", "serial",
        Step("parent", count: 8, maxParallel: 2, mode: "mixed", childrenMode: "parallel",
            children: [Step("left", 2, requires: ["parent"]), Step("right", 2, requires: ["parent"])]),
        Group("nested", "serial", Step("tail", 1, requires: ["left", "right"])));

    var run = await SubmitAsync(tree);
    await ExpectStatusAsync(run, "Invalid");
    var progress = await ReadStatusAsync(run);
    Equal(13L, progress.GetProperty("Progress").GetProperty("ProcessedTasks").GetInt64(), "processed includes invalid results once");
    Equal(1L, progress.GetProperty("Progress").GetProperty("InvalidTasks").GetInt64(), "invalid count is separate from errors");
    Equal(0L, progress.GetProperty("Progress").GetProperty("FailedTasks").GetInt64(), "a successful retry is not a failed record");
    Equal(2, await QueryAsync<int>("SELECT COUNT(DISTINCT WorkerId) FROM batch.TaskStatus WHERE RunId=@Run AND ProcessKey='parent'", run),
        "both worker processes executed the same process");
    Equal(1, await QueryAsync<int>("SELECT Attempts FROM batch.TaskStatus WHERE RunId=@Run AND ProcessKey='parent' AND RecordKey='0' AND Status='Invalid'", run),
        "business rejection completed in one attempt");
    Equal(2, await QueryAsync<int>("SELECT Attempts FROM batch.TaskStatus WHERE RunId=@Run AND ProcessKey='parent' AND RecordKey='1' AND Status='Success'", run),
        "transient failure retried");
    Equal(9, await QueryAsync<int>("SELECT COUNT(*) FROM batch.SharedData WHERE RunId=@Run AND [Key] LIKE 'task-end.%'", run),
        "after-task hooks observed every non-crashed attempt");
    Equal(0, await QueryAsync<int>("""
        SELECT COUNT(*) FROM batch.Job task JOIN batch.Job fetchStage ON task.NodeId=fetchStage.NodeId
        JOIN batch.Node n ON n.Id=task.NodeId
        WHERE n.RunId=@Run AND task.Kind='Task' AND fetchStage.Kind='Fetch' AND task.StartedAt < fetchStage.FinishedAt
        """, run), "dispatch waited for all fetch pages");
    Equal(0, await QueryAsync<int>("""
        SELECT COUNT(*) FROM batch.TaskStatus a JOIN batch.TaskStatus b
            ON a.RunId=b.RunId AND a.ProcessKey=b.ProcessKey AND a.Id<b.Id
        WHERE a.RunId=@Run AND a.ProcessKey IN ('left', 'right')
            AND a.StartedAt < b.FinishedAt AND b.StartedAt < a.FinishedAt
        """, run), "single-task limits hold across the two nodes");
    Console.WriteLine("Passed hierarchy, parallel claims, invalid result, retry, hooks and cross-node state.");
}

async Task FailuresAsync()
{
    var tree = Group("root", "parallel",
        Group("serial", "serial",
            Step("fail", 1, mode: "permanent", children: [Step("child-skipped", 1)]),
            Step("sibling-skipped", 1)),
        Step("healthy", 3, mode: "hold"),
        Step("hard-timeout", 1, mode: "timeout", timeout: 1),
        Step("fetch-error", 5, mode: "cursor-cycle"));

    var run = await SubmitAsync(tree);
    await WaitAsync(async () => await QueryAsync<int>("""
        SELECT COUNT(*) FROM batch.RunProgress
        WHERE RunId=@Run AND Status='Running' AND ProcessedTasks=1 AND FailedTasks=1
            AND SkippedTasks=2 AND RunningTasks=1 AND PendingTasks=2
        """, run) == 1);

    var status = await ReadStatusAsync(run);
    Equal("Running", status.GetProperty("Status").GetString(), "live CLI status");
    var progress = status.GetProperty("Progress");
    Equal(7L, progress.GetProperty("TotalTasks").GetInt64(), "live total includes staged records");
    Equal(1L, progress.GetProperty("ProcessedTasks").GetInt64(), "live processed count");
    Equal(2L, progress.GetProperty("PendingTasks").GetInt64(), "live pending count");
    Equal(1L, progress.GetProperty("RunningTasks").GetInt64(), "live running count");
    Equal(1L, progress.GetProperty("FailedTasks").GetInt64(), "live error count");
    Equal(2L, progress.GetProperty("SkippedTasks").GetInt64(), "live skipped count");
    var active = status.GetProperty("Nodes").EnumerateArray()
        .Single(node => node.GetProperty("NodeKey").GetString() == "healthy");
    Equal(2L, active.GetProperty("PendingTasks").GetInt64(), "live process pending count");
    Equal(0, status.GetProperty("Tasks").GetArrayLength(), "progress can omit individual task rows");

    await QueryAsync<int>("""
        INSERT batch.SharedData (RunId, [Key], ValueJson, Version)
        VALUES (@Run, N'integration.release', N'true', 1);
        SELECT 1;
        """, run);
    await ExpectStatusAsync(run, "Failed");
    Equal(0L, await QueryAsync<long>("SELECT PendingTasks FROM batch.RunProgress WHERE RunId=@Run", run),
        "terminal run has no pending records");
    Equal(2, await QueryAsync<int>("SELECT COUNT(*) FROM batch.Node WHERE RunId=@Run AND Status='Skipped'", run),
        "dependent processes were skipped");
    Equal(2, await QueryAsync<int>("SELECT COUNT(*) FROM batch.TaskStatus WHERE RunId=@Run AND Status='Skipped' AND ProcessKey='fetch-error' AND Attempts=0", run),
        "partially fetched tasks were skipped");
    Equal(1, await QueryAsync<int>("SELECT Attempts FROM batch.TaskStatus WHERE RunId=@Run AND ProcessKey='fail' AND Status='Failed'", run),
        "permanent task failed immediately");
    Equal(2, await QueryAsync<int>("SELECT Attempts FROM batch.TaskStatus WHERE RunId=@Run AND ProcessKey='hard-timeout' AND Status='Success'", run),
        "hard timeout recovered in a fresh runner");
    Equal("Success", await QueryAsync<string>("SELECT Status FROM batch.Node WHERE RunId=@Run AND NodeKey='healthy'", run),
        "independent parallel branch finished");

    var malformed = await SubmitAsync(Group("bad", "serial", new { key = "x", type = "process", handler = "missing" }));
    await ExpectStatusAsync(malformed, "Failed");
    Equal(0, await QueryAsync<int>("SELECT COUNT(*) FROM batch.Node WHERE RunId=@Run", malformed),
        "malformed submission failed before scheduling");
    Console.WriteLine("Passed live progress, permanent failure, branch skipping, cursor-cycle detection, timeout and validation.");
}

async Task ConfiguredVolumesAsync()
{
    var fixtureId = Guid.NewGuid();
    await QueryAsync<int>("""
        IF OBJECT_ID('dbo.BatchEngineIntegrationVolumeRow', 'U') IS NULL
            CREATE TABLE dbo.BatchEngineIntegrationVolumeRow (
                FixtureId uniqueidentifier NOT NULL, Id int NOT NULL, Amount int NOT NULL,
                PRIMARY KEY (FixtureId, Id)
            );
        INSERT dbo.BatchEngineIntegrationVolumeRow VALUES (@Run, 1, 10), (@Run, 2, 20),
            (@Run, 3, -5), (@Run, 4, 40), (@Run, 5, 99), (@Run, 6, 60);
        SELECT 1;
        """, fixtureId);

    var configuration = $"volumes-{fixtureId:N}";
    var settings = JsonSerializer.Serialize(new { fixtureId, rowsPerVolume = 2 });
    Equal(1L, await SaveConfigurationAsync(configuration, 0, 2, settings), "initial process configuration");
    var tree = Group("root", "serial", new { key = "volumes", type = "process", configuration });
    var first = await SubmitAsync(tree);
    await WaitAsync(async () => await QueryAsync<int>("""
        SELECT COUNT(*) FROM batch.RunProgress
        WHERE RunId=@Run AND RunningTasks=2 AND PendingTasks=1 AND RunningRecords=4 AND PendingRecords=2
        """, first) == 1);
    Equal(2, await QueryAsync<int>("""
        SELECT COUNT(DISTINCT WorkerId) FROM batch.TaskStatus WHERE RunId=@Run AND Status='Running'
        """, first), "different workers lease the result-set volumes");

    Equal(2L, await SaveConfigurationAsync(configuration, 1, 1, settings), "edited process configuration");
    try
    {
        await SaveConfigurationAsync(configuration, 1, 2, settings);
        throw new InvalidOperationException("A stale edit overwrote the process configuration.");
    }
    catch (SqlException ex) when (ex.Number == 51013) { }
    Equal(2, await QueryAsync<int>("SELECT MaxParallelTasks FROM batch.Node WHERE RunId=@Run AND NodeKey='volumes'", first),
        "running process retained its accepted concurrency");
    await ReleaseAndCheckAsync(first);

    var next = await SubmitAsync(tree);
    await WaitAsync(async () => await QueryAsync<int>("""
        SELECT COUNT(*) FROM batch.RunProgress
        WHERE RunId=@Run AND RunningTasks=1 AND PendingTasks=2 AND RunningRecords=2 AND PendingRecords=4
        """, next) == 1);
    Equal(1, await QueryAsync<int>("SELECT MaxParallelTasks FROM batch.Node WHERE RunId=@Run AND NodeKey='volumes'", next),
        "new process picked up the revised concurrency");
    await ReleaseAndCheckAsync(next);

    Console.WriteLine("Passed editable process configuration, parallel SQL result sets and row progress.");

    async Task ReleaseAndCheckAsync(Guid run)
    {
        await QueryAsync<int>("""
            INSERT batch.SharedData (RunId, [Key], ValueJson, Version) VALUES (@Run, N'volumes.release', N'true', 1);
            SELECT 1;
            """, run);
        await ExpectStatusAsync(run, "Failed");
        var progress = (await ReadStatusAsync(run)).GetProperty("Progress");
        Equal(3L, progress.GetProperty("TotalTasks").GetInt64(), "one task per result set");
        Equal(6L, progress.GetProperty("TotalRecords").GetInt64(), "all result rows were counted");
        Equal(4L, progress.GetProperty("ProcessedRecords").GetInt64(), "processed rows include invalid rows");
        Equal(2L, progress.GetProperty("InvalidRecords").GetInt64(), "invalid volume carries its row count");
        Equal(2L, progress.GetProperty("FailedRecords").GetInt64(), "failed volume carries its row count");
        Equal(0L, progress.GetProperty("PendingRecords").GetInt64(), "a terminal run has no pending rows");
        await QueryAsync<int>("""
            IF EXISTS (SELECT 1 FROM batch.TaskStatus WHERE RunId=@Run AND RecordKey='1'
                AND Status='Success' AND OutputJson=N'[1,2]' AND [RowCount]=2 AND Attempts=2)
                SELECT 1;
            ELSE THROW 51014, 'Expected the first result set to reach its task intact', 1;
            """, run);
    }
}

async Task<long> SaveConfigurationAsync(string key, long expectedRevision, int parallelTasks, string options)
{
    await using var connection = new SqlConnection(dsn);
    await connection.OpenAsync();
    await using var command = new SqlCommand("batch.SaveProcessConfiguration", connection)
        { CommandType = CommandType.StoredProcedure };
    command.Parameters.Add("@ConfigurationKey", SqlDbType.NVarChar, 64).Value = key;
    command.Parameters.Add("@Handler", SqlDbType.NVarChar, 128).Value = "integration.volumes.v1";
    command.Parameters.Add("@ExpectedRevision", SqlDbType.BigInt).Value = expectedRevision;
    command.Parameters.Add("@MaxParallelTasks", SqlDbType.Int).Value = parallelTasks;
    command.Parameters.Add("@OptionsJson", SqlDbType.NVarChar, -1).Value = options;
    return (long)(await command.ExecuteScalarAsync())!;
}

async Task CrashRecoveryAsync()
{
    var dead = Worker("crashed");
    workers.Add(dead);
    var run = await SubmitAsync(Group("root", "serial", Step("recover", 1, mode: "crash")));
    await WaitAsync(async () => await QueryAsync<int>(
        "SELECT COUNT(*) FROM batch.SharedData WHERE RunId=@Run AND [Key]='crash.entered'", run) == 1);
    var jobId = await QueryAsync<long>("""
        SELECT j.Id FROM batch.Job j JOIN batch.Node n ON n.Id=j.NodeId
        WHERE n.RunId=@Run AND j.Kind='Task'
        """, run);
    var expiredToken = await QueryAsync<Guid>("SELECT LeaseToken FROM batch.Job WHERE Id=@Run", jobId);
    dead.Kill();
    await dead.WaitForExitAsync();

    await WaitAsync(async () => await QueryAsync<int>(
        "SELECT COUNT(*) FROM batch.Job WHERE Id=@Run AND Status='Running' AND LeaseUntil<=SYSUTCDATETIME()", jobId) == 1);
    await QueryAsync<int>("EXEC batch.Settle @Run; SELECT 1;", JsonSerializer.Serialize(
        new[] { new { Id = jobId, Token = expiredToken, Status = "Success", Retryable = false } }));
    Equal("Running", await QueryAsync<string>("SELECT Status FROM batch.Job WHERE Id=@Run", jobId),
        "an expired attempt cannot publish a late result");

    workers.Add(Worker("replacement"));
    await ExpectStatusAsync(run, "Success");
    Equal(2, await QueryAsync<int>("SELECT Attempts FROM batch.TaskStatus WHERE RunId=@Run", run),
        "expired lease was retried");
    var worker = await QueryAsync<string>("SELECT WorkerId FROM batch.TaskStatus WHERE RunId=@Run", run);
    if (!worker.StartsWith("replacement/", StringComparison.Ordinal))
        throw new InvalidOperationException("The replacement node did not finish the recovered task.");
    Console.WriteLine("Passed worker crash and durable lease recovery.");
}

static object Group(string key, string mode, params object[] children) =>
    new { key, type = "group", children_mode = mode, children };

static object Step(string key, int count, int maxParallel = 1, string mode = "normal", string childrenMode = "serial",
    int timeout = 30, string[]? requires = null, object[]? children = null) => new
    {
        key, type = "process", handler = "integration.fixture.v1", children_mode = childrenMode,
        max_parallel_tasks = maxParallel, max_attempts = 2, timeout_seconds = timeout,
        options = new { count, mode, requires = requires ?? [] }, children = children ?? []
    };

async Task<Guid> SubmitAsync(object tree)
{
    await using var connection = new SqlConnection(dsn);
    await connection.OpenAsync();
    // Exercise direct DB ingestion, independently of the submit CLI.
    await using var command = new SqlCommand("""
        INSERT batch.Submission(DefinitionJson) OUTPUT inserted.Id VALUES (@Definition);
        """, connection);
    command.Parameters.Add("@Definition", SqlDbType.NVarChar, -1).Value = JsonSerializer.Serialize(tree);
    return (Guid)(await command.ExecuteScalarAsync())!;
}

async Task<T> QueryAsync<T>(string sql, object value)
{
    await using var connection = new SqlConnection(dsn);
    await connection.OpenAsync();
    await using var command = new SqlCommand(sql, connection);
    switch (value)
    {
        case Guid guid:
            command.Parameters.Add("@Run", SqlDbType.UniqueIdentifier).Value = guid;
            break;
        case long number:
            command.Parameters.Add("@Run", SqlDbType.BigInt).Value = number;
            break;
        case string json:
            command.Parameters.Add("@Run", SqlDbType.NVarChar, -1).Value = json;
            break;
        default:
            throw new ArgumentException("Unsupported integration test parameter");
    }
    return (T)(await command.ExecuteScalarAsync())!;
}

async Task ExpectStatusAsync(Guid run, string expected)
{
    await WaitAsync(async () => await QueryAsync<string>(
        "SELECT Status FROM batch.Submission WHERE Id=@Run", run) is "Success" or "Invalid" or "Failed");
    Equal(expected, await QueryAsync<string>("SELECT Status FROM batch.Submission WHERE Id=@Run", run), $"run {run}");
}

static async Task WaitAsync(Func<Task<bool>> condition)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(50));
    while (!await condition())
        await Task.Delay(50, timeout.Token);
}

static void Equal<T>(T expected, T actual, string operation)
{
    if (!EqualityComparer<T>.Default.Equals(expected, actual))
        throw new InvalidOperationException($"{operation}: expected {expected}, found {actual}");
}

ProcessStartInfo StartInfo(string command)
{
    var start = new ProcessStartInfo(dotnet) { UseShellExecute = false };
    start.ArgumentList.Add(engine);
    start.ArgumentList.Add(command);
    start.Environment["BATCH_SQL_CONNECTION"] = dsn;
    start.Environment["BATCH_TEST_BUSINESS_CONNECTION"] = dsn;
    start.Environment["BATCH_PLUGIN_PATHS"] = typeof(Fixture).Assembly.Location;
    start.Environment["BATCH_SLOTS"] = "1";
    start.Environment["BATCH_PAGE_SIZE"] = "2";
    start.Environment["BATCH_LEASE_SECONDS"] = "6";
    start.Environment["BATCH_SWEEP_SECONDS"] = "1";
    start.Environment["BATCH_MIN_POLL_MS"] = "50";
    start.Environment["BATCH_MAX_POLL_MS"] = "300";
    return start;
}

Process Worker(string name)
{
    var start = StartInfo("worker");
    start.Environment["BATCH_NODE_NAME"] = name;
    return Process.Start(start)!;
}

async Task RunCliAsync(string command)
{
    using var process = Process.Start(StartInfo(command))!;
    await process.WaitForExitAsync();
    Equal(0, process.ExitCode, command);
}

async Task<JsonElement> ReadStatusAsync(Guid run)
{
    var start = StartInfo("status");
    start.ArgumentList.Add(run.ToString());
    start.ArgumentList.Add("0");
    start.RedirectStandardOutput = true;
    using var process = Process.Start(start)!;
    var output = await process.StandardOutput.ReadToEndAsync();
    await process.WaitForExitAsync();
    Equal(0, process.ExitCode, "status");
    using var json = JsonDocument.Parse(output);
    return json.RootElement.Clone();
}

async Task StopWorkersAsync()
{
    foreach (var worker in workers)
    {
        if (!worker.HasExited)
            worker.Kill(entireProcessTree: true);
        await worker.WaitForExitAsync();
        worker.Dispose();
    }
    workers.Clear();
}
