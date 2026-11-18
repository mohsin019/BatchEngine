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
        Step("healthy", 1),
        Step("hard-timeout", 1, mode: "timeout", timeout: 1),
        Step("fetch-error", 5, mode: "cursor-cycle"));

    var run = await SubmitAsync(tree);
    await ExpectStatusAsync(run, "Failed");
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
    Console.WriteLine("Passed permanent failure, branch skipping, cursor-cycle detection, timeout and validation.");
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
