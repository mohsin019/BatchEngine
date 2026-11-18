using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;

namespace BatchEngine;

internal sealed record WorkerOptions(
    string Id, int Slots, int PoolSize, int PageSize, int LeaseSeconds,
    int SweepSeconds, int MinPollMs, int MaxPollMs, int GraceSeconds)
{
    internal static WorkerOptions Load()
    {
        var node = Environment.GetEnvironmentVariable("BATCH_NODE_NAME") ?? Environment.MachineName;
        Protocol.Key(node, "Node name", 90);
        var minPoll = Read("BATCH_MIN_POLL_MS", 200, 25, 10_000);
        var instance = Guid.NewGuid().ToString("N")[..12];
        return new(
            $"{node}/{Environment.ProcessId}/{instance}", Read("BATCH_SLOTS", 8, 1, 64),
            Read("BATCH_POOL_SIZE", 3, 1, 16), Read("BATCH_PAGE_SIZE", 200, 1, 1000),
            Read("BATCH_LEASE_SECONDS", 45, 6, 3600), Read("BATCH_SWEEP_SECONDS", 6, 1, 300),
            minPoll, Read("BATCH_MAX_POLL_MS", Math.Max(5000, minPoll), minPoll, 60_000),
            Read("BATCH_GRACE_SECONDS", 15, 0, 120));
    }

    private static int Read(string name, int fallback, int min, int max)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (value is null)
            return fallback;
        if (!int.TryParse(value, out var integer) || integer < min || integer > max)
            throw new ArgumentException($"{name} must be an integer between {min} and {max}");
        return integer;
    }
}

internal sealed class Worker(Store store, PluginRegistry plugins, WorkerOptions options)
{
    private sealed record Active(int Slot, JobLease Lease, CancellationTokenSource Cancellation, Task<AttemptResult> Task);

    internal async Task RunAsync(CancellationToken stopping)
    {
        var stopwatch = Stopwatch.StartNew();
        var runners = Enumerable.Range(0, options.Slots)
            .Select(_ => new Runner(store, options.Id, options.PageSize)).ToArray();
        var active = new Dictionary<long, Active>();
        var slots = new Queue<int>(Enumerable.Range(0, options.Slots));
        var wake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            SingleReader = true, FullMode = BoundedChannelFullMode.DropWrite
        });

        void Wake() => wake.Writer.TryWrite(true);
        using var stopRegistration = stopping.Register(Wake);
        var nextPoll = 0L;
        var nextSweep = 0L;
        var nextRenew = long.MaxValue;
        var noWorkDelay = options.MinPollMs;
        var databaseDelay = 250;
        var flushAt = long.MaxValue;
        var stopAt = long.MaxValue;
        var renewEveryMs = options.LeaseSeconds * 1000L / 3;

        Console.Error.WriteLine($"Worker {options.Id} started with {options.Slots} slots");
        try
        {
            while (true)
            {
                var now = stopwatch.ElapsedMilliseconds;
                if (stopping.IsCancellationRequested)
                {
                    if (stopAt == long.MaxValue)
                        stopAt = now + options.GraceSeconds * 1000L;
                    if (active.Count == 0 || now >= stopAt)
                        break;
                }

                try
                {
                    if (active.Count != 0 && now >= nextRenew)
                    {
                        var renewed = await store.RenewAsync(active.Values.Select(item => item.Lease), options.LeaseSeconds);
                        foreach (var item in active.Values.Where(item => !renewed.Contains(item.Lease.Id)).ToArray())
                        {
                            await CancelAsync(item);
                            Release(item);
                        }
                        nextRenew = stopwatch.ElapsedMilliseconds + renewEveryMs;
                        nextPoll = now;
                    }

                    var finished = active.Values.Where(item => item.Task.IsCompleted).ToArray();
                    if (finished.Length > 0 && flushAt == long.MaxValue)
                        flushAt = now + 25;
                    if (finished.Length > 0 && (now >= flushAt || finished.Length == active.Count || stopping.IsCancellationRequested))
                    {
                        var completions = new List<Active>();
                        var results = new List<Settlement>();
                        var budget = 8 * 1024 * 1024;
                        foreach (var item in finished)
                        {
                            var result = await item.Task;
                            var bytes = EstimateResultSize(result);
                            if (completions.Count > 0 && bytes > budget)
                                break;
                            budget -= bytes;
                            completions.Add(item);
                            results.Add(new(item.Lease, result));
                        }
                        await store.SettleAsync([.. results]);
                        foreach (var item in completions)
                            Release(item);
                        flushAt = long.MaxValue;
                        nextPoll = now;
                    }
                    else if (finished.Length == 0)
                        flushAt = long.MaxValue;

                    if (!stopping.IsCancellationRequested && slots.Count > 0 && now >= nextPoll)
                    {
                        var performSweep = now >= nextSweep;
                        var (queued, leases) = await store.PollAsync(slots.Count, options.Id, options.LeaseSeconds, performSweep);
                        if (performSweep)
                            nextSweep = now + options.SweepSeconds * 1000L + Random.Shared.Next(250);
                        if (active.Count == 0 && leases.Length > 0)
                            nextRenew = stopwatch.ElapsedMilliseconds + renewEveryMs;
                        foreach (var lease in leases)
                        {
                            var slot = slots.Dequeue();
                            var cancellation = new CancellationTokenSource();
                            var task = runners[slot].RunAsync(lease, cancellation.Token);
                            active.Add(lease.Id, new(slot, lease, cancellation, task));
                            _ = task.ContinueWith(_ => Wake(), CancellationToken.None,
                                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                        }
                        if (queued.Length > 0)
                            await store.IngestAsync(queued.Select(item => Workflow.Prepare(item, plugins)).ToArray());

                        if (leases.Length > 0 || queued.Length > 0)
                        {
                            noWorkDelay = options.MinPollMs;
                            nextPoll = stopwatch.ElapsedMilliseconds + (queued.Length > 0 ? 25 : noWorkDelay);
                        }
                        else
                        {
                            nextPoll = stopwatch.ElapsedMilliseconds + noWorkDelay + Random.Shared.Next(noWorkDelay / 5 + 1);
                            noWorkDelay = Math.Min(options.MaxPollMs, noWorkDelay * 2);
                        }
                    }
                    else if (!stopping.IsCancellationRequested && now >= nextSweep)
                    {
                        await store.ReapAsync();
                        nextSweep = stopwatch.ElapsedMilliseconds + options.SweepSeconds * 1000L + Random.Shared.Next(250);
                    }
                    databaseDelay = 250;
                }
                catch (Exception ex) when (ex is SqlException or TimeoutException)
                {
                    Console.Error.WriteLine($"Batch database operation failed for {options.Id}: {Protocol.Error(ex)}");
                    var wait = Math.Min(databaseDelay + Random.Shared.Next(125),
                        Math.Max(0L, stopAt - stopwatch.ElapsedMilliseconds));
                    await Task.Delay(TimeSpan.FromMilliseconds(wait));
                    databaseDelay = Math.Min(databaseDelay * 2, 4000);
                    continue;
                }

                now = stopwatch.ElapsedMilliseconds;
                var due = Math.Min(stopAt, flushAt);
                if (active.Count > 0)
                    due = Math.Min(due, nextRenew);
                if (!stopping.IsCancellationRequested)
                {
                    due = Math.Min(due, nextSweep);
                    if (slots.Count > 0)
                        due = Math.Min(due, nextPoll);
                }

                if (due > now)
                {
                    using var wakeAfter = new CancellationTokenSource(TimeSpan.FromMilliseconds(Math.Min(due - now, 60_000)));
                    try { await wake.Reader.ReadAsync(wakeAfter.Token); }
                    catch (OperationCanceledException) when (wakeAfter.IsCancellationRequested) { }
                }
            }
        }
        finally
        {
            await Task.WhenAll(active.Values.Select(CancelAsync));
            foreach (var item in active.Values)
                item.Cancellation.Dispose();
            await Task.WhenAll(runners.Select(runner => runner.DisposeAsync().AsTask()));
        }

        void Release(Active item)
        {
            active.Remove(item.Lease.Id);
            slots.Enqueue(item.Slot);
            item.Cancellation.Dispose();
        }
    }

    private static async Task CancelAsync(Active item)
    {
        await item.Cancellation.CancelAsync();
        try { await item.Task; }
        catch (OperationCanceledException) when (item.Cancellation.IsCancellationRequested) { }
    }

    private static int EstimateResultSize(AttemptResult result) =>
        256 + Encoding.UTF8.GetByteCount(result.OutputJson ?? "") +
        (result.Records?.Sum(record => 64 + Encoding.UTF8.GetByteCount(record.PayloadJson) +
            Encoding.UTF8.GetByteCount(record.Key)) ?? 0);
}
