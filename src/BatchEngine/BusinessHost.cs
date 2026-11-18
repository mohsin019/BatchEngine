using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using BatchEngine.Contracts;

namespace BatchEngine;

internal static partial class BusinessHost
{
    [LibraryImport("libc", EntryPoint = "prctl", SetLastError = true)]
    private static partial int Prctl(int option, nuint arg2, nuint arg3, nuint arg4, nuint arg5);

    [LibraryImport("libc", EntryPoint = "getppid")]
    private static partial int GetParentPid();

    internal static async Task RunAsync(int parentId)
    {
        if (OperatingSystem.IsLinux())
        {
            const int setParentDeathSignal = 1;
            const int sigKill = 9;
            if (Prctl(setParentDeathSignal, sigKill, 0, 0, 0) != 0)
                throw new IOException("Could not set the runner parent-death signal");
            if (GetParentPid() != parentId)
                return;
        }

        using var output = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false)) { AutoFlush = true };
        Console.SetOut(Console.Error);
        var input = Console.In;
        var registry = new PluginRegistry();
        using var shared = new SharedProxy(input, output);
        while (await input.ReadLineAsync() is { } line)
        {
            var request = JsonSerializer.Deserialize<RunnerRequest>(line, Protocol.Json)
                ?? throw new InvalidDataException("Missing runner request");
            var result = await ExecuteAsync(request, registry, shared);
            await output.WriteLineAsync(JsonSerializer.Serialize(new RunnerMessage("result", result), Protocol.Json));
        }
    }

    private static async Task<AttemptResult> ExecuteAsync(
        RunnerRequest request, PluginRegistry plugins, ISharedData shared)
    {
        var job = request.Job;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(job.TimeoutSeconds));
        var ct = timeout.Token;
        try
        {
            var plugin = plugins.Create(job.Handler);
            var options = Protocol.Parse(job.OptionsJson);
            var key = $"{job.RunId:N}:{job.Id}";
            if (job.Kind == "Fetch" && job.Cursor is not null)
                key += ":" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(job.Cursor)));

            if (job.Kind == "Task")
            {
                var context = new TaskContext(job.RunId, job.ProcessKey, options, job.Attempts,
                    key, request.WorkerId, shared, job.RecordKey!);
                return await ExecuteTaskAsync(plugin, context, Protocol.Parse(job.PayloadJson!), ct);
            }

            var process = new ProcessContext(job.RunId, job.ProcessKey, options, job.Attempts,
                key, request.WorkerId, shared, Protocol.Parse(job.InputJson!));
            switch (job.Kind)
            {
                case "Start":
                    await plugin.BeforeProcessAsync(process, ct);
                    break;
                case "Fetch":
                    var page = await plugin.FetchRecordsAsync(process, job.Cursor, request.PageSize, ct);
                    return PreparePage(page, request.PageSize);
                case "Finish":
                    await plugin.AfterProcessAsync(process, new ProcessOutcome(
                        Enum.Parse<BusinessStatus>(job.OwnStatus!), job.TotalTasks, job.SucceededTasks,
                        job.InvalidTasks, job.FailedTasks, job.SkippedTasks, job.ProcessError), ct);
                    break;
                default:
                    throw new PermanentProcessException("Unknown job stage");
            }
            return new("Success");
        }
        catch (Exception exception)
        {
            return Classify(exception, allowInvalid: job.Kind != "Finish");
        }
    }

    private static async Task<AttemptResult> ExecuteTaskAsync(
        IBatchProcess plugin, TaskContext context, JsonElement record, CancellationToken ct)
    {
        AttemptResult result;
        try
        {
            await plugin.BeforeTaskAsync(context, record, ct);
            var business = await plugin.RunTaskAsync(context, record, ct);
            if (business is null || business.Status is not (BusinessStatus.Success or BusinessStatus.Invalid))
                throw new InvalidDataException("RunTaskAsync must return Success or Invalid");
            if (business.Status == BusinessStatus.Invalid && (string.IsNullOrWhiteSpace(business.Reason) ||
                business.Reason.Length > 1800 || business.Reason.Contains('\0')))
                throw new InvalidDataException("Invalid results require a reason of up to 1,800 characters");

            var output = business.Output is { } value ? Protocol.Value(value, "Task output", Protocol.ValueBytes) : null;
            result = new(business.Status.ToString(), OutputJson: output,
                Error: business.Status == BusinessStatus.Invalid ? business.Reason : null);
        }
        catch (Exception exception)
        {
            result = Classify(exception);
        }

        try
        {
            var outcome = new AttemptOutcome(Enum.Parse<BusinessStatus>(result.Status),
                result.OutputJson is null ? null : Protocol.Parse(result.OutputJson), result.Error, result.Retryable);
            await plugin.AfterTaskAsync(context, record, outcome, ct);
        }
        catch (Exception exception)
        {
            return Classify(exception, allowInvalid: false);
        }
        return result;
    }

    private static AttemptResult PreparePage(RecordPage page, int limit)
    {
        if (page?.Records is null || page.Records.Count > limit)
            throw new InvalidDataException("FetchRecordsAsync must return a page within its requested limit");
        if (page.NextCursor is { } cursor)
            Protocol.Key(cursor, "Fetch cursor", 600);

        var records = new List<FetchedRecord>(page.Records.Count);
        foreach (var record in page.Records)
        {
            if (record is null)
                throw new InvalidDataException("FetchRecordsAsync returned a null record");
            Protocol.Key(record.Key, "Record key", 300);
            records.Add(new(record.Key, Protocol.Value(record.Payload, "Record payload", Protocol.ValueBytes)));
        }
        if (JsonSerializer.SerializeToUtf8Bytes(records, Protocol.Json).Length > Protocol.PageBytes)
            throw new InvalidDataException("A fetch page may contain up to 4 MiB of serialized records");
        return new("Success", Records: records, NextCursor: page.NextCursor);
    }

    private static AttemptResult Classify(Exception exception, bool allowInvalid = true)
    {
        if (allowInvalid && exception is BusinessInvalidException)
            return new("Invalid", Error: Protocol.Error(exception));
        return AttemptResult.Failure(Protocol.Error(exception),
            exception is not (PermanentProcessException or InvalidDataException or JsonException));
    }

    private sealed class SharedProxy(TextReader input, TextWriter output) : ISharedData, IDisposable
    {
        private readonly SemaphoreSlim gate = new(1);

        public async ValueTask<SharedValue?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            (await GetManyAsync([key], cancellationToken)).GetValueOrDefault(key);

        public async ValueTask<IReadOnlyDictionary<string, SharedValue>> GetManyAsync(
            IReadOnlyCollection<string> keys, CancellationToken cancellationToken = default)
        {
            if (keys.Count > 16)
                throw new ArgumentException("GetManyAsync accepts up to 16 keys", nameof(keys));
            foreach (var key in keys)
                Protocol.Key(key, "Shared key", 200);
            var reply = await ExchangeAsync(new("get", [.. keys.Distinct(StringComparer.Ordinal)]), cancellationToken);
            return (reply.Values ?? []).ToDictionary(
                row => row.Key, row => new SharedValue(Protocol.Parse(row.ValueJson), row.Version), StringComparer.Ordinal);
        }

        public async ValueTask<long> PutAsync(string key, JsonElement value, long? expectedVersion = null,
            CancellationToken cancellationToken = default)
        {
            Protocol.Key(key, "Shared key", 200);
            ArgumentOutOfRangeException.ThrowIfNegative(expectedVersion ?? 0);
            var json = Protocol.Value(value, "Shared value", Protocol.ValueBytes);
            var reply = await ExchangeAsync(new("put", Key: key, ValueJson: json, ExpectedVersion: expectedVersion),
                cancellationToken);
            return reply.Version!.Value;
        }

        private async Task<SharedReply> ExchangeAsync(SharedRequest request, CancellationToken ct)
        {
            await gate.WaitAsync(ct);
            try
            {
                await output.WriteLineAsync(JsonSerializer.Serialize(new RunnerMessage("shared", Shared: request), Protocol.Json));
                var line = await input.ReadLineAsync() ?? throw new EndOfStreamException("Shared storage disconnected");
                var reply = JsonSerializer.Deserialize<SharedReply>(line, Protocol.Json)
                    ?? throw new InvalidDataException("Missing shared storage response");
                if (reply.Conflict)
                    throw new SharedVersionConflictException(reply.Error!);
                if (reply.Error is not null)
                    throw new IOException(reply.Error);
                return reply;
            }
            finally { gate.Release(); }
        }

        public void Dispose() => gate.Dispose();
    }
}
