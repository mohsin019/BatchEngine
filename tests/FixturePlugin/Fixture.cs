using System.Globalization;
using System.Text.Json;
using BatchEngine.Contracts;

namespace FixturePlugin;

[ProcessHandler("integration.fixture.v1")]
public sealed class Fixture : IBatchProcess
{
    public async ValueTask BeforeProcessAsync(ProcessContext context, CancellationToken ct)
    {
        if (context.Options.TryGetProperty("requires", out var requirements))
        {
            var keys = requirements.EnumerateArray().Select(key => "end." + key.GetString()).ToArray();
            var found = await context.Shared.GetManyAsync(keys, ct);
            if (found.Count != keys.Length)
                throw new PermanentProcessException("A required after-process hook did not finish");
        }
        var flag = JsonSerializer.SerializeToElement(true);
        await context.Shared.PutAsync("start." + context.ProcessKey, flag, cancellationToken: ct);
    }

    public async ValueTask<RecordPage> FetchRecordsAsync(ProcessContext context, string? cursor, int limit, CancellationToken ct)
    {
        if (await context.Shared.GetAsync("start." + context.ProcessKey, ct) is null)
            throw new PermanentProcessException("Before-process hook did not run");
        var from = cursor is null ? 0 : int.Parse(cursor, CultureInfo.InvariantCulture);
        var count = context.Options.GetProperty("count").GetInt32();
        var to = Math.Min(count, from + limit);
        var records = Enumerable.Range(from, to - from).Select(i =>
            new Record(i.ToString(CultureInfo.InvariantCulture), JsonSerializer.SerializeToElement(i))).ToArray();
        if (Mode(context.Options) == "cursor-cycle")
            return new(records, "0");
        return new(records, to < count ? to.ToString(CultureInfo.InvariantCulture) : null);
    }

    private bool beforeTaskRan;

    public ValueTask BeforeTaskAsync(TaskContext context, JsonElement record, CancellationToken ct)
    {
        beforeTaskRan = true;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<TaskResult> RunTaskAsync(TaskContext context, JsonElement record, CancellationToken ct)
    {
        if (!beforeTaskRan)
            throw new PermanentProcessException("Before-task hook did not run");
        var mode = Mode(context.Options);
        if (mode == "permanent")
            throw new PermanentProcessException("Unrecoverable sample failure");
        if (mode == "timeout" && context.Attempt == 1)
            Thread.Sleep(Timeout.Infinite);
        if (mode == "crash" && context.Attempt == 1)
        {
            await context.Shared.PutAsync("crash.entered", JsonSerializer.SerializeToElement(Environment.ProcessId),
                expectedVersion: 0, cancellationToken: ct);
            Thread.Sleep(Timeout.Infinite);
        }

        if (mode == "hold")
        {
            while (await context.Shared.GetAsync("integration.release", ct) is null)
                await Task.Delay(150, ct);
        }

        await Task.Delay(300, ct);
        if (mode == "mixed" && record.GetInt32() == 0)
            return TaskResult.Invalid("Sample rejection");
        if (mode == "mixed" && record.GetInt32() == 1 && context.Attempt == 1)
            throw new IOException("Sample transient failure");
        return TaskResult.Success(JsonSerializer.SerializeToElement(context.IdempotencyKey));
    }

    public async ValueTask AfterTaskAsync(TaskContext context, JsonElement record, AttemptOutcome outcome,
        CancellationToken ct)
    {
        if (Mode(context.Options) == "mixed")
            await context.Shared.PutAsync($"task-end.{context.RecordKey}.{context.Attempt}",
                JsonSerializer.SerializeToElement(outcome.Status.ToString()), cancellationToken: ct);
    }

    public async ValueTask AfterProcessAsync(ProcessContext context, ProcessOutcome outcome, CancellationToken ct)
    {
        var value = JsonSerializer.SerializeToElement(outcome.Status.ToString());
        await context.Shared.PutAsync("end." + context.ProcessKey, value, cancellationToken: ct);
    }

    private static string? Mode(JsonElement options) => options.GetProperty("mode").GetString();
}
