using System.Globalization;
using System.Text.Json;
using BatchEngine.Contracts;

namespace BatchEngine;

[ProcessHandler("demo.configure.v1")]
public sealed class ConfigureDemo : IBatchProcess
{
    public async ValueTask BeforeProcessAsync(ProcessContext context, CancellationToken ct)
    {
        if (!context.Input.TryGetProperty("limit", out var limit) || !limit.TryGetInt32(out _))
            throw new BusinessInvalidException("An integer limit is required");
        await context.Shared.PutAsync("demo.limit", limit, cancellationToken: ct);
    }

    public ValueTask<RecordPage> FetchRecordsAsync(ProcessContext context, string? cursor, int limit, CancellationToken ct) =>
        ValueTask.FromResult(new RecordPage([]));

    public ValueTask<TaskResult> RunTaskAsync(TaskContext context, JsonElement record, CancellationToken ct) =>
        ValueTask.FromResult(TaskResult.Success());
}

[ProcessHandler("demo.validate.v1")]
public sealed class ValidateDemo : IBatchProcess
{
    public async ValueTask<RecordPage> FetchRecordsAsync(ProcessContext context, string? cursor, int limit, CancellationToken ct)
    {
        var shared = await context.Shared.GetAsync("demo.limit", ct)
            ?? throw new BusinessInvalidException("Configure the validation limit first");
        var rows = context.Input.GetProperty("records");
        var start = cursor is null ? 0 : int.Parse(cursor, CultureInfo.InvariantCulture);
        var end = Math.Min(start + limit, rows.GetArrayLength());
        var records = new List<Record>(end - start);
        for (var index = start; index < end; index++)
        {
            var row = rows[index];
            records.Add(new(row.GetProperty("id").GetString()!,
                JsonSerializer.SerializeToElement(new { amount = row.GetProperty("amount"), limit = shared.Value })));
        }
        return new(records, end < rows.GetArrayLength() ? end.ToString(CultureInfo.InvariantCulture) : null);
    }

    public async ValueTask<TaskResult> RunTaskAsync(TaskContext context, JsonElement record, CancellationToken ct)
    {
        await Task.Delay(75, ct);
        var amount = record.GetProperty("amount").GetInt32();
        var limit = record.GetProperty("limit").GetInt32();
        return amount > limit
            ? TaskResult.Invalid("Amount is above the shared limit", record)
            : TaskResult.Success(record);
    }

    public async ValueTask AfterProcessAsync(ProcessContext context, ProcessOutcome outcome, CancellationToken ct) =>
        await context.Shared.PutAsync("summary." + context.ProcessKey, JsonSerializer.SerializeToElement(outcome),
            cancellationToken: ct);
}
