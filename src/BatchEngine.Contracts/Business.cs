using System.Text.Json;

namespace BatchEngine.Contracts;

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ProcessHandlerAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

public enum BusinessStatus { Success, Invalid, Failed }

public sealed record Record(string Key, JsonElement Payload);
public sealed record RecordPage(IReadOnlyList<Record> Records, string? NextCursor = null);
public sealed record TaskResult(BusinessStatus Status, JsonElement? Output = null, string? Reason = null)
{
    public static TaskResult Success(JsonElement? output = null) => new(BusinessStatus.Success, output);
    public static TaskResult Invalid(string reason, JsonElement? output = null) =>
        new(BusinessStatus.Invalid, output, reason);
}

public sealed record AttemptOutcome(BusinessStatus Status, JsonElement? Output, string? Error, bool MayRetry);
public sealed record ProcessOutcome(
    BusinessStatus Status, long Total, long Succeeded, long Invalid, long Failed, long Skipped, string? Error);

public sealed class BusinessInvalidException(string message) : Exception(message);
public sealed class PermanentProcessException(string message) : Exception(message);
public sealed class SharedVersionConflictException(string message) : Exception(message);

public sealed record SharedValue(JsonElement Value, long Version);

public interface ISharedData
{
    ValueTask<SharedValue?> GetAsync(string key, CancellationToken cancellationToken = default);
    ValueTask<IReadOnlyDictionary<string, SharedValue>> GetManyAsync(
        IReadOnlyCollection<string> keys, CancellationToken cancellationToken = default);
    ValueTask<long> PutAsync(string key, JsonElement value, long? expectedVersion = null,
        CancellationToken cancellationToken = default);
}

public abstract record ExecutionContext(
    Guid RunId, string ProcessKey, JsonElement Options, int Attempt,
    string IdempotencyKey, string WorkerId, ISharedData Shared);

public sealed record ProcessContext(
    Guid RunId, string ProcessKey, JsonElement Options, int Attempt, string IdempotencyKey,
    string WorkerId, ISharedData Shared, JsonElement Input)
    : ExecutionContext(RunId, ProcessKey, Options, Attempt, IdempotencyKey, WorkerId, Shared);

public sealed record TaskContext(
    Guid RunId, string ProcessKey, JsonElement Options, int Attempt,
    string IdempotencyKey, string WorkerId, ISharedData Shared, string RecordKey)
    : ExecutionContext(RunId, ProcessKey, Options, Attempt, IdempotencyKey, WorkerId, Shared);

public interface IBatchProcess
{
    ValueTask BeforeProcessAsync(ProcessContext context, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
    ValueTask<RecordPage> FetchRecordsAsync(ProcessContext context, string? cursor, int limit,
        CancellationToken cancellationToken);
    ValueTask BeforeTaskAsync(TaskContext context, JsonElement record, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
    ValueTask<TaskResult> RunTaskAsync(TaskContext context, JsonElement record, CancellationToken cancellationToken);
    ValueTask AfterTaskAsync(TaskContext context, JsonElement record, AttemptOutcome outcome,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
    ValueTask AfterProcessAsync(ProcessContext context, ProcessOutcome outcome, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}
