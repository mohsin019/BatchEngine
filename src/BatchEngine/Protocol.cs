using System.Text;
using System.Text.Json;

namespace BatchEngine;

internal static class Protocol
{
    internal const int ValueBytes = 256 * 1024;
    internal const int PageBytes = 4 * 1024 * 1024;
    internal const int SubmissionBytes = 2 * 1024 * 1024;
    internal static readonly JsonSerializerOptions Json = new()
    {
        MaxDepth = 64,
        PropertyNameCaseInsensitive = true
    };

    internal static JsonElement Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    internal static string Value(JsonElement value, string field, int limit)
    {
        if (value.ValueKind == JsonValueKind.Undefined)
            throw new InvalidDataException(field + " must be a JSON value");
        var json = JsonSerializer.Serialize(value, Json);
        if (Encoding.UTF8.GetByteCount(json) > limit)
            throw new InvalidDataException(field + " exceeds its size limit");
        return json;
    }

    internal static void Key(string? value, string field, int limit)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > limit ||
            value.Trim() != value || value.Any(char.IsControl))
            throw new InvalidDataException($"{field} must be 1–{limit} characters without controls or edge whitespace");
    }

    internal static string Error(Exception exception)
    {
        var text = $"{exception.GetType().Name}: {exception.Message}".Replace('\0', '\uFFFD');
        return text.Length > 1800 ? text[..1800] : text;
    }
}

internal sealed record JobLease(
    long Id, Guid RunId, string ProcessKey, string Kind, string Handler, string OptionsJson,
    string? InputJson, string? RecordKey, string? PayloadJson, string? Cursor, int Attempts,
    Guid LeaseToken, int TimeoutSeconds, string? OwnStatus, long TotalTasks,
    long SucceededTasks, long InvalidTasks, long FailedTasks, long SkippedTasks, string? ProcessError);

internal sealed record FetchedRecord(string Key, string PayloadJson, int RowCount);
internal sealed record AttemptResult(
    string Status, bool Retryable = false, string? OutputJson = null, string? Error = null,
    IReadOnlyList<FetchedRecord>? Records = null, string? NextCursor = null)
{
    internal static AttemptResult Failure(string error, bool retryable = true) =>
        new("Failed", retryable, Error: error);
}

internal sealed record Settlement(
    long Id, Guid Token, string Status, bool Retryable, string? OutputJson, string? Error,
    IReadOnlyList<FetchedRecord>? Records, string? NextCursor)
{
    internal Settlement(JobLease lease, AttemptResult result)
        : this(lease.Id, lease.LeaseToken, result.Status, result.Retryable, result.OutputJson,
            result.Error, result.Records, result.NextCursor) { }
}

internal sealed record RunnerRequest(JobLease Job, int PageSize, string WorkerId);
internal sealed record SharedRequest(
    string Operation, string[]? Keys = null, string? Key = null, string? ValueJson = null, long? ExpectedVersion = null);
internal sealed record SharedRow(string Key, string ValueJson, long Version);
internal sealed record SharedReply(
    SharedRow[]? Values = null, long? Version = null, string? Error = null, bool Conflict = false);
internal sealed record RunnerMessage(string Type, AttemptResult? Result = null, SharedRequest? Shared = null);
