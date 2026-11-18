using System.Data;
using Microsoft.Data.SqlClient;

namespace BatchEngine;

internal sealed partial class Store
{
    internal Task<string> ListGroupsAsync(string? afterKey, int limit, CancellationToken ct) =>
        ReadCatalogPageAsync("""
            SELECT @Json = (
                SELECT TOP (@Limit) GroupKey AS [key], DisplayName AS displayName, Revision AS revision, UpdatedAt AS updatedAt
                FROM batch.ProcessGroup
                WHERE @AfterKey IS NULL OR GroupKey > @AfterKey
                ORDER BY GroupKey FOR JSON PATH
            );
            """, afterKey, limit, ct);

    internal Task<string> ListProcessConfigurationsAsync(string? afterKey, int limit, CancellationToken ct) =>
        ReadCatalogPageAsync("""
            SELECT @Json = (
                SELECT TOP (@Limit) ConfigurationKey AS [key], Handler AS handler, Revision AS revision,
                    MaxParallelTasks AS maxParallelTasks, UpdatedAt AS updatedAt
                FROM batch.ProcessConfiguration
                WHERE @AfterKey IS NULL OR ConfigurationKey > @AfterKey
                ORDER BY ConfigurationKey FOR JSON PATH
            );
            """, afterKey, limit, ct);

    private async Task<string> ReadCatalogPageAsync(string query, string? afterKey, int limit, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand(
            "DECLARE @Json nvarchar(max);\n" + query + "\nSELECT COALESCE(@Json, N'[]');", connection)
            { CommandTimeout = 5 };
        AddText(command, "@AfterKey", afterKey, 64);
        command.Parameters.Add("@Limit", SqlDbType.Int).Value = limit;
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    internal async Task<string?> ReadGroupAsync(string key, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand("""
            DECLARE @Json nvarchar(max) = (
                SELECT GroupKey AS [key], DisplayName AS displayName, Revision AS revision,
                    UpdatedAt AS updatedAt, JSON_QUERY(DefinitionJson, '$.children') AS children
                FROM batch.ProcessGroup WHERE GroupKey = @GroupKey
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
            SELECT @Json;
            """, connection) { CommandTimeout = 5 };
        AddText(command, "@GroupKey", key, 64);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    internal async Task<long> SaveGroupAsync(string key, string displayName, string definition,
        long expectedRevision, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = Procedure(connection, "batch.SaveProcessGroup");
        AddText(command, "@GroupKey", key, 64);
        AddText(command, "@DisplayName", displayName, 120);
        AddText(command, "@DefinitionJson", definition);
        command.Parameters.Add("@ExpectedRevision", SqlDbType.BigInt).Value = expectedRevision;
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    internal async Task<Guid> SubmitGroupAsync(string key, long revision, string input, string? submissionKey,
        CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = Procedure(connection, "batch.Submit");
        AddText(command, "@GroupKey", key, 64);
        command.Parameters.Add("@GroupRevision", SqlDbType.BigInt).Value = revision;
        AddText(command, "@InputJson", input);
        AddText(command, "@SubmissionKey", submissionKey, 200);
        return (Guid)(await command.ExecuteScalarAsync(ct))!;
    }
}
