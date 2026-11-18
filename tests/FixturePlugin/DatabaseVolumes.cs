using System.Data;
using System.Globalization;
using System.Text.Json;
using BatchEngine.Contracts;
using Microsoft.Data.SqlClient;

namespace FixturePlugin;

[ProcessHandler("integration.volumes.v1")]
public sealed class DatabaseVolumes : IBatchProcess
{
    private sealed record SourceRow(int Id, int Amount);

    public async ValueTask<RecordPage> FetchRecordsAsync(
        ProcessContext context, string? cursor, int limit, CancellationToken ct)
    {
        var rowsPerVolume = context.Options.GetProperty("rowsPerVolume").GetInt32();
        var rowLimit = checked(limit * rowsPerVolume);
        var after = cursor is null ? 0 : int.Parse(cursor, CultureInfo.InvariantCulture);
        var connectionString = Environment.GetEnvironmentVariable("BATCH_TEST_BUSINESS_CONNECTION")
            ?? throw new PermanentProcessException("The test business database connection is missing");
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("""
            SELECT TOP (@Limit) Id, Amount FROM dbo.BatchEngineIntegrationVolumeRow
            WHERE FixtureId = @FixtureId AND Id > @After ORDER BY Id;
            """, connection);
        command.Parameters.Add("@Limit", SqlDbType.Int).Value = rowLimit;
        command.Parameters.Add("@FixtureId", SqlDbType.UniqueIdentifier).Value =
            context.Options.GetProperty("fixtureId").GetGuid();
        command.Parameters.Add("@After", SqlDbType.Int).Value = after;

        var rows = new List<SourceRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(new(reader.GetInt32(0), reader.GetInt32(1)));

        var volumes = rows.Chunk(rowsPerVolume)
            .Select(chunk => Record.FromResultSet(chunk[0].Id.ToString(CultureInfo.InvariantCulture), chunk))
            .ToArray();
        return new(volumes, rows.Count == rowLimit ? rows[^1].Id.ToString(CultureInfo.InvariantCulture) : null);
    }

    public async ValueTask<TaskResult> RunTaskAsync(TaskContext context, JsonElement resultSet, CancellationToken ct)
    {
        var rows = resultSet.Deserialize<SourceRow[]>()
            ?? throw new PermanentProcessException("Expected a result set");
        var firstId = int.Parse(context.RecordKey, CultureInfo.InvariantCulture);
        if (rows.Length != 2 || rows[0].Id != firstId || rows[1].Id != firstId + 1)
            throw new PermanentProcessException("The result set did not match its volume key");

        while (await context.Shared.GetAsync("volumes.release", ct) is null)
            await Task.Delay(150, ct);

        if (firstId == 1 && context.Attempt == 1)
            throw new IOException("Sample transient volume failure");

        var output = JsonSerializer.SerializeToElement(rows.Select(row => row.Id));
        if (rows.Any(row => row.Amount == 99))
            throw new PermanentProcessException("Sample technical error for this volume");
        return rows.Any(row => row.Amount < 0)
            ? TaskResult.Invalid("Sample rejection for this volume", output)
            : TaskResult.Success(output);
    }
}
