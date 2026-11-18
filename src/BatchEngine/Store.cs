using System.Data;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace BatchEngine;

internal sealed class Store
{
    private readonly string connectionString;

    internal Store(string connectionString, string applicationName, int poolSize = 3)
    {
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            ApplicationName = applicationName,
            MaxPoolSize = poolSize,
            MinPoolSize = 0,
            ConnectTimeout = 5,
            ConnectRetryCount = 0
        };
        this.connectionString = builder.ConnectionString;
    }

    internal async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(ct);
        await using var gate = new SqlCommand("""
            DECLARE @Result int;
            EXEC @Result = sys.sp_getapplock @Resource = N'batch.migration',
                @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 10000;
            IF @Result < 0 THROW 51012, 'Could not lock batch migrations', 1;
            """, connection, transaction) { CommandTimeout = 15 };
        await gate.ExecuteNonQueryAsync(ct);

        var assembly = typeof(Store).Assembly;
        foreach (var resource in assembly.GetManifestResourceNames().Where(s => s.EndsWith(".sql")).Order())
        {
            using var reader = new StreamReader(assembly.GetManifestResourceStream(resource)!);
            var script = await reader.ReadToEndAsync(ct);
            foreach (var batch in Regex.Split(script, @"^GO[ \t]*\r?$", RegexOptions.Multiline))
            {
                if (string.IsNullOrWhiteSpace(batch))
                    continue;
                await using var command = new SqlCommand(batch, connection, transaction) { CommandTimeout = 30 };
                await command.ExecuteNonQueryAsync(ct);
            }
        }
        await transaction.CommitAsync(ct);
    }

    internal async Task<Guid> SubmitAsync(string definition, string input, string? key, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = Procedure(connection, "batch.Submit");
        AddText(command, "@DefinitionJson", definition);
        AddText(command, "@InputJson", input);
        AddText(command, "@SubmissionKey", key, 200);
        return (Guid)(await command.ExecuteScalarAsync(ct))!;
    }

    internal async Task<(QueuedSubmission[] Queued, JobLease[] Claims)> PollAsync(
        int limit, string workerId, int leaseSeconds, bool reap, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand("""
            SET NOCOUNT ON;
            IF @Reap = 1 EXEC batch.Reap @Limit = 128;
            SELECT TOP (2) Id,
                CASE WHEN DATALENGTH(DefinitionJson) <= 4194304 THEN DefinitionJson END,
                CASE WHEN DATALENGTH(InputJson) <= 4194304 THEN InputJson END
            FROM batch.Submission WITH (READPAST, READCOMMITTEDLOCK)
            WHERE Status = 'Queued' ORDER BY SubmittedAt, Id;
            EXEC batch.Claim @Limit, @WorkerId, @LeaseSeconds;
            """, connection) { CommandTimeout = 5 };
        command.Parameters.Add("@Limit", SqlDbType.Int).Value = limit;
        command.Parameters.Add("@LeaseSeconds", SqlDbType.Int).Value = leaseSeconds;
        command.Parameters.Add("@Reap", SqlDbType.Bit).Value = reap;
        AddText(command, "@WorkerId", workerId, 128);

        var queued = new List<QueuedSubmission>();
        await using var data = await command.ExecuteReaderAsync(ct);
        while (await data.ReadAsync(ct))
            queued.Add(new(data.GetGuid(0), data.IsDBNull(1) ? null : data.GetString(1),
                data.IsDBNull(2) ? null : data.GetString(2)));

        if (!await data.NextResultAsync(ct) || !await data.ReadAsync(ct))
            throw new InvalidDataException("SQL Server did not return a claim result");
        return ([.. queued], JsonSerializer.Deserialize<JobLease[]>(data.GetString(0), Protocol.Json)!);
    }

    internal async Task<IReadOnlyDictionary<string, ProcessConfiguration>> ReadConfigurationsAsync(
        string[] keys, CancellationToken ct = default)
    {
        var configurations = new Dictionary<string, ProcessConfiguration>(StringComparer.Ordinal);
        if (keys.Length == 0)
            return configurations;

        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand("""
            SELECT c.ConfigurationKey, c.Handler, c.ChildrenMode, c.OptionsJson,
                c.MaxParallelTasks, c.MaxAttempts, c.TimeoutSeconds
            FROM batch.ProcessConfiguration c
            JOIN OPENJSON(@Keys) WITH (ConfigurationKey nvarchar(64) '$') k
                ON c.ConfigurationKey = k.ConfigurationKey COLLATE Latin1_General_100_BIN2;
            """, connection) { CommandTimeout = 5 };
        AddText(command, "@Keys", JsonSerializer.Serialize(keys, Protocol.Json));
        await using var data = await command.ExecuteReaderAsync(ct);
        while (await data.ReadAsync(ct))
            configurations.Add(data.GetString(0), new(data.GetString(1), data.GetString(2),
                data.GetString(3), data.GetInt32(4), data.GetInt32(5), data.GetInt32(6)));
        return configurations;
    }

    internal async Task IngestAsync(Intake[] plans, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = Procedure(connection, "batch.Ingest");
        AddText(command, "@Intake", JsonSerializer.Serialize(plans, Protocol.Json));
        await command.ExecuteNonQueryAsync(ct);
    }

    internal async Task<IReadOnlySet<long>> RenewAsync(IEnumerable<JobLease> leases, int leaseSeconds)
    {
        await using var connection = await OpenAsync(default);
        await using var command = Procedure(connection, "batch.Renew");
        AddText(command, "@Claims", JsonSerializer.Serialize(
            leases.Select(job => new { job.Id, Token = job.LeaseToken }), Protocol.Json));
        command.Parameters.Add("@LeaseSeconds", SqlDbType.Int).Value = leaseSeconds;
        await using var data = await command.ExecuteReaderAsync();
        var renewed = new HashSet<long>();
        while (await data.ReadAsync())
            renewed.Add(data.GetInt64(0));
        return renewed;
    }

    internal async Task SettleAsync(Settlement[] results)
    {
        await using var connection = await OpenAsync(default);
        await using var command = Procedure(connection, "batch.Settle");
        AddText(command, "@Results", JsonSerializer.Serialize(results, Protocol.Json));
        await command.ExecuteNonQueryAsync();
    }

    internal async Task ReapAsync()
    {
        await using var connection = await OpenAsync(default);
        await using var command = Procedure(connection, "batch.Reap");
        command.Parameters.Add("@Limit", SqlDbType.Int).Value = 128;
        await command.ExecuteNonQueryAsync();
    }

    internal async Task<SharedReply> ShareAsync(JobLease job, SharedRequest request, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = Procedure(connection, "batch.Share");
        command.Parameters.Add("@JobId", SqlDbType.BigInt).Value = job.Id;
        command.Parameters.Add("@Token", SqlDbType.UniqueIdentifier).Value = job.LeaseToken;
        command.Parameters.Add("@RunId", SqlDbType.UniqueIdentifier).Value = job.RunId;
        command.Parameters.Add("@Operation", SqlDbType.VarChar, 5).Value = request.Operation;
        AddText(command, "@Keys", request.Keys is null ? null : JsonSerializer.Serialize(request.Keys, Protocol.Json));
        AddText(command, "@Key", request.Key, 200);
        AddText(command, "@ValueJson", request.ValueJson);
        command.Parameters.Add("@ExpectedVersion", SqlDbType.BigInt).Value = (object?)request.ExpectedVersion ?? DBNull.Value;
        var response = (string)(await command.ExecuteScalarAsync(ct))!;
        return JsonSerializer.Deserialize<SharedReply>(response, Protocol.Json)!;
    }

    internal async Task<string?> StatusAsync(Guid id, int taskLimit, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand("""
            DECLARE @Overview nvarchar(max) = (
                SELECT s.Id, s.SubmissionKey, s.Status, s.Error, s.SubmittedAt, s.StartedAt, s.FinishedAt,
                    JSON_QUERY((
                        SELECT r.TotalTasks, r.ProcessedTasks, r.PendingTasks, r.RunningTasks,
                            r.SucceededTasks, r.InvalidTasks, r.FailedTasks, r.SkippedTasks,
                            r.TotalRecords, r.ProcessedRecords, r.PendingRecords, r.RunningRecords,
                            r.SucceededRecords, r.InvalidRecords, r.FailedRecords, r.SkippedRecords
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                    )) AS Progress,
                    JSON_QUERY((
                        SELECT n.NodeKey, p.NodeKey AS ParentKey, n.Position, n.Kind, n.Status, n.OwnStatus,
                            n.Phase, n.TotalTasks, n.RunningTasks, n.SucceededTasks, n.InvalidTasks, n.FailedTasks,
                            COALESCE(progress.SkippedTasks, 0) AS SkippedTasks,
                            COALESCE(progress.ProcessedTasks, 0) AS ProcessedTasks,
                            COALESCE(progress.PendingTasks, 0) AS PendingTasks,
                            n.MaxParallelTasks, n.TotalRecords, n.RunningRecords, n.SucceededRecords,
                            n.InvalidRecords, n.FailedRecords,
                            COALESCE(progress.SkippedRecords, 0) AS SkippedRecords,
                            COALESCE(progress.ProcessedRecords, 0) AS ProcessedRecords,
                            COALESCE(progress.PendingRecords, 0) AS PendingRecords,
                            n.StartedAt, n.FinishedAt, n.Error
                        FROM batch.Node n
                        LEFT JOIN batch.Node p ON n.ParentId = p.Id
                        LEFT JOIN batch.ProcessProgress progress ON progress.NodeId = n.Id
                        WHERE n.RunId = s.Id
                        ORDER BY n.NodeKey FOR JSON PATH
                    )) AS Nodes,
                    JSON_QUERY(COALESCE((
                        SELECT TOP (@TaskLimit) ProcessKey, Id, RecordKey, Status, [RowCount], Attempts, WorkerId,
                            OutputJson, Error, StartedAt, FinishedAt
                        FROM batch.TaskStatus WHERE RunId = s.Id ORDER BY Id FOR JSON PATH
                    ), N'[]')) AS Tasks
                FROM batch.Submission s JOIN batch.RunProgress r ON r.RunId = s.Id
                WHERE s.Id = @Id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
            SELECT @Overview;
            """, connection) { CommandTimeout = 5 };
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;
        command.Parameters.Add("@TaskLimit", SqlDbType.Int).Value = taskLimit;
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private async Task<SqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private static SqlCommand Procedure(SqlConnection connection, string name) =>
        new(name, connection) { CommandType = CommandType.StoredProcedure, CommandTimeout = 5 };

    private static void AddText(SqlCommand command, string name, string? value, int size = -1) =>
        command.Parameters.Add(name, SqlDbType.NVarChar, size).Value = (object?)value ?? DBNull.Value;
}
