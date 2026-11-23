using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace BatchEngine;

internal sealed class Runner(Store store, string workerId, int pageSize) : IAsyncDisposable
{
    private Process? process;

    internal async Task<AttemptResult> RunAsync(JobLease job, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (job.AttemptTimeout is { } limit)
            timeout.CancelAfter(limit);
        var deadline = timeout.Token;
        try
        {
            var child = GetProcess();
            await SendAsync(child, new RunnerRequest(job, pageSize, workerId), deadline);
            while (true)
            {
                var line = await child.StandardOutput.ReadLineAsync(deadline)
                    ?? throw new EndOfStreamException("Business runner exited during an attempt");
                var message = JsonSerializer.Deserialize<RunnerMessage>(line, Protocol.Json)
                    ?? throw new InvalidDataException("Missing business runner message");
                if (message is { Type: "result", Result: { } outcome })
                    return outcome;
                if (message is not { Type: "shared", Shared: { } request })
                    throw new InvalidDataException("Unexpected business runner message");

                SharedReply reply;
                try { reply = await store.ShareAsync(job, request, deadline); }
                catch (Exception ex) when (ex is SqlException or TimeoutException)
                {
                    reply = new(Error: "The shared SQL Server store is temporarily unavailable");
                }
                await SendAsync(child, reply, deadline);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await StopAsync();
            throw;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            await StopAsync();
            return AttemptResult.Failure("Attempt timed out");
        }
        catch (Exception ex)
        {
            await StopAsync();
            return AttemptResult.Failure(Protocol.Error(ex));
        }
    }

    private Process GetProcess()
    {
        if (process is { HasExited: false })
            return process;

        process?.Dispose();
        process = null;
        var path = Environment.ProcessPath ?? throw new InvalidOperationException("Cannot locate the worker executable");
        var start = new ProcessStartInfo(path)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            UseShellExecute = false
        };
        if (Path.GetFileNameWithoutExtension(path).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            start.ArgumentList.Add(typeof(Runner).Assembly.Location);
        start.ArgumentList.Add("business-host");
        start.ArgumentList.Add(Environment.ProcessId.ToString(CultureInfo.InvariantCulture));

        // The runner uses the worker's fenced shared-data interface.
        start.Environment.Remove("BATCH_SQL_CONNECTION");
        return process = Process.Start(start) ?? throw new IOException("Could not start a business runner");
    }

    private static async Task SendAsync<T>(Process child, T value, CancellationToken ct)
    {
        var text = JsonSerializer.Serialize(value, Protocol.Json);
        await child.StandardInput.WriteLineAsync(text.AsMemory(), ct);
        await child.StandardInput.FlushAsync(ct);
    }

    private async ValueTask StopAsync()
    {
        var child = process;
        process = null;
        if (child is null)
            return;
        using (child)
        {
            try
            {
                if (!child.HasExited)
                    child.Kill(entireProcessTree: true);
                using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await child.WaitForExitAsync(wait.Token);
            }
            catch (InvalidOperationException) when (child.HasExited) { }
            catch (OperationCanceledException) { }
        }
    }

    public ValueTask DisposeAsync() => StopAsync();
}
