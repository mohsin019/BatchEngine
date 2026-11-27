using System.Diagnostics;
using System.Globalization;
using System.IO.Pipes;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace BatchEngine;

internal sealed class Runner(Store store, string workerId, int pageSize) : IAsyncDisposable
{
    private Process? process;
    private AnonymousPipeServerStream? shutdownPipe;

    internal void RequestStop() => Interlocked.Exchange(ref shutdownPipe, null)?.Dispose();

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
                {
                    deadline.ThrowIfCancellationRequested();
                    return outcome;
                }
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
            await StopAsync(force: true);
            throw;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            await StopAsync(force: true);
            return AttemptResult.TimedOut;
        }
        catch (Exception ex)
        {
            await StopAsync(force: true);
            return AttemptResult.Failure(Protocol.Error(ex));
        }
    }

    private Process GetProcess()
    {
        if (process is { HasExited: false })
            return process;

        RequestStop();
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

        shutdownPipe = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.Inheritable);
        start.ArgumentList.Add(shutdownPipe.GetClientHandleAsString());

        // The runner uses the worker's fenced shared-data interface.
        start.Environment.Remove("BATCH_SQL_CONNECTION");
        process = Process.Start(start) ?? throw new IOException("Could not start a business runner");
        shutdownPipe.DisposeLocalCopyOfClientHandle();
        return process;
    }

    private static async Task SendAsync<T>(Process child, T value, CancellationToken ct)
    {
        var text = JsonSerializer.Serialize(value, Protocol.Json);
        await child.StandardInput.WriteLineAsync(text.AsMemory(), ct);
        await child.StandardInput.FlushAsync(ct);
    }

    private async ValueTask StopAsync(bool force)
    {
        RequestStop();
        var child = process;
        process = null;
        if (child is null)
            return;
        using (child)
        {
            try
            {
                if (!force && !child.HasExited)
                {
                    child.StandardInput.Close();
                    using var grace = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                    try { await child.WaitForExitAsync(grace.Token); }
                    catch (OperationCanceledException) when (grace.IsCancellationRequested) { }
                }
                if (!child.HasExited)
                    child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync();
            }
            catch (InvalidOperationException) when (child.HasExited) { }
        }
    }

    public ValueTask DisposeAsync() => StopAsync(force: false);
}
