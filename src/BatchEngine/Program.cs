using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Text.Json;
using BatchEngine;

try
{
    if (args is ["business-host", var parent, var stopPipe] && int.TryParse(parent, out var parentId))
    {
        await BusinessHost.RunAsync(parentId, stopPipe);
        return 0;
    }

    if (args.Length == 0 || args[0] is "-h" or "--help")
    {
        Console.WriteLine("""
            BatchEngine (.NET 10 / SQL Server)
            Set BATCH_SQL_CONNECTION before running these commands.

              init-db
              worker
              submit <workflow.json> [input.json] [submission-key]
              resume <run-id>
              status <run-id> [task-limit]
            """);
        return 0;
    }

    var connection = Environment.GetEnvironmentVariable("BATCH_SQL_CONNECTION")
        ?? throw new ArgumentException("Set BATCH_SQL_CONNECTION to the SQL Server database connection string");

    if (args is ["worker"])
    {
        var options = WorkerOptions.Load();
        var store = new Store(connection, $"BatchEngine/{options.Id}", options.PoolSize);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddWindowsService(service => service.ServiceName = "BatchEngine");
        builder.Services.Configure<HostOptions>(host =>
            host.ShutdownTimeout = TimeSpan.FromSeconds(options.GraceSeconds + 30));
        builder.Services.AddSingleton(store);
        builder.Services.AddSingleton(options);
        builder.Services.AddHostedService<BatchWorkerService>();
        using var host = builder.Build();
        await host.RunAsync();
        return 0;
    }

    var cli = new Store(connection, "BatchEngine CLI");
    if (args is ["init-db"])
    {
        await cli.InitializeAsync();
        Console.WriteLine("Batch schema installed.");
        return 0;
    }

    if (args[0] == "submit" && args.Length is >= 2 and <= 4)
    {
        var definition = await File.ReadAllTextAsync(args[1]);
        var input = args.Length >= 3 ? await File.ReadAllTextAsync(args[2]) : "{}";
        var key = args.Length == 4 ? args[3] : null;
        if (key is not null)
            Protocol.Key(key, "Submission key", 200);
        Console.WriteLine(await cli.SubmitAsync(
            Protocol.Value(Protocol.Parse(definition), "Definition", Protocol.SubmissionBytes),
            Protocol.Value(Protocol.Parse(input), "Input", Protocol.SubmissionBytes), key));
        return 0;
    }

    if (args is ["resume", var run] && Guid.TryParse(run, out var resumeId))
    {
        await cli.ResumeAsync(resumeId);
        Console.WriteLine(resumeId);
        return 0;
    }

    if (args[0] == "status" && args.Length is 2 or 3 && Guid.TryParse(args[1], out var id))
    {
        var limit = args.Length == 3 ? int.Parse(args[2]) : 20;
        if (limit is < 0 or > 100)
            throw new ArgumentOutOfRangeException(nameof(limit), "Task limit must be between 0 and 100");
        var json = await cli.StatusAsync(id, limit);
        if (json is null)
        {
            Console.Error.WriteLine("Submission was not found.");
            return 1;
        }
        Console.WriteLine(JsonSerializer.Serialize(Protocol.Parse(json), new JsonSerializerOptions { WriteIndented = true }));
        return 0;
    }

    Console.Error.WriteLine("Invalid command. Run --help for usage.");
    return 2;
}
catch (Exception ex)
{
    Console.Error.WriteLine(Protocol.Error(ex));
    return 1;
}
