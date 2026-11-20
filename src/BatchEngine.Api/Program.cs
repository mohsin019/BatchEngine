using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using BatchEngine;
using Microsoft.Data.SqlClient;

var builder = WebApplication.CreateBuilder(args);
var connection = Environment.GetEnvironmentVariable("BATCH_SQL_CONNECTION")
    ?? throw new InvalidOperationException("Set BATCH_SQL_CONNECTION for the group API.");
var apiKey = Environment.GetEnvironmentVariable("BATCH_API_KEY");
if (string.IsNullOrWhiteSpace(apiKey))
    throw new InvalidOperationException("Set BATCH_API_KEY for the group API.");
var keyDigest = Hash(apiKey);

builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = Protocol.SubmissionBytes + 4096);
builder.Services.AddProblemDetails();
var store = new Store(connection, "BatchEngine API");

var app = builder.Build();
app.UseExceptionHandler();
app.Use(async (context, next) =>
{
    var auth = context.Request.Headers.Authorization;
    var header = auth.Count == 1 ? auth[0] : null;
    const string scheme = "Bearer ";
    if (header is null || !header.StartsWith(scheme, StringComparison.OrdinalIgnoreCase) ||
        !CryptographicOperations.FixedTimeEquals(Hash(header[scheme.Length..]), keyDigest))
    {
        context.Response.Headers.WWWAuthenticate = "Bearer";
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }

    try
    {
        await next(context);
    }
    catch (Exception ex) when (ex is InvalidDataException or ArgumentException or JsonException)
    {
        await Results.Problem(detail: ex.Message, statusCode: StatusCodes.Status400BadRequest).ExecuteAsync(context);
    }
    catch (SqlException ex) when (ex.Number is >= 51017 and <= 51020)
    {
        var status = ex.Number == 51018 ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict;
        await Results.Problem(detail: ex.Message, statusCode: status).ExecuteAsync(context);
    }
});

var api = app.MapGroup("/api");

api.MapGet("/groups", async (string? afterKey, int? limit, CancellationToken ct) =>
    Json(await store.ListGroupsAsync(ValidateCursor(afterKey), PageSize(limit), ct)));

api.MapGet("/process-configurations", async (string? afterKey, int? limit, CancellationToken ct) =>
    Json(await store.ListProcessConfigurationsAsync(ValidateCursor(afterKey), PageSize(limit), ct)));

api.MapGet("/groups/{key}", async (string key, CancellationToken ct) =>
{
    ValidateKey(key);
    var group = await store.ReadGroupAsync(key, ct);
    return group is null ? Results.NotFound() : Json(group);
});

api.MapPut("/groups/{key}", async (string key, SaveGroupRequest request, CancellationToken ct) =>
{
    ValidateKey(key);
    Protocol.Key(request.DisplayName, "Display name", 120);
    if (request.ExpectedRevision is null or < 0 || request.Children.ValueKind != JsonValueKind.Array)
        throw new InvalidDataException("Provide a nonnegative expectedRevision and a children array.");

    var definition = request.Lifecycle.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null
        ? JsonSerializer.SerializeToElement(new { key, type = "group", children = request.Children }, Protocol.Json)
        : JsonSerializer.SerializeToElement(new
        {
            key, type = "group", children = request.Children, lifecycle = request.Lifecycle
        }, Protocol.Json);
    var json = Protocol.Value(definition, "Group definition", Protocol.SubmissionBytes);
    var submission = new QueuedSubmission(Guid.Empty, json, "{}", SavedGroup: true);
    var configurations = await store.ReadConfigurationsAsync(Workflow.ConfigurationKeys(submission).ToArray(), ct);
    var validation = Workflow.Prepare(submission, configurations);
    if (validation.Error is not null)
        throw new InvalidDataException(validation.Error);

    var revision = await store.SaveGroupAsync(key, request.DisplayName!, json, request.ExpectedRevision.Value, ct);
    var saved = new { key, revision };
    return revision == 1 ? Results.Created($"/api/groups/{key}", saved) : Results.Ok(saved);
});

api.MapPost("/groups/{key}/runs", async (string key, SubmitGroupRequest request, CancellationToken ct) =>
{
    ValidateKey(key);
    if (request.Revision is null or <= 0)
        throw new InvalidDataException("Provide the group revision to submit.");
    if (request.SubmissionKey is not null)
        Protocol.Key(request.SubmissionKey, "Submission key", 200);
    var input = request.Input;
    if (input.ValueKind is not (JsonValueKind.Object or JsonValueKind.Undefined))
        throw new InvalidDataException("Input must be a JSON object.");
    var inputJson = input.ValueKind == JsonValueKind.Undefined ? "{}" :
        Protocol.Value(input, "Submission input", Protocol.SubmissionBytes);

    var id = await store.SubmitGroupAsync(key, request.Revision.Value, inputJson, request.SubmissionKey, ct);
    return Results.Accepted($"/api/runs/{id}", new { runId = id });
});

api.MapGet("/runs/{id:guid}", async (Guid id, int? taskLimit, CancellationToken ct) =>
{
    var limit = taskLimit ?? 0;
    if (limit is < 0 or > 100)
        throw new InvalidDataException("Task limit must be between 0 and 100.");
    var status = await store.StatusAsync(id, limit, ct);
    return status is null ? Results.NotFound() : Json(status);
});

await app.RunAsync();

static byte[] Hash(string value) => SHA256.HashData(Encoding.UTF8.GetBytes(value));
static IResult Json(string value) => Results.Text(value, "application/json");

static void ValidateKey(string key)
{
    if (!Workflow.IsNodeKey(key))
        throw new InvalidDataException("Use a key of up to 64 letters, digits, dots, underscores or hyphens starting with a letter or digit.");
}

static string? ValidateCursor(string? key)
{
    if (key is not null)
        ValidateKey(key);
    return key;
}

static int PageSize(int? limit) => (limit ?? 100) switch
{
    < 1 or > 100 => throw new InvalidDataException("Limit must be between 1 and 100."),
    var size => size
};

internal sealed record SaveGroupRequest(string? DisplayName, long? ExpectedRevision, JsonElement Children, JsonElement Lifecycle);
internal sealed record SubmitGroupRequest(long? Revision, string? SubmissionKey, JsonElement Input);
