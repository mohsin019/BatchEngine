using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace BatchEngine;

internal sealed record PlanNode(
    Guid Id, Guid? ParentId, int Position, string NodeKey, string Kind, string ChildrenMode,
    string? Handler, string OptionsJson, int MaxParallelTasks, int MaxAttempts, int TimeoutSeconds, int ChildCount);
internal sealed record QueuedSubmission(Guid Id, string? DefinitionJson, string? InputJson, bool SavedGroup = false);
internal sealed record Intake(Guid Id, PlanNode[]? Nodes, string? Error);
internal sealed record ProcessConfiguration(string Handler, string ChildrenMode, string OptionsJson,
    int MaxParallelTasks, int MaxAttempts, int TimeoutSeconds);

internal static partial class Workflow
{
    [GeneratedRegex(@"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\z", RegexOptions.CultureInvariant)]
    private static partial Regex NodeKey();

    internal static bool IsNodeKey(string key) => NodeKey().IsMatch(key);

    internal static IEnumerable<string> ConfigurationKeys(QueuedSubmission submission)
    {
        if (submission.DefinitionJson is null ||
            Encoding.UTF8.GetByteCount(submission.DefinitionJson) > Protocol.SubmissionBytes)
            return [];
        try
        {
            var keys = new HashSet<string>(StringComparer.Ordinal);
            var nodes = new Stack<JsonElement>();
            nodes.Push(Protocol.Parse(submission.DefinitionJson));
            for (var visited = 0; visited < 1000 && nodes.TryPop(out var node); visited++)
            {
                if (node.ValueKind != JsonValueKind.Object)
                    continue;
                if (node.TryGetProperty("configuration", out var value) && value.ValueKind == JsonValueKind.String &&
                    value.GetString() is { } key && IsNodeKey(key))
                    keys.Add(key);
                if (node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array &&
                    children.GetArrayLength() <= 1000)
                    foreach (var child in children.EnumerateArray())
                        nodes.Push(child);
            }
            return keys;
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException)
        {
            // Prepare records the validation error on this submission.
            return [];
        }
    }

    internal static Intake Prepare(QueuedSubmission submission, Func<string, bool> hasHandler,
        IReadOnlyDictionary<string, ProcessConfiguration> configurations)
    {
        try
        {
            if (submission.DefinitionJson is null || submission.InputJson is null ||
                Encoding.UTF8.GetByteCount(submission.DefinitionJson) > Protocol.SubmissionBytes ||
                Encoding.UTF8.GetByteCount(submission.InputJson) > Protocol.SubmissionBytes)
                throw new InvalidDataException("Definition and input are limited to 2 MiB each");

            var definition = Protocol.Parse(submission.DefinitionJson);
            if (Protocol.Parse(submission.InputJson).ValueKind != JsonValueKind.Object)
                throw new InvalidDataException("Submission input must be a JSON object");

            var nodes = new List<PlanNode>();
            var keys = new HashSet<string>(StringComparer.Ordinal);
            Visit(definition, null, 0, 1);
            return new(submission.Id, [.. nodes], null);

            void Visit(JsonElement node, PlanNode? parent, int position, int depth)
            {
                if (depth > 24 || nodes.Count >= 1000)
                    throw new InvalidDataException("Definitions are limited to 24 levels and 1,000 nodes");
                if (node.ValueKind != JsonValueKind.Object)
                    throw new InvalidDataException("Each definition node must be a JSON object");

                var key = RequiredString(node, "key");
                if (!IsNodeKey(key) || !keys.Add(key))
                    throw new InvalidDataException($"Invalid or repeated node key: {key}");
                var kind = RequiredString(node, "type");
                if (kind is not ("group" or "process") || (parent is null && kind != "group") ||
                    (parent?.Kind == "Process" && kind == "group"))
                    throw new InvalidDataException($"{key}: use a group root and only process children under a process");

                var allowed = new HashSet<string>(["key", "type", "children", "children_mode"]);
                var configured = kind == "process" && node.TryGetProperty("configuration", out _);
                if (submission.SavedGroup && kind == "process" && !configured)
                    throw new InvalidDataException($"{key}: a saved group process must reference a process configuration");
                if (submission.SavedGroup)
                    allowed.Remove("children_mode");
                if (configured)
                {
                    allowed.Remove("children_mode");
                    allowed.Add("configuration");
                }
                else if (kind == "process")
                    allowed.UnionWith(["handler", "options", "max_parallel_tasks", "max_attempts", "timeout_seconds"]);
                foreach (var property in node.EnumerateObject())
                    if (!allowed.Remove(property.Name))
                        throw new InvalidDataException($"{key}: unknown or repeated field {property.Name}");

                ProcessConfiguration? configuration = null;
                if (configured)
                {
                    var configurationKey = RequiredString(node, "configuration");
                    if (!configurations.TryGetValue(configurationKey, out configuration))
                        throw new InvalidDataException($"{key}: unknown process configuration '{configurationKey}'");
                }

                var mode = submission.SavedGroup ? "serial" :
                    configuration?.ChildrenMode ?? (node.TryGetProperty("children_mode", out _)
                        ? RequiredString(node, "children_mode") : "serial");
                if (mode is not ("serial" or "parallel"))
                    throw new InvalidDataException($"{key}: children_mode must be serial or parallel");

                var children = Array.Empty<JsonElement>();
                if (node.TryGetProperty("children", out var childArray))
                {
                    if (childArray.ValueKind != JsonValueKind.Array)
                        throw new InvalidDataException($"{key}: children must be an array");
                    if (childArray.GetArrayLength() > 1000)
                        throw new InvalidDataException("Each node can have up to 1,000 children");
                    children = [.. childArray.EnumerateArray()];
                }

                var handler = configuration?.Handler ?? (kind == "process" ? RequiredString(node, "handler") : null);
                if (handler is not null && !hasHandler(handler))
                    throw new InvalidDataException($"{key}: handler '{handler}' is not registered");
                var options = configuration is not null ? Protocol.Parse(configuration.OptionsJson) :
                    node.TryGetProperty("options", out var value) ? value : Protocol.Parse("{}");
                if (options.ValueKind != JsonValueKind.Object)
                    throw new InvalidDataException($"{key}: options must be a JSON object");
                var optionsJson = Protocol.Value(options, "Process options", 64 * 1024);

                var item = new PlanNode(Guid.NewGuid(), parent?.Id, position, key,
                    kind == "group" ? "Group" : "Process", mode, handler, optionsJson,
                    configuration?.MaxParallelTasks ?? Integer(node, "max_parallel_tasks", 1, 256),
                    configuration?.MaxAttempts ?? Integer(node, "max_attempts", 3, 20),
                    configuration?.TimeoutSeconds ?? Integer(node, "timeout_seconds", 60, 86400), children.Length);
                nodes.Add(item);

                for (var index = 0; index < children.Length; index++)
                    Visit(children[index], item, index, depth + 1);
            }
        }
        catch (Exception exception) when (exception is JsonException or InvalidDataException or InvalidOperationException)
        {
            return new(submission.Id, null, Protocol.Error(exception));
        }
    }

    private static string RequiredString(JsonElement node, string name)
    {
        if (!node.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String)
            throw new InvalidDataException($"'{name}' must be a string");
        return value.GetString()!;
    }

    private static int Integer(JsonElement node, string name, int fallback, int maximum)
    {
        if (!node.TryGetProperty(name, out var value))
            return fallback;
        if (value.ValueKind != JsonValueKind.Number || !value.TryGetInt32(out var number) ||
            number < 1 || number > maximum)
            throw new InvalidDataException($"'{name}' must be an integer between 1 and {maximum}");
        return number;
    }
}
