using System.Text.Json;
using BatchEngine.Contracts;

namespace FixturePlugin;

[GroupHandler("integration.group.v1")]
public sealed class GroupEvents : IBatchGroup
{
    public async ValueTask BeforeGroupAsync(GroupContext context, CancellationToken ct)
    {
        if (Enabled(context, "failStart"))
            throw new PermanentProcessException("Sample group start failure");
        await context.Shared.PutAsync("group.start." + context.GroupKey,
            JsonSerializer.SerializeToElement(context.IdempotencyKey), cancellationToken: ct);
    }

    public async ValueTask AfterGroupAsync(GroupContext context, GroupOutcome outcome, CancellationToken ct)
    {
        if (Enabled(context, "retryFinish") && context.Attempt == 1)
            throw new IOException("Sample transient group finish failure");
        await context.Shared.PutAsync("group.finish." + context.GroupKey,
            JsonSerializer.SerializeToElement(outcome.Status.ToString()), cancellationToken: ct);
    }

    private static bool Enabled(GroupContext context, string key) =>
        context.Options.TryGetProperty(key, out var value) && value.GetBoolean();
}
