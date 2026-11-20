using System.Reflection;
using System.Runtime.Loader;
using BatchEngine.Contracts;

namespace BatchEngine;

internal sealed class PluginRegistry
{
    private readonly Dictionary<(string Name, Type Contract), Type> handlers = new();

    internal PluginRegistry()
    {
        Register(typeof(PluginRegistry).Assembly);
        var paths = Environment.GetEnvironmentVariable("BATCH_PLUGIN_PATHS") ?? "";
        foreach (var path in paths.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var fullPath = Path.GetFullPath(path);
            Register(new PluginLoadContext(fullPath).LoadFromAssemblyPath(fullPath));
        }
    }

    internal bool Contains<T>(string name) => handlers.ContainsKey((name, typeof(T)));

    internal T Create<T>(string name) => handlers.TryGetValue((name, typeof(T)), out var type)
        ? (T)Activator.CreateInstance(type)!
        : throw new PermanentProcessException($"Handler '{name}' is unavailable");

    private void Register(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (type.GetCustomAttribute<ProcessHandlerAttribute>() is { } process)
                Register(typeof(IBatchProcess), process.Name, type);
            if (type.GetCustomAttribute<GroupHandlerAttribute>() is { } group)
                Register(typeof(IBatchGroup), group.Name, type);
        }
    }

    private void Register(Type contract, string name, Type type)
    {
        Protocol.Key(name, "Handler name", 128);
        if (!type.IsClass || type.IsAbstract || !contract.IsAssignableFrom(type) ||
            type.GetConstructor(Type.EmptyTypes) is null)
            throw new InvalidOperationException($"{type.FullName} must be a concrete {contract.Name} with a public parameterless constructor");
        if (!handlers.TryAdd((name, contract), type))
            throw new InvalidOperationException($"Handler '{name}' is registered twice for {contract.Name}");
    }

    private sealed class PluginLoadContext(string mainPath) : AssemblyLoadContext
    {
        private readonly AssemblyDependencyResolver resolver = new(mainPath);

        protected override Assembly? Load(AssemblyName name)
        {
            if (name.Name == typeof(IBatchProcess).Assembly.GetName().Name)
                return typeof(IBatchProcess).Assembly;
            // Share the platform-specific SqlClient already selected by the host when it meets the plugin's version.
            if (name.Name == "Microsoft.Data.SqlClient")
            {
                var sqlClient = typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly;
                if (name.Version is null || sqlClient.GetName().Version >= name.Version)
                    return sqlClient;
            }

            var path = resolver.ResolveAssemblyToPath(name);
            if (path is not null)
                return LoadFromAssemblyPath(path);
            var sibling = Path.Combine(Path.GetDirectoryName(mainPath)!, name.Name + ".dll");
            return File.Exists(sibling) ? LoadFromAssemblyPath(sibling) : null;
        }

        protected override nint LoadUnmanagedDll(string name)
        {
            var path = resolver.ResolveUnmanagedDllToPath(name);
            return path is null ? 0 : LoadUnmanagedDllFromPath(path);
        }
    }
}
