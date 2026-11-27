FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props ./
COPY src/BatchEngine.Contracts/BatchEngine.Contracts.csproj src/BatchEngine.Contracts/
COPY src/BatchEngine/BatchEngine.csproj src/BatchEngine/
RUN dotnet restore src/BatchEngine/BatchEngine.csproj
COPY src/ src/
RUN dotnet publish src/BatchEngine/BatchEngine.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build --chown=$APP_UID /app/ .
COPY --chown=$APP_UID examples/ examples/
USER $APP_UID
ENTRYPOINT ["dotnet", "BatchEngine.dll"]
CMD ["worker"]
