IF OBJECT_ID('batch.ProcessConfiguration', 'U') IS NULL
BEGIN
    CREATE TABLE batch.ProcessConfiguration (
        ConfigurationKey nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY
            CHECK (ConfigurationKey LIKE '[A-Za-z0-9]%' AND ConfigurationKey NOT LIKE '%[^-A-Za-z0-9_.]%'
                AND DATALENGTH(ConfigurationKey) = 2 * LEN(ConfigurationKey)),
        Handler nvarchar(128) NOT NULL
            CHECK (LEN(Handler) > 0 AND DATALENGTH(Handler) = DATALENGTH(TRIM(Handler))),
        ChildrenMode varchar(8) COLLATE Latin1_General_100_BIN2 NOT NULL
            CHECK (ChildrenMode IN ('serial', 'parallel') AND DATALENGTH(ChildrenMode) = LEN(ChildrenMode)),
        OptionsJson nvarchar(max) NOT NULL CHECK (ISJSON(OptionsJson, OBJECT) = 1 AND DATALENGTH(OptionsJson) <= 131072),
        MaxParallelTasks int NOT NULL CHECK (MaxParallelTasks BETWEEN 1 AND 256),
        MaxAttempts int NOT NULL CHECK (MaxAttempts BETWEEN 1 AND 20),
        TimeoutSeconds int NOT NULL CHECK (TimeoutSeconds BETWEEN 1 AND 86400),
        Revision bigint NOT NULL CHECK (Revision > 0),
        UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

CREATE OR ALTER PROCEDURE batch.SaveProcessConfiguration
    @ConfigurationKey nvarchar(256), @Handler nvarchar(512), @ExpectedRevision bigint,
    @MaxParallelTasks int = 1, @MaxAttempts int = 3, @TimeoutSeconds int = 60,
    @ChildrenMode varchar(32) = 'serial', @OptionsJson nvarchar(max) = N'{}'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @ConfigurationKey IS NULL OR DATALENGTH(@ConfigurationKey) > 128 OR
       @ConfigurationKey COLLATE Latin1_General_100_BIN2 NOT LIKE '[A-Za-z0-9]%' OR
       @ConfigurationKey COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_.]%' OR
       DATALENGTH(@ConfigurationKey) <> 2 * LEN(@ConfigurationKey)
        THROW 51015, 'Process configuration key must start with an alphanumeric and use at most 64 letters, digits, dots, underscores or hyphens.', 1;
    IF @Handler IS NULL OR DATALENGTH(@Handler) > 256 OR @ChildrenMode IS NULL OR DATALENGTH(@ChildrenMode) > 8
        THROW 51016, 'Invalid handler or children mode for process configuration.', 1;

    BEGIN TRANSACTION;

    DECLARE @Revision bigint;
    SELECT @Revision = Revision FROM batch.ProcessConfiguration WITH (UPDLOCK, HOLDLOCK)
    WHERE ConfigurationKey = @ConfigurationKey;
    IF @ExpectedRevision IS NULL OR @ExpectedRevision <> COALESCE(@Revision, 0)
        THROW 51013, 'Process configuration revision changed. Reload before saving.', 1;

    IF @Revision IS NULL
    BEGIN
        SET @Revision = 1;
        INSERT batch.ProcessConfiguration (ConfigurationKey, Handler, ChildrenMode, OptionsJson,
            MaxParallelTasks, MaxAttempts, TimeoutSeconds, Revision)
        VALUES (@ConfigurationKey, @Handler, @ChildrenMode, @OptionsJson,
            @MaxParallelTasks, @MaxAttempts, @TimeoutSeconds, @Revision);
    END
    ELSE
    BEGIN
        SET @Revision += 1;
        UPDATE batch.ProcessConfiguration SET Handler = @Handler, ChildrenMode = @ChildrenMode,
            OptionsJson = @OptionsJson, MaxParallelTasks = @MaxParallelTasks, MaxAttempts = @MaxAttempts,
            TimeoutSeconds = @TimeoutSeconds, Revision = @Revision, UpdatedAt = SYSUTCDATETIME()
        WHERE ConfigurationKey = @ConfigurationKey;
    END;

    COMMIT TRANSACTION;
    SELECT @Revision;
END;
GO
