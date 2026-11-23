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

IF COL_LENGTH('batch.ProcessConfiguration', 'TaskExecutionMode') IS NULL
BEGIN
    ALTER TABLE batch.ProcessConfiguration ADD
        TaskExecutionMode varchar(8) COLLATE Latin1_General_100_BIN2 NOT NULL DEFAULT 'parallel'
            CHECK (TaskExecutionMode IN ('serial', 'parallel') AND DATALENGTH(TaskExecutionMode) = LEN(TaskExecutionMode)),
        TaskTimeoutEnabled bit NOT NULL DEFAULT 1,
        TaskTimeoutSeconds int NULL CHECK (TaskTimeoutSeconds BETWEEN 1 AND 86400),
        ProcessTimeoutEnabled bit NOT NULL DEFAULT 0,
        ProcessTimeoutSeconds int NOT NULL DEFAULT 3600 CHECK (ProcessTimeoutSeconds BETWEEN 1 AND 86400),
        AllowSameProcessAcrossRuns bit NOT NULL DEFAULT 1,
        AllowOtherProcessesAcrossRuns bit NOT NULL DEFAULT 1;
END;
GO

CREATE OR ALTER PROCEDURE batch.SaveProcessConfiguration
    @ConfigurationKey nvarchar(256), @Handler nvarchar(512), @ExpectedRevision bigint,
    @MaxParallelTasks int = 1, @MaxAttempts int = 3, @TimeoutSeconds int = 60,
    @ChildrenMode varchar(32) = 'serial', @OptionsJson nvarchar(max) = N'{}',
    @TaskExecutionMode varchar(32) = NULL, @TaskTimeoutEnabled bit = NULL, @TaskTimeoutSeconds int = NULL,
    @ProcessTimeoutEnabled bit = NULL, @ProcessTimeoutSeconds int = NULL,
    @AllowSameProcessAcrossRuns bit = NULL, @AllowOtherProcessesAcrossRuns bit = NULL
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

    IF DATALENGTH(@TaskExecutionMode) > 8
        THROW 51016, 'Task execution mode must be serial or parallel.', 1;

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
            MaxParallelTasks, MaxAttempts, TimeoutSeconds, Revision, TaskExecutionMode, TaskTimeoutEnabled,
            TaskTimeoutSeconds, ProcessTimeoutEnabled, ProcessTimeoutSeconds, AllowSameProcessAcrossRuns, AllowOtherProcessesAcrossRuns)
        VALUES (@ConfigurationKey, @Handler, @ChildrenMode, @OptionsJson,
            @MaxParallelTasks, @MaxAttempts, @TimeoutSeconds, @Revision,
            COALESCE(@TaskExecutionMode, 'parallel'), COALESCE(@TaskTimeoutEnabled, 1), @TaskTimeoutSeconds,
            COALESCE(@ProcessTimeoutEnabled, 0), COALESCE(@ProcessTimeoutSeconds, 3600),
            COALESCE(@AllowSameProcessAcrossRuns, 1), COALESCE(@AllowOtherProcessesAcrossRuns, 1));
    END
    ELSE
    BEGIN
        SET @Revision += 1;
        UPDATE batch.ProcessConfiguration SET Handler = @Handler, ChildrenMode = @ChildrenMode,
            OptionsJson = @OptionsJson, MaxParallelTasks = @MaxParallelTasks, MaxAttempts = @MaxAttempts,
            TimeoutSeconds = @TimeoutSeconds, Revision = @Revision, UpdatedAt = SYSUTCDATETIME(),
            TaskExecutionMode = COALESCE(@TaskExecutionMode, TaskExecutionMode),
            TaskTimeoutEnabled = COALESCE(@TaskTimeoutEnabled, TaskTimeoutEnabled),
            TaskTimeoutSeconds = COALESCE(@TaskTimeoutSeconds, TaskTimeoutSeconds),
            ProcessTimeoutEnabled = COALESCE(@ProcessTimeoutEnabled, ProcessTimeoutEnabled),
            ProcessTimeoutSeconds = COALESCE(@ProcessTimeoutSeconds, ProcessTimeoutSeconds),
            AllowSameProcessAcrossRuns = COALESCE(@AllowSameProcessAcrossRuns, AllowSameProcessAcrossRuns),
            AllowOtherProcessesAcrossRuns = COALESCE(@AllowOtherProcessesAcrossRuns, AllowOtherProcessesAcrossRuns)
        WHERE ConfigurationKey = @ConfigurationKey;
    END;

    COMMIT TRANSACTION;
    SELECT @Revision;
END;
GO
