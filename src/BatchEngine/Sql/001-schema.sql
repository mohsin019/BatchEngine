IF SCHEMA_ID('batch') IS NULL EXEC('CREATE SCHEMA batch');
GO

IF OBJECT_ID('batch.Submission', 'U') IS NULL
BEGIN
    CREATE TABLE batch.Submission (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_BatchSubmission PRIMARY KEY DEFAULT NEWSEQUENTIALID(),
        SubmissionKey nvarchar(200) COLLATE Latin1_General_100_BIN2 NULL,
        DefinitionJson nvarchar(max) NOT NULL CHECK (ISJSON(DefinitionJson, OBJECT) = 1),
        InputJson nvarchar(max) NOT NULL DEFAULT N'{}' CHECK (ISJSON(InputJson, OBJECT) = 1),
        Status varchar(12) NOT NULL DEFAULT 'Queued'
            CHECK (Status IN ('Queued', 'Running', 'Success', 'Invalid', 'Failed')),
        Error nvarchar(4000) NULL,
        SubmittedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        StartedAt datetime2(3) NULL,
        FinishedAt datetime2(3) NULL
    );
    CREATE UNIQUE INDEX UX_BatchSubmission_Key ON batch.Submission(SubmissionKey) WHERE SubmissionKey IS NOT NULL;
    CREATE INDEX IX_BatchSubmission_Queued ON batch.Submission(SubmittedAt, Id) WHERE Status = 'Queued';
    CREATE INDEX IX_BatchSubmission_Running ON batch.Submission(SubmittedAt, Id) WHERE Status = 'Running';
END;
GO

IF OBJECT_ID('batch.Node', 'U') IS NULL
BEGIN
    CREATE TABLE batch.Node (
        Id uniqueidentifier NOT NULL CONSTRAINT PK_BatchNode PRIMARY KEY,
        RunId uniqueidentifier NOT NULL REFERENCES batch.Submission(Id),
        ParentId uniqueidentifier NULL REFERENCES batch.Node(Id),
        Position int NOT NULL,
        NodeKey nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
        Kind varchar(8) NOT NULL CHECK (Kind IN ('Group', 'Process')),
        ChildrenMode varchar(8) NOT NULL CHECK (ChildrenMode IN ('serial', 'parallel')),
        Handler nvarchar(128) COLLATE Latin1_General_100_BIN2 NULL,
        OptionsJson nvarchar(max) NOT NULL,
        MaxParallelTasks int NOT NULL CHECK (MaxParallelTasks BETWEEN 1 AND 256),
        MaxAttempts int NOT NULL CHECK (MaxAttempts BETWEEN 1 AND 20),
        TimeoutSeconds int NOT NULL CHECK (TimeoutSeconds BETWEEN 1 AND 86400),
        RemainingChildren int NOT NULL CHECK (RemainingChildren >= 0),
        ChildrenStatus varchar(8) NOT NULL DEFAULT 'Success',
        Phase varchar(12) NOT NULL DEFAULT 'Waiting'
            CHECK (Phase IN ('Waiting', 'Start', 'Fetch', 'Task', 'Finish', 'Children', 'Done')),
        Status varchar(12) NOT NULL DEFAULT 'Pending'
            CHECK (Status IN ('Pending', 'Running', 'Success', 'Invalid', 'Failed', 'Skipped')),
        OwnStatus varchar(8) NULL,
        TasksReleased bit NOT NULL DEFAULT 0,
        TotalTasks bigint NOT NULL DEFAULT 0,
        RunningTasks int NOT NULL DEFAULT 0,
        SucceededTasks bigint NOT NULL DEFAULT 0,
        InvalidTasks bigint NOT NULL DEFAULT 0,
        FailedTasks bigint NOT NULL DEFAULT 0,
        StartedAt datetime2(3) NULL,
        FinishedAt datetime2(3) NULL,
        Error nvarchar(4000) NULL,
        UNIQUE (RunId, NodeKey),
        CHECK (RunningTasks BETWEEN 0 AND MaxParallelTasks),
        CHECK (SucceededTasks + InvalidTasks + FailedTasks + RunningTasks <= TotalTasks),
        CHECK ((Kind = 'Group' AND Handler IS NULL) OR (Kind = 'Process' AND Handler IS NOT NULL))
    );
    CREATE INDEX IX_BatchNode_Children ON batch.Node(ParentId, Position);
    CREATE INDEX IX_BatchNode_Dispatch ON batch.Node(RunId, Phase, Id)
        INCLUDE (RunningTasks, MaxParallelTasks) WHERE Phase IN ('Start', 'Fetch', 'Task', 'Finish');
END;
GO

IF COL_LENGTH('batch.Node', 'GroupHandler') IS NULL
    ALTER TABLE batch.Node ADD GroupHandler nvarchar(128) COLLATE Latin1_General_100_BIN2 NULL,
        CONSTRAINT CK_BatchNode_GroupHandler CHECK (GroupHandler IS NULL OR Kind = 'Group');
GO

IF OBJECT_ID('batch.Job', 'U') IS NULL
BEGIN
    CREATE TABLE batch.Job (
        Id bigint IDENTITY NOT NULL CONSTRAINT PK_BatchJob PRIMARY KEY,
        NodeId uniqueidentifier NOT NULL REFERENCES batch.Node(Id),
        Kind varchar(8) NOT NULL CHECK (Kind IN ('Start', 'Fetch', 'Task', 'Finish')),
        RecordKey nvarchar(300) COLLATE Latin1_General_100_BIN2 NULL,
        PayloadJson nvarchar(max) NULL,
        CursorValue nvarchar(600) COLLATE Latin1_General_100_BIN2 NULL,
        Status varchar(8) NOT NULL DEFAULT 'Ready'
            CHECK (Status IN ('Ready', 'Running', 'Success', 'Invalid', 'Failed')),
        Attempts int NOT NULL DEFAULT 0,
        WorkerId nvarchar(128) NULL,
        LeaseToken uniqueidentifier NULL,
        LeaseUntil datetime2(3) NULL,
        AvailableAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        StartedAt datetime2(3) NULL,
        FinishedAt datetime2(3) NULL,
        OutputJson nvarchar(max) NULL,
        Error nvarchar(2000) NULL,
        CHECK ((Kind = 'Task' AND RecordKey IS NOT NULL) OR (Kind <> 'Task' AND RecordKey IS NULL)),
        CHECK ((Status = 'Running' AND LeaseToken IS NOT NULL AND LeaseUntil IS NOT NULL)
            OR (Status <> 'Running' AND LeaseToken IS NULL AND LeaseUntil IS NULL))
    );
    CREATE UNIQUE INDEX UX_BatchJob_Record ON batch.Job(NodeId, RecordKey) WHERE Kind = 'Task';
    CREATE UNIQUE INDEX UX_BatchJob_Stage ON batch.Job(NodeId, Kind) WHERE Kind <> 'Task';
    CREATE INDEX IX_BatchJob_Ready ON batch.Job(NodeId, Kind, AvailableAt, Id) WHERE Status = 'Ready';
    CREATE INDEX IX_BatchJob_Lease ON batch.Job(LeaseUntil, Id) INCLUDE (LeaseToken) WHERE Status = 'Running';
END;
GO

IF OBJECT_ID('batch.FetchCursor', 'U') IS NULL
BEGIN
    CREATE TABLE batch.FetchCursor (
        NodeId uniqueidentifier NOT NULL REFERENCES batch.Node(Id),
        CursorHash binary(32) NOT NULL,
        PRIMARY KEY (NodeId, CursorHash)
    );
END;
GO

IF OBJECT_ID('batch.SharedData', 'U') IS NULL
BEGIN
    CREATE TABLE batch.SharedData (
        RunId uniqueidentifier NOT NULL REFERENCES batch.Submission(Id),
        [Key] nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
        ValueJson nvarchar(max) NOT NULL CHECK (ISJSON(ValueJson, VALUE) = 1),
        Version bigint NOT NULL,
        UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        PRIMARY KEY (RunId, [Key])
    );
END;
GO

IF COL_LENGTH('batch.Job', 'RowCount') IS NULL
    ALTER TABLE batch.Job ADD [RowCount] int NOT NULL DEFAULT 1 CHECK ([RowCount] > 0);
GO

IF COL_LENGTH('batch.Node', 'TotalRecords') IS NULL
BEGIN
    ALTER TABLE batch.Node ADD TotalRecords bigint NOT NULL DEFAULT 0, RunningRecords bigint NOT NULL DEFAULT 0,
        SucceededRecords bigint NOT NULL DEFAULT 0, InvalidRecords bigint NOT NULL DEFAULT 0,
        FailedRecords bigint NOT NULL DEFAULT 0;

    -- Before result-set volumes, every task represented one record.
    EXEC(N'UPDATE batch.Node SET TotalRecords = TotalTasks, RunningRecords = RunningTasks,
        SucceededRecords = SucceededTasks, InvalidRecords = InvalidTasks, FailedRecords = FailedTasks
        WHERE TotalTasks > 0');
END;
GO

CREATE OR ALTER VIEW batch.TaskStatus AS
SELECT n.RunId, n.NodeKey AS ProcessKey, j.Id, j.RecordKey,
    CASE WHEN n.TasksReleased = 1 THEN j.Status
         WHEN n.OwnStatus IS NULL THEN 'Staged'
         ELSE 'Skipped' END AS Status,
    j.Attempts, j.WorkerId, j.OutputJson, j.Error, j.StartedAt, j.FinishedAt, j.[RowCount]
FROM batch.Node n JOIN batch.Job j ON j.NodeId = n.Id
WHERE j.Kind = 'Task';
GO

CREATE OR ALTER FUNCTION batch.MergeStatus(@A varchar(8), @B varchar(8))
RETURNS varchar(8)
AS
BEGIN
    RETURN CASE WHEN 'Failed' IN (@A, @B) THEN 'Failed'
                WHEN 'Invalid' IN (@A, @B) THEN 'Invalid'
                ELSE 'Success' END;
END;
GO

CREATE OR ALTER PROCEDURE batch.SkipWaitingChildren @NodeId uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    ;WITH Descendants AS (
        SELECT Id FROM batch.Node WHERE ParentId = @NodeId AND Phase = 'Waiting'
        UNION ALL
        SELECT n.Id FROM batch.Node n JOIN Descendants d ON n.ParentId = d.Id WHERE n.Phase = 'Waiting'
    )
    UPDATE n SET Status = 'Skipped', Phase = 'Done', FinishedAt = SYSUTCDATETIME(),
        Error = N'A prerequisite failed'
    FROM batch.Node n JOIN Descendants d ON n.Id = d.Id
    OPTION (MAXRECURSION 32);
    UPDATE batch.Node SET RemainingChildren = 0 WHERE Id = @NodeId;
END;
GO

-- All transitions for a running tree take its run-specific transaction lock.
CREATE OR ALTER PROCEDURE batch.MoveTree @NodeId uniqueidentifier, @Action char(1), @FinalOutcome varchar(8) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Work table (
        Sequence int IDENTITY PRIMARY KEY, NodeId uniqueidentifier NOT NULL,
        Action char(1) NOT NULL, Outcome varchar(8) NULL
    );
    INSERT @Work (NodeId, Action, Outcome) VALUES (@NodeId, @Action, @FinalOutcome);

    DECLARE @Sequence int, @ParentId uniqueidentifier, @RunId uniqueidentifier,
        @Kind varchar(8), @Mode varchar(8), @Own varchar(8), @Children varchar(8),
        @Outcome varchar(8), @Remaining int, @GroupHandler nvarchar(128), @Phase varchar(12);
    WHILE EXISTS (SELECT 1 FROM @Work)
    BEGIN
        SELECT TOP (1) @Sequence = Sequence, @NodeId = NodeId, @Action = Action, @Outcome = Outcome
        FROM @Work ORDER BY Sequence;
        DELETE @Work WHERE Sequence = @Sequence;

        IF @Action = 'S'
        BEGIN
            SELECT @Kind = Kind, @GroupHandler = GroupHandler FROM batch.Node WHERE Id = @NodeId;
            SET @Phase = CASE WHEN @Kind = 'Group' AND @GroupHandler IS NULL THEN 'Children' ELSE 'Start' END;
            UPDATE batch.Node SET Status = 'Running', StartedAt = SYSUTCDATETIME(),
                Phase = @Phase, OwnStatus = CASE WHEN @Phase = 'Children' THEN 'Success' END
            WHERE Id = @NodeId AND Phase = 'Waiting';
            IF @@ROWCOUNT <> 1 THROW 51010, 'Node is not waiting', 1;

            IF @Phase = 'Start' INSERT batch.Job (NodeId, Kind) VALUES (@NodeId, 'Start');
            ELSE INSERT @Work (NodeId, Action) VALUES (@NodeId, 'O');
        END
        ELSE IF @Action = 'O'
        BEGIN
            SELECT @Mode = ChildrenMode, @Own = OwnStatus, @Remaining = RemainingChildren
            FROM batch.Node WHERE Id = @NodeId;
            UPDATE batch.Node SET Phase = 'Children' WHERE Id = @NodeId;

            IF @Own = 'Failed' EXEC batch.SkipWaitingChildren @NodeId;
            IF @Own = 'Failed' OR @Remaining = 0
                INSERT @Work (NodeId, Action, Outcome) VALUES (@NodeId, 'C', @Own);
            ELSE IF @Mode = 'serial'
                INSERT @Work (NodeId, Action)
                SELECT TOP (1) Id, 'S' FROM batch.Node
                WHERE ParentId = @NodeId AND Phase = 'Waiting' ORDER BY Position;
            ELSE
                INSERT @Work (NodeId, Action)
                SELECT Id, 'S' FROM batch.Node WHERE ParentId = @NodeId AND Phase = 'Waiting';
        END
        ELSE
        BEGIN
            SELECT @GroupHandler = GroupHandler, @Phase = Phase FROM batch.Node WHERE Id = @NodeId;
            IF @GroupHandler IS NOT NULL AND @Phase <> 'Finish'
            BEGIN
                UPDATE batch.Node SET Phase = 'Finish' WHERE Id = @NodeId;
                INSERT batch.Job (NodeId, Kind) VALUES (@NodeId, 'Finish');
                CONTINUE;
            END;

            UPDATE batch.Node SET Status = @Outcome, Phase = 'Done', FinishedAt = SYSUTCDATETIME()
            WHERE Id = @NodeId;
            SELECT @ParentId = ParentId, @RunId = RunId FROM batch.Node WHERE Id = @NodeId;
            IF @ParentId IS NULL
                UPDATE batch.Submission SET Status = @Outcome, FinishedAt = SYSUTCDATETIME() WHERE Id = @RunId;
            ELSE
            BEGIN
                UPDATE batch.Node SET RemainingChildren = RemainingChildren - 1,
                    ChildrenStatus = batch.MergeStatus(ChildrenStatus, @Outcome) WHERE Id = @ParentId;
                SELECT @Mode = ChildrenMode, @Own = OwnStatus, @Children = ChildrenStatus,
                    @Remaining = RemainingChildren FROM batch.Node WHERE Id = @ParentId;
                IF @Mode = 'serial' AND @Outcome = 'Failed'
                BEGIN
                    EXEC batch.SkipWaitingChildren @ParentId;
                    INSERT @Work (NodeId, Action, Outcome) VALUES (@ParentId, 'C', 'Failed');
                END
                ELSE IF @Remaining = 0
                    INSERT @Work (NodeId, Action, Outcome)
                    VALUES (@ParentId, 'C', batch.MergeStatus(@Own, @Children));
                ELSE IF @Mode = 'serial'
                    INSERT @Work (NodeId, Action)
                    SELECT TOP (1) Id, 'S' FROM batch.Node
                    WHERE ParentId = @ParentId AND Phase = 'Waiting' ORDER BY Position;
            END;
        END;
    END;
END;
GO

CREATE OR ALTER PROCEDURE batch.Ingest @Intake nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Id uniqueidentifier, @Nodes nvarchar(max), @Error nvarchar(2000), @Root uniqueidentifier;
    DECLARE plans CURSOR LOCAL FAST_FORWARD FOR
        SELECT Id, Nodes, Error FROM OPENJSON(@Intake)
        WITH (Id uniqueidentifier, Nodes nvarchar(max) AS JSON, Error nvarchar(2000)) ORDER BY Id;
    OPEN plans;
    FETCH NEXT FROM plans INTO @Id, @Nodes, @Error;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRANSACTION;
        UPDATE batch.Submission SET Status = CASE WHEN @Error IS NULL THEN 'Running' ELSE 'Failed' END,
            StartedAt = CASE WHEN @Error IS NULL THEN SYSUTCDATETIME() END,
            FinishedAt = CASE WHEN @Error IS NOT NULL THEN SYSUTCDATETIME() END,
            Error = CASE WHEN @Error IS NOT NULL THEN CONCAT(N'Invalid workflow: ', @Error) END
        WHERE Id = @Id AND Status = 'Queued';
        IF @@ROWCOUNT = 1 AND @Error IS NULL
        BEGIN
            INSERT batch.Node (Id, RunId, ParentId, Position, NodeKey, Kind, ChildrenMode,
                Handler, OptionsJson, MaxParallelTasks, MaxAttempts, TimeoutSeconds, RemainingChildren, GroupHandler)
            SELECT Id, @Id, ParentId, Position, NodeKey, Kind, ChildrenMode, Handler,
                OptionsJson, MaxParallelTasks, MaxAttempts, TimeoutSeconds, ChildCount, GroupHandler
            FROM OPENJSON(@Nodes) WITH (Id uniqueidentifier, ParentId uniqueidentifier, Position int,
                NodeKey nvarchar(64), Kind varchar(8), ChildrenMode varchar(8), Handler nvarchar(128),
                OptionsJson nvarchar(max), MaxParallelTasks int, MaxAttempts int, TimeoutSeconds int, ChildCount int,
                GroupHandler nvarchar(128));
            SELECT @Root = Id FROM batch.Node WHERE RunId = @Id AND ParentId IS NULL;
            EXEC batch.MoveTree @Root, 'S';
        END;
        COMMIT TRANSACTION;
        FETCH NEXT FROM plans INTO @Id, @Nodes, @Error;
    END;
    CLOSE plans;
    DEALLOCATE plans;
END;
GO
