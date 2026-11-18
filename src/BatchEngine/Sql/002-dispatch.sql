CREATE OR ALTER PROCEDURE batch.Claim
    @Limit int, @WorkerId nvarchar(128), @LeaseSeconds int
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RunId uniqueidentifier, @Resource nvarchar(255), @LockResult int, @Payload nvarchar(max);
    DECLARE @Locked table(RunId uniqueidentifier PRIMARY KEY);
    DECLARE @Candidates table(Id bigint PRIMARY KEY);
    DECLARE @Claimed table(Id bigint PRIMARY KEY, NodeId uniqueidentifier, Kind varchar(8));

    -- Look for ready work before taking any locks. Re-read after taking each run lock.
    DECLARE runs CURSOR LOCAL FAST_FORWARD FOR
        SELECT TOP (@Limit * 2) s.Id FROM batch.Submission s WITH (READPAST, READCOMMITTEDLOCK)
        WHERE s.Status = 'Running' AND EXISTS (
            SELECT 1 FROM batch.Node n WITH (READPAST, READCOMMITTEDLOCK)
            WHERE n.RunId = s.Id AND n.Phase IN ('Start', 'Fetch', 'Task', 'Finish')
                AND n.RunningTasks < n.MaxParallelTasks AND EXISTS (
                    SELECT 1 FROM batch.Job j WITH (READPAST, READCOMMITTEDLOCK)
                    WHERE j.NodeId = n.Id AND j.Kind = n.Phase AND j.Status = 'Ready'
                        AND j.AvailableAt <= SYSUTCDATETIME()
                )
        )
        ORDER BY s.SubmittedAt, s.Id;

    BEGIN TRANSACTION;
    OPEN runs;
    FETCH NEXT FROM runs INTO @RunId;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Resource = CONCAT(N'batch.run.', CONVERT(nvarchar(36), @RunId));
        EXEC @LockResult = sys.sp_getapplock
            @Resource = @Resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 0;
        IF @LockResult >= 0 INSERT @Locked VALUES (@RunId);
        FETCH NEXT FROM runs INTO @RunId;
    END;
    CLOSE runs;
    DEALLOCATE runs;

    IF EXISTS (SELECT 1 FROM @Locked)
    BEGIN
        INSERT @Candidates (Id)
        SELECT TOP (@Limit) q.Id
        FROM @Locked l JOIN batch.Node n ON n.RunId = l.RunId
        CROSS APPLY (
            SELECT TOP (CASE WHEN n.MaxParallelTasks - n.RunningTasks < @Limit
                THEN n.MaxParallelTasks - n.RunningTasks ELSE @Limit END)
                j.Id, j.AvailableAt, ROW_NUMBER() OVER (ORDER BY j.AvailableAt, j.Id) AS QueuePosition
            FROM batch.Job j
            WHERE j.NodeId = n.Id AND j.Kind = n.Phase AND j.Status = 'Ready'
                AND j.AvailableAt <= SYSUTCDATETIME()
            ORDER BY j.AvailableAt, j.Id
        ) q
        WHERE n.Phase IN ('Start', 'Fetch', 'Task', 'Finish')
        ORDER BY q.QueuePosition, q.AvailableAt, q.Id;

        UPDATE j SET Status = 'Running', Attempts = Attempts + 1, WorkerId = @WorkerId,
            LeaseToken = NEWID(), LeaseUntil = DATEADD(second, @LeaseSeconds, SYSUTCDATETIME()),
            StartedAt = COALESCE(StartedAt, SYSUTCDATETIME())
        OUTPUT inserted.Id, inserted.NodeId, inserted.Kind INTO @Claimed
        FROM batch.Job j JOIN @Candidates c ON j.Id = c.Id;

        UPDATE n SET RunningTasks = RunningTasks + c.Amount
        FROM batch.Node n JOIN (
            SELECT NodeId, COUNT(*) AS Amount FROM @Claimed WHERE Kind = 'Task' GROUP BY NodeId
        ) c ON n.Id = c.NodeId;
    END;

    SET @Payload = (
        SELECT j.Id, n.RunId, n.NodeKey AS ProcessKey, j.Kind, n.Handler, n.OptionsJson,
            CASE WHEN j.Kind <> 'Task' THEN s.InputJson END AS InputJson,
            j.RecordKey, j.PayloadJson, j.CursorValue AS [Cursor], j.Attempts, j.LeaseToken, n.TimeoutSeconds,
            n.OwnStatus, n.TotalTasks, n.SucceededTasks, n.InvalidTasks, n.FailedTasks,
            CASE WHEN n.TasksReleased = 1 THEN CONVERT(bigint, 0) ELSE n.TotalTasks END AS SkippedTasks,
            n.Error AS ProcessError
        FROM @Claimed c JOIN batch.Job j ON c.Id = j.Id
        JOIN batch.Node n ON j.NodeId = n.Id JOIN batch.Submission s ON n.RunId = s.Id
        FOR JSON PATH, INCLUDE_NULL_VALUES
    );
    COMMIT TRANSACTION;
    SELECT COALESCE(@Payload, N'[]');
END;
GO

CREATE OR ALTER PROCEDURE batch.Renew @Claims nvarchar(max), @LeaseSeconds int
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE j SET LeaseUntil = DATEADD(second, @LeaseSeconds, SYSUTCDATETIME())
    OUTPUT inserted.Id
    FROM batch.Job j JOIN OPENJSON(@Claims) WITH (Id bigint, Token uniqueidentifier) c ON j.Id = c.Id
    WHERE j.LeaseToken = c.Token AND j.Status = 'Running' AND j.LeaseUntil > SYSUTCDATETIME();
END;
GO

CREATE OR ALTER FUNCTION batch.BackoffSeconds(@Attempt int) RETURNS int
AS
BEGIN
    RETURN CONVERT(int, POWER(2, CASE WHEN @Attempt < 7 THEN @Attempt - 1 ELSE 6 END));
END;
GO

CREATE OR ALTER PROCEDURE batch.BeginFinish
    @NodeId uniqueidentifier, @Status varchar(8), @Error nvarchar(2000)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE batch.Node SET Phase = 'Finish', OwnStatus = @Status, Error = @Error WHERE Id = @NodeId;
    INSERT batch.Job (NodeId, Kind) VALUES (@NodeId, 'Finish');
END;
GO

CREATE OR ALTER PROCEDURE batch.SettleStage
    @Id bigint, @Token uniqueidentifier, @Status varchar(8), @Retryable bit, @Error nvarchar(2000),
    @Records nvarchar(max), @NextCursor nvarchar(600), @Expired bit
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NodeId uniqueidentifier, @Kind varchar(8), @Attempts int, @MaxAttempts int, @Inserted int;
    SELECT @NodeId = j.NodeId, @Kind = j.Kind, @Attempts = j.Attempts, @MaxAttempts = n.MaxAttempts
    FROM batch.Job j WITH (UPDLOCK) JOIN batch.Node n ON n.Id = j.NodeId
    WHERE j.Id = @Id AND j.LeaseToken = @Token AND j.Status = 'Running'
        AND ((@Expired = 1 AND j.LeaseUntil <= SYSUTCDATETIME()) OR
             (@Expired = 0 AND j.LeaseUntil > SYSUTCDATETIME()));
    IF @NodeId IS NULL RETURN;

    IF @Kind = 'Fetch' AND @Status = 'Success' AND @NextCursor IS NOT NULL
    BEGIN
        DECLARE @Hash binary(32) = HASHBYTES('SHA2_256', @NextCursor);
        IF EXISTS (SELECT 1 FROM batch.FetchCursor WHERE NodeId = @NodeId AND CursorHash = @Hash)
        BEGIN
            SET @Status = 'Failed';
            SET @Retryable = 0;
            SET @Error = N'FetchRecordsAsync repeated a cursor';
        END
        ELSE INSERT batch.FetchCursor VALUES (@NodeId, @Hash);
    END;

    IF @Status = 'Failed' AND @Retryable = 1 AND @Attempts < @MaxAttempts
    BEGIN
        UPDATE batch.Job SET Status = 'Ready', LeaseToken = NULL, LeaseUntil = NULL,
            Error = @Error, AvailableAt = DATEADD(second, batch.BackoffSeconds(@Attempts), SYSUTCDATETIME())
        WHERE Id = @Id;
        RETURN;
    END;

    IF @Kind = 'Fetch' AND @Status = 'Success'
    BEGIN
        ;WITH PageRecords AS (
            SELECT r.RecordKey, r.PayloadJson, ROW_NUMBER() OVER (
                PARTITION BY r.RecordKey COLLATE Latin1_General_100_BIN2 ORDER BY CONVERT(int, entries.[key])
            ) AS RowNumber
            FROM OPENJSON(@Records) entries
            CROSS APPLY OPENJSON(entries.value) WITH (RecordKey nvarchar(300) '$.Key', PayloadJson nvarchar(max)) r
        )
        INSERT batch.Job(NodeId, Kind, RecordKey, PayloadJson)
        SELECT @NodeId, 'Task', r.RecordKey, r.PayloadJson
        FROM PageRecords r WHERE r.RowNumber = 1 AND NOT EXISTS (
            SELECT 1 FROM batch.Job j
            WHERE j.NodeId = @NodeId AND j.Kind = 'Task' AND j.RecordKey = r.RecordKey COLLATE Latin1_General_100_BIN2
        );
        SET @Inserted = @@ROWCOUNT;
        UPDATE batch.Node SET TotalTasks = TotalTasks + @Inserted WHERE Id = @NodeId;

        IF @NextCursor IS NOT NULL
        BEGIN
            UPDATE batch.Job SET Status = 'Ready', LeaseToken = NULL, LeaseUntil = NULL,
                CursorValue = @NextCursor, Attempts = 0, Error = NULL, AvailableAt = SYSUTCDATETIME()
            WHERE Id = @Id;
            RETURN;
        END;
    END;

    UPDATE batch.Job SET Status = @Status, LeaseToken = NULL, LeaseUntil = NULL,
        Error = @Error, FinishedAt = SYSUTCDATETIME() WHERE Id = @Id;

    IF @Kind = 'Start' AND @Status = 'Success'
    BEGIN
        UPDATE batch.Node SET Phase = 'Fetch' WHERE Id = @NodeId;
        INSERT batch.Job(NodeId, Kind) VALUES (@NodeId, 'Fetch');
    END
    ELSE IF @Kind = 'Fetch' AND @Status = 'Success'
    BEGIN
        UPDATE batch.Node SET Phase = 'Task', TasksReleased = 1 WHERE Id = @NodeId;
        IF EXISTS (SELECT 1 FROM batch.Node WHERE Id = @NodeId AND TotalTasks = 0)
            EXEC batch.BeginFinish @NodeId, 'Success', NULL;
    END
    ELSE IF @Kind = 'Finish'
    BEGIN
        IF @Status <> 'Success'
            UPDATE batch.Node SET OwnStatus = batch.MergeStatus(OwnStatus, @Status),
                Error = CONCAT_WS(N' / ', Error, @Error) WHERE Id = @NodeId;
        EXEC batch.MoveTree @NodeId, 'O';
    END
    ELSE EXEC batch.BeginFinish @NodeId, @Status, @Error;
END;
GO

CREATE OR ALTER PROCEDURE batch.Settle @Results nvarchar(max), @Expired bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @Result table (
        Id bigint PRIMARY KEY, Token uniqueidentifier, Status varchar(8), Retryable bit,
        OutputJson nvarchar(max), Error nvarchar(2000), Records nvarchar(max), NextCursor nvarchar(600)
    );
    INSERT @Result
    SELECT Id, Token, Status, Retryable, OutputJson, Error, Records, NextCursor FROM OPENJSON(@Results)
        WITH (Id bigint, Token uniqueidentifier, Status varchar(8), Retryable bit,
            OutputJson nvarchar(max), Error nvarchar(2000), Records nvarchar(max) AS JSON, NextCursor nvarchar(600));
    DECLARE @RunId uniqueidentifier, @Resource nvarchar(255), @LockResult int;
    DECLARE @Changed table(NodeId uniqueidentifier, Status varchar(8));
    DECLARE @Done table(NodeId uniqueidentifier PRIMARY KEY, Status varchar(8));

    BEGIN TRANSACTION;
    DECLARE runs CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT n.RunId
        FROM @Result r JOIN batch.Job j ON j.Id = r.Id AND j.LeaseToken = r.Token
        JOIN batch.Node n ON n.Id = j.NodeId ORDER BY n.RunId;
    OPEN runs;
    FETCH NEXT FROM runs INTO @RunId;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Resource = CONCAT(N'batch.run.', CONVERT(nvarchar(36), @RunId));
        EXEC @LockResult = sys.sp_getapplock
            @Resource = @Resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 2000;
        IF @LockResult < 0 THROW 51011, 'Could not lock the batch run for settlement', 1;
        FETCH NEXT FROM runs INTO @RunId;
    END;
    CLOSE runs;
    DEALLOCATE runs;

    UPDATE j SET Status = CASE WHEN retry.ShouldRetry = 1 THEN 'Ready' ELSE r.Status END,
        AvailableAt = CASE WHEN retry.ShouldRetry = 1
            THEN DATEADD(second, batch.BackoffSeconds(j.Attempts), SYSUTCDATETIME()) ELSE j.AvailableAt END,
        FinishedAt = CASE WHEN retry.ShouldRetry = 0 THEN SYSUTCDATETIME() END,
        LeaseToken = NULL, LeaseUntil = NULL, OutputJson = r.OutputJson, Error = r.Error
    OUTPUT inserted.NodeId, inserted.Status INTO @Changed
    FROM @Result r JOIN batch.Job j ON j.Id = r.Id
    JOIN batch.Node n ON n.Id = j.NodeId
    CROSS APPLY (VALUES (CASE WHEN r.Status = 'Failed' AND r.Retryable = 1 AND j.Attempts < n.MaxAttempts
        THEN 1 ELSE 0 END)) retry(ShouldRetry)
    WHERE j.Kind = 'Task' AND j.Status = 'Running' AND j.LeaseToken = r.Token AND (
        (@Expired = 1 AND j.LeaseUntil <= SYSUTCDATETIME()) OR
        (@Expired = 0 AND j.LeaseUntil > SYSUTCDATETIME()));

    UPDATE n SET RunningTasks = n.RunningTasks - c.Amount,
        SucceededTasks = n.SucceededTasks + c.Successes,
        InvalidTasks = n.InvalidTasks + c.Invalids,
        FailedTasks = n.FailedTasks + c.Failures
    FROM batch.Node n JOIN (
        SELECT NodeId, COUNT(*) AS Amount,
            SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) AS Successes,
            SUM(CASE WHEN Status = 'Invalid' THEN 1 ELSE 0 END) AS Invalids,
            SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS Failures
        FROM @Changed GROUP BY NodeId
    ) c ON c.NodeId = n.Id;

    INSERT @Done
    SELECT n.Id, CASE WHEN n.FailedTasks > 0 THEN 'Failed' WHEN n.InvalidTasks > 0 THEN 'Invalid' ELSE 'Success' END
    FROM batch.Node n JOIN (SELECT DISTINCT NodeId FROM @Changed) c ON c.NodeId = n.Id
    WHERE n.Phase = 'Task' AND n.TotalTasks = n.SucceededTasks + n.InvalidTasks + n.FailedTasks;

    DECLARE @NodeId uniqueidentifier, @Status varchar(8), @Error nvarchar(2000);
    DECLARE finished CURSOR LOCAL FAST_FORWARD FOR SELECT NodeId, Status FROM @Done;
    OPEN finished;
    FETCH NEXT FROM finished INTO @NodeId, @Status;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Error = CASE @Status WHEN 'Failed' THEN N'One or more tasks failed'
                                   WHEN 'Invalid' THEN N'One or more tasks were invalid' END;
        EXEC batch.BeginFinish @NodeId, @Status, @Error;
        FETCH NEXT FROM finished INTO @NodeId, @Status;
    END;
    CLOSE finished;
    DEALLOCATE finished;

    DECLARE @JobId bigint, @Token uniqueidentifier, @Retryable bit, @Records nvarchar(max), @Cursor nvarchar(600);
    DECLARE stages CURSOR LOCAL FAST_FORWARD FOR
        SELECT r.Id, r.Token, r.Status, r.Retryable, r.Error, r.Records, r.NextCursor
        FROM @Result r JOIN batch.Job j ON j.Id = r.Id WHERE j.Kind <> 'Task';
    OPEN stages;
    FETCH NEXT FROM stages INTO @JobId, @Token, @Status, @Retryable, @Error, @Records, @Cursor;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC batch.SettleStage @JobId, @Token, @Status, @Retryable, @Error, @Records, @Cursor, @Expired;
        FETCH NEXT FROM stages INTO @JobId, @Token, @Status, @Retryable, @Error, @Records, @Cursor;
    END;
    CLOSE stages;
    DEALLOCATE stages;
    COMMIT TRANSACTION;
END;
GO

CREATE OR ALTER PROCEDURE batch.Reap @Limit int
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Expired nvarchar(max) = (
        SELECT TOP (@Limit) Id, LeaseToken AS Token,
            'Failed' AS Status, CONVERT(bit, 1) AS Retryable, N'Worker lease expired' AS Error
        FROM batch.Job WITH (READPAST, READCOMMITTEDLOCK)
        WHERE Status = 'Running' AND LeaseUntil <= SYSUTCDATETIME()
        ORDER BY LeaseUntil, Id FOR JSON PATH
    );
    IF @Expired IS NOT NULL AND @Expired <> N'[]' EXEC batch.Settle @Expired, 1;
END;
GO
