CREATE OR ALTER PROCEDURE batch.Claim
    @Limit int, @WorkerId nvarchar(128), @LeaseSeconds int
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RunId uniqueidentifier, @Resource nvarchar(255), @LockResult int, @Payload nvarchar(max), @NextProcess uniqueidentifier;
    DECLARE @Locked table(RunId uniqueidentifier PRIMARY KEY);
    DECLARE @Candidates table(Id bigint PRIMARY KEY);
    DECLARE @Claimed table(Id bigint PRIMARY KEY, NodeId uniqueidentifier, Kind varchar(8), [RowCount] int);

    DECLARE @EligibleAdmissions table(NodeId uniqueidentifier PRIMARY KEY);
    BEGIN TRANSACTION;

    -- Admission checks and the first claim are atomic across all workers.
    EXEC @LockResult = sys.sp_getapplock
        @Resource = N'batch.process-admission', @LockMode = 'Exclusive',
        @LockOwner = 'Transaction', @LockTimeout = 0;
    -- Already admitted processes can continue claiming work while another worker admits a new process.
    IF @LockResult >= 0
        INSERT @EligibleAdmissions
        SELECT n.Id FROM batch.Node n WITH (READPAST, READCOMMITTEDLOCK)
        WHERE n.Status = 'Running' AND n.Kind = 'Process' AND n.Phase IN ('Start','Fetch','Task','Finish')
            AND n.AdmittedAt IS NULL AND EXISTS (
                SELECT 1 FROM batch.Job j WITH (READPAST, READCOMMITTEDLOCK) WHERE j.NodeId = n.Id
                    AND j.Kind = n.Phase AND j.Status = 'Ready' AND j.AvailableAt <= SYSUTCDATETIME()
            )
            AND NOT EXISTS (
                SELECT 1 FROM batch.Node active
                WHERE active.AdmittedAt IS NOT NULL AND active.Phase IN ('Start', 'Fetch', 'Task', 'Finish')
                    AND active.RunId <> n.RunId
                    AND (active.AllowOtherProcessesAcrossRuns = 0 OR n.AllowOtherProcessesAcrossRuns = 0
                        OR (active.ConfigurationKey = n.ConfigurationKey
                            AND (active.AllowSameProcessAcrossRuns = 0 OR n.AllowSameProcessAcrossRuns = 0)))
            );

    -- A waiting process cannot displace an already admitted process that must finish first.
    DECLARE runs CURSOR LOCAL FAST_FORWARD FOR
        SELECT TOP (@Limit * 2) s.Id FROM batch.Submission s WITH (READPAST, READCOMMITTEDLOCK)
        WHERE s.Status = 'Running' AND EXISTS (
            SELECT 1 FROM batch.Node n WITH (READPAST, READCOMMITTEDLOCK)
            WHERE n.RunId = s.Id AND n.Phase IN ('Start', 'Fetch', 'Task', 'Finish')
                AND (n.Phase = 'Finish' OR n.ProcessDeadlineAt IS NULL OR n.ProcessDeadlineAt > SYSUTCDATETIME())
                AND (n.Kind = 'Group' OR n.AdmittedAt IS NOT NULL
                    OR EXISTS (SELECT 1 FROM @EligibleAdmissions e WHERE e.NodeId = n.Id))
                AND n.RunningTasks < n.MaxParallelTasks AND EXISTS (
                    SELECT 1 FROM batch.Job j WITH (READPAST, READCOMMITTEDLOCK)
                    WHERE j.NodeId = n.Id AND j.Kind = n.Phase AND j.Status = 'Ready'
                        AND j.AvailableAt <= SYSUTCDATETIME()
                        AND (n.SequentialTasks = 0 OR j.Kind <> 'Task' OR j.Id = (
                            SELECT TOP (1) firstTask.Id FROM batch.Job firstTask
                            WHERE firstTask.NodeId = n.Id AND firstTask.Kind = 'Task' AND firstTask.Status = 'Ready'
                            ORDER BY firstTask.Id
                        ))
                )
        )
        ORDER BY s.SubmittedAt, s.Id;

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
        -- Claim at most one initial process per batch so candidates cannot conflict with each other.
        SELECT TOP (1) @NextProcess = n.Id
        FROM @EligibleAdmissions e JOIN batch.Node n ON n.Id = e.NodeId
        JOIN @Locked l ON l.RunId = n.RunId JOIN batch.Submission s ON s.Id = n.RunId
        WHERE n.Status = 'Running' AND s.Status = 'Running' AND n.AdmittedAt IS NULL
        ORDER BY s.SubmittedAt, n.StartedAt, n.Id;

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
                AND (n.SequentialTasks = 0 OR j.Kind <> 'Task' OR j.Id = (
                    SELECT TOP (1) firstTask.Id FROM batch.Job firstTask
                    WHERE firstTask.NodeId = n.Id AND firstTask.Kind = 'Task' AND firstTask.Status = 'Ready'
                    ORDER BY firstTask.Id
                ))
            ORDER BY j.AvailableAt, j.Id
        ) q
        WHERE n.Status = 'Running' AND n.Phase IN ('Start', 'Fetch', 'Task', 'Finish')
            AND (n.Phase = 'Finish' OR n.ProcessDeadlineAt IS NULL OR n.ProcessDeadlineAt > SYSUTCDATETIME())
            AND (n.Kind = 'Group' OR n.AdmittedAt IS NOT NULL OR n.Id = @NextProcess)
        ORDER BY q.QueuePosition, q.AvailableAt, q.Id;

        UPDATE j SET Status = 'Running', Attempts = Attempts + 1, WorkerId = @WorkerId,
            LeaseToken = NEWID(), LeaseUntil = DATEADD(second, @LeaseSeconds, SYSUTCDATETIME()),
            StartedAt = COALESCE(StartedAt, SYSUTCDATETIME())
        OUTPUT inserted.Id, inserted.NodeId, inserted.Kind, inserted.[RowCount] INTO @Claimed
        FROM batch.Job j JOIN @Candidates c ON j.Id = c.Id;

        UPDATE n SET AdmittedAt = SYSUTCDATETIME(),
            ProcessDeadlineAt = DATEADD(second, n.ProcessTimeoutSeconds, SYSUTCDATETIME())
        FROM batch.Node n JOIN @Claimed c ON c.NodeId = n.Id
        WHERE n.Id = @NextProcess;

        UPDATE n SET RunningTasks = RunningTasks + c.Amount, RunningRecords = RunningRecords + c.Records
        FROM batch.Node n JOIN (
            SELECT NodeId, COUNT(*) AS Amount, SUM(CONVERT(bigint, [RowCount])) AS Records
            FROM @Claimed WHERE Kind = 'Task' GROUP BY NodeId
        ) c ON n.Id = c.NodeId;
    END;

    SET @Payload = (
        SELECT j.Id, n.RunId, n.NodeKey AS ProcessKey, j.Kind, n.Kind AS NodeKind,
            COALESCE(n.Handler, n.GroupHandler) AS Handler, n.OptionsJson,
            CASE WHEN j.Kind <> 'Task' THEN s.InputJson END AS InputJson,
            j.RecordKey, j.PayloadJson, j.CursorValue AS [Cursor], j.Attempts, j.LeaseToken,
            CASE WHEN j.Kind <> 'Task' THEN n.TimeoutSeconds
                 WHEN n.TaskTimeoutEnabled = 1 THEN COALESCE(n.TaskTimeoutSeconds, n.TimeoutSeconds)
            END AS TimeoutSeconds,
            CASE WHEN j.Kind <> 'Finish'
                 THEN DATEDIFF_BIG(millisecond, SYSUTCDATETIME(), n.ProcessDeadlineAt)
            END AS ProcessMillisecondsRemaining,
            CASE WHEN n.Kind = 'Group' AND j.Kind = 'Finish' THEN batch.MergeStatus(n.OwnStatus, n.ChildrenStatus)
                 ELSE n.OwnStatus END AS OwnStatus,
            n.TotalTasks, n.SucceededTasks, n.InvalidTasks, n.FailedTasks,
            CASE WHEN n.TasksReleased = 1 THEN CONVERT(bigint, 0) ELSE n.TotalTasks END AS SkippedTasks,
            COALESCE(n.Error, CASE WHEN n.Kind = 'Group' AND j.Kind = 'Finish' AND n.ChildrenStatus = 'Failed'
                THEN N'One or more children failed' END) AS ProcessError
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
    JOIN batch.Node n ON n.Id = j.NodeId
    WHERE j.LeaseToken = c.Token AND j.Status = 'Running' AND j.LeaseUntil > SYSUTCDATETIME()
        AND j.Kind = n.Phase
        AND (j.Kind = 'Finish' OR n.ProcessDeadlineAt IS NULL OR n.ProcessDeadlineAt > SYSUTCDATETIME());
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
    DECLARE @NodeId uniqueidentifier, @Kind varchar(8), @NodeKind varchar(8), @Attempts int, @MaxAttempts int;
    SELECT @NodeId = j.NodeId, @Kind = j.Kind, @NodeKind = n.Kind,
        @Attempts = j.Attempts - j.AttemptBase, @MaxAttempts = n.MaxAttempts
    FROM batch.Job j WITH (UPDLOCK) JOIN batch.Node n ON n.Id = j.NodeId
    WHERE j.Id = @Id AND j.LeaseToken = @Token AND j.Status = 'Running' AND j.Kind = n.Phase
        AND (j.Kind = 'Finish' OR n.ProcessDeadlineAt IS NULL OR n.ProcessDeadlineAt > SYSUTCDATETIME())
        AND ((@Expired = 1 AND j.LeaseUntil <= SYSUTCDATETIME()) OR
             (@Expired = 0 AND j.LeaseUntil > SYSUTCDATETIME()));
    IF @NodeId IS NULL RETURN;

    IF @Status = 'Released'
    BEGIN
        UPDATE batch.Job SET Status = 'Ready', AttemptBase += 1, LeaseToken = NULL, LeaseUntil = NULL,
            AvailableAt = SYSUTCDATETIME() WHERE Id = @Id;
        RETURN;
    END;

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
        DECLARE @Staged table ([RowCount] int NOT NULL);
        ;WITH PageRecords AS (
            SELECT r.RecordKey, r.PayloadJson, COALESCE(r.[RowCount], 1) AS [RowCount],
                CONVERT(int, entries.[key]) AS Position, ROW_NUMBER() OVER (
                PARTITION BY r.RecordKey COLLATE Latin1_General_100_BIN2 ORDER BY CONVERT(int, entries.[key])
            ) AS RowNumber
            FROM OPENJSON(@Records) entries
            CROSS APPLY OPENJSON(entries.value)
                WITH (RecordKey nvarchar(300) '$.Key', PayloadJson nvarchar(max), [RowCount] int) r
        )
        INSERT batch.Job(NodeId, Kind, RecordKey, PayloadJson, [RowCount])
        OUTPUT inserted.[RowCount] INTO @Staged
        SELECT @NodeId, 'Task', r.RecordKey, r.PayloadJson, r.[RowCount]
        FROM PageRecords r WHERE r.RowNumber = 1 AND NOT EXISTS (
            SELECT 1 FROM batch.Job j
            WHERE j.NodeId = @NodeId AND j.Kind = 'Task' AND j.RecordKey = r.RecordKey COLLATE Latin1_General_100_BIN2
        )
        ORDER BY r.Position;
        UPDATE batch.Node SET TotalTasks = TotalTasks + staged.Tasks, TotalRecords = TotalRecords + staged.Records
        FROM (SELECT COUNT(*) AS Tasks, COALESCE(SUM(CONVERT(bigint, [RowCount])), 0) AS Records FROM @Staged) staged
        WHERE Id = @NodeId;

        IF @NextCursor IS NOT NULL
        BEGIN
            UPDATE batch.Job SET Status = 'Ready', LeaseToken = NULL, LeaseUntil = NULL,
                CursorValue = @NextCursor, Attempts = 0, AttemptBase = 0, Error = NULL, AvailableAt = SYSUTCDATETIME()
            WHERE Id = @Id;
            RETURN;
        END;
    END;

    UPDATE batch.Job SET Status = @Status, LeaseToken = NULL, LeaseUntil = NULL,
        Error = @Error, FinishedAt = SYSUTCDATETIME() WHERE Id = @Id;

    IF @NodeKind = 'Process' AND @Status = 'Failed'
    BEGIN
        EXEC batch.StopRun @NodeId, @Error;
        RETURN;
    END;

    IF @NodeKind = 'Group'
    BEGIN
        IF @Kind = 'Start'
        BEGIN
            UPDATE batch.Node SET OwnStatus = @Status, Error = @Error WHERE Id = @NodeId;
            EXEC batch.MoveTree @NodeId, 'O';
        END
        ELSE
        BEGIN
            IF @Status <> 'Success'
                UPDATE batch.Node SET OwnStatus = batch.MergeStatus(OwnStatus, @Status),
                    Error = CONCAT_WS(N' / ', Error, @Error) WHERE Id = @NodeId;
            DECLARE @GroupOutcome varchar(8);
            SELECT @GroupOutcome = batch.MergeStatus(OwnStatus, ChildrenStatus) FROM batch.Node WHERE Id = @NodeId;
            EXEC batch.MoveTree @NodeId, 'C', @GroupOutcome;
        END;
        RETURN;
    END;

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
    DECLARE @Changed table(NodeId uniqueidentifier, Status varchar(8), [RowCount] bigint, Error nvarchar(2000));
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

    UPDATE j SET Status = CASE WHEN retry.ShouldRetry = 1 OR r.Status = 'Released' THEN 'Ready' ELSE r.Status END,
        AttemptBase = j.AttemptBase + CASE WHEN r.Status = 'Released' THEN 1 ELSE 0 END,
        AvailableAt = CASE WHEN retry.ShouldRetry = 1
            THEN DATEADD(second, batch.BackoffSeconds(j.Attempts - j.AttemptBase), SYSUTCDATETIME()) ELSE j.AvailableAt END,
        FinishedAt = CASE WHEN retry.ShouldRetry = 0 AND r.Status <> 'Released' THEN SYSUTCDATETIME() END,
        LeaseToken = NULL, LeaseUntil = NULL, OutputJson = r.OutputJson, Error = r.Error
    OUTPUT inserted.NodeId, inserted.Status, inserted.[RowCount], inserted.Error INTO @Changed
    FROM @Result r JOIN batch.Job j ON j.Id = r.Id
    JOIN batch.Node n ON n.Id = j.NodeId
    CROSS APPLY (VALUES (CASE WHEN r.Status = 'Failed' AND r.Retryable = 1 AND j.Attempts - j.AttemptBase < n.MaxAttempts
        THEN 1 ELSE 0 END)) retry(ShouldRetry)
    WHERE j.Kind = 'Task' AND n.Phase = 'Task' AND j.Status = 'Running' AND j.LeaseToken = r.Token
        AND (n.ProcessDeadlineAt IS NULL OR n.ProcessDeadlineAt > SYSUTCDATETIME()) AND (
        (@Expired = 1 AND j.LeaseUntil <= SYSUTCDATETIME()) OR
        (@Expired = 0 AND j.LeaseUntil > SYSUTCDATETIME()));

    UPDATE n SET RunningTasks = n.RunningTasks - c.Amount,
        SucceededTasks = n.SucceededTasks + c.Successes,
        InvalidTasks = n.InvalidTasks + c.Invalids,
        FailedTasks = n.FailedTasks + c.Failures,
        RunningRecords = n.RunningRecords - c.Records,
        SucceededRecords = n.SucceededRecords + c.SuccessfulRecords,
        InvalidRecords = n.InvalidRecords + c.InvalidRecords,
        FailedRecords = n.FailedRecords + c.FailedRecords
    FROM batch.Node n JOIN (
        SELECT NodeId, COUNT(*) AS Amount,
            SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) AS Successes,
            SUM(CASE WHEN Status = 'Invalid' THEN 1 ELSE 0 END) AS Invalids,
            SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS Failures,
            SUM([RowCount]) AS Records,
            SUM(CASE WHEN Status = 'Success' THEN [RowCount] ELSE 0 END) AS SuccessfulRecords,
            SUM(CASE WHEN Status = 'Invalid' THEN [RowCount] ELSE 0 END) AS InvalidRecords,
            SUM(CASE WHEN Status = 'Failed' THEN [RowCount] ELSE 0 END) AS FailedRecords
        FROM @Changed GROUP BY NodeId
    ) c ON c.NodeId = n.Id;

    INSERT @Done
    SELECT n.Id, CASE WHEN n.FailedTasks > 0 THEN 'Failed' WHEN n.InvalidTasks > 0 THEN 'Invalid' ELSE 'Success' END
    FROM batch.Node n JOIN (SELECT DISTINCT NodeId FROM @Changed) c ON c.NodeId = n.Id
    WHERE n.Phase = 'Task' AND (n.FailedTasks > 0 OR n.TotalTasks = n.SucceededTasks + n.InvalidTasks);

    DECLARE @NodeId uniqueidentifier, @Status varchar(8), @Error nvarchar(2000);
    -- Record completed parallel branches before a failure in the same result batch pauses their run.
    DECLARE finished CURSOR LOCAL FAST_FORWARD FOR
        SELECT NodeId, Status FROM @Done ORDER BY CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END, NodeId;
    OPEN finished;
    FETCH NEXT FROM finished INTO @NodeId, @Status;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Error = CASE @Status WHEN 'Failed' THEN N'One or more tasks failed'
                                   WHEN 'Invalid' THEN N'One or more tasks were invalid' END;
        IF @Status = 'Failed'
        BEGIN
            SELECT TOP (1) @Error = COALESCE(Error, @Error) FROM @Changed WHERE NodeId = @NodeId AND Status = 'Failed';
            EXEC batch.StopRun @NodeId, @Error;
        END
        ELSE EXEC batch.BeginFinish @NodeId, @Status, @Error;
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

CREATE OR ALTER PROCEDURE batch.ExpireProcesses @Limit int
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @NodeId uniqueidentifier, @RunId uniqueidentifier, @Resource nvarchar(255), @LockResult int;
    DECLARE deadlines CURSOR LOCAL FAST_FORWARD FOR
        SELECT TOP (@Limit) Id, RunId FROM batch.Node WITH (READPAST, READCOMMITTEDLOCK)
        WHERE ProcessDeadlineAt <= SYSUTCDATETIME() AND Phase IN ('Start', 'Fetch', 'Task')
        ORDER BY ProcessDeadlineAt, Id;

    OPEN deadlines;
    FETCH NEXT FROM deadlines INTO @NodeId, @RunId;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRANSACTION;
        SET @Resource = CONCAT(N'batch.run.', CONVERT(nvarchar(36), @RunId));
        EXEC @LockResult = sys.sp_getapplock @Resource = @Resource, @LockMode = 'Exclusive',
            @LockOwner = 'Transaction', @LockTimeout = 0;
        IF @LockResult >= 0 AND EXISTS (
            SELECT 1 FROM batch.Node WHERE Id = @NodeId
                AND ProcessDeadlineAt <= SYSUTCDATETIME() AND Phase IN ('Start', 'Fetch', 'Task')
        )
            EXEC batch.StopRun @NodeId, N'Process execution timed out', 1;
        COMMIT TRANSACTION;
        FETCH NEXT FROM deadlines INTO @NodeId, @RunId;
    END;
    CLOSE deadlines;
    DEALLOCATE deadlines;
END;
GO

CREATE OR ALTER PROCEDURE batch.Reap @Limit int
AS
BEGIN
    SET NOCOUNT ON;
    EXEC batch.ExpireProcesses @Limit;
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
