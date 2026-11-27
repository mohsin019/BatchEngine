-- Called inside the transaction holding batch.run.<id>. Keep the execution phase and all page checkpoints.
CREATE OR ALTER PROCEDURE batch.StopRun @NodeId uniqueidentifier, @Error nvarchar(2000), @Deadline bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunId uniqueidentifier, @Now datetime2(3) = SYSUTCDATETIME(), @Key nvarchar(64);
    SELECT @RunId = RunId, @Key = NodeKey FROM batch.Node WHERE Id = @NodeId AND Status = 'Running';
    IF @RunId IS NULL RETURN;

    DECLARE @Interrupted table(NodeId uniqueidentifier, Kind varchar(8), Status varchar(8), [RowCount] int);
    UPDATE j SET Status = CASE WHEN n.Id = @NodeId AND @Deadline = 1 THEN 'Failed' ELSE 'Ready' END,
        LeaseToken = NULL, LeaseUntil = NULL, AvailableAt = @Now,
        FinishedAt = CASE WHEN n.Id = @NodeId AND @Deadline = 1 THEN @Now END,
        Error = CASE WHEN n.Id = @NodeId AND @Deadline = 1 THEN @Error END,
        AttemptBase = CASE WHEN n.Id = @NodeId AND @Deadline = 1 THEN j.AttemptBase ELSE j.AttemptBase + 1 END
    OUTPUT inserted.NodeId, inserted.Kind, inserted.Status, inserted.[RowCount] INTO @Interrupted
    FROM batch.Node n JOIN batch.Job j ON j.NodeId = n.Id
    WHERE n.RunId = @RunId AND j.Status = 'Running';

    UPDATE n SET Status = 'Stopped', RunningTasks = 0, RunningRecords = 0,
        FailedTasks = n.FailedTasks + COALESCE(f.Tasks, 0), FailedRecords = n.FailedRecords + COALESCE(f.Records, 0),
        AdmittedAt = NULL, ProcessDeadlineAt = NULL,
        TimedOutAt = CASE WHEN n.Id = @NodeId AND @Deadline = 1 THEN @Now ELSE TimedOutAt END,
        ErrorBeforeStop = Error, Error = CASE WHEN n.Id = @NodeId THEN @Error ELSE Error END
    FROM batch.Node n LEFT JOIN (
        SELECT NodeId, COUNT(*) AS Tasks, SUM(CONVERT(bigint, [RowCount])) AS Records
        FROM @Interrupted WHERE Kind = 'Task' AND Status = 'Failed' GROUP BY NodeId
    ) f ON n.Id = f.NodeId
    WHERE n.RunId = @RunId AND n.Status = 'Running';
    UPDATE batch.Submission SET Status = 'Stopped', StoppedAt = @Now, Error = CONCAT(@Key, N': ', @Error)
    WHERE Id = @RunId;
END;
GO

CREATE OR ALTER PROCEDURE batch.Resume @RunId uniqueidentifier, @ExpectedResumeCount int = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @Resource nvarchar(255) = CONCAT(N'batch.run.', CONVERT(nvarchar(36), @RunId)), @Lock int, @Status varchar(12);
    EXEC @Lock = sys.sp_getapplock @Resource = @Resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 3000;
    IF @Lock < 0 THROW 51011, 'Could not lock the batch run for resume.', 1;
    DECLARE @ResumeCount int;
    SELECT @Status = Status, @ResumeCount = ResumeCount FROM batch.Submission WITH (UPDLOCK) WHERE Id = @RunId;
    IF @Status IS NULL THROW 51018, 'Run was not found.', 1;
    IF @ExpectedResumeCount IS NOT NULL AND @ExpectedResumeCount <> @ResumeCount
        THROW 51022, 'Run changed. Reload its status before resuming.', 1;
    IF (@Status = 'Running' AND @ResumeCount = 0) OR @Status NOT IN ('Stopped', 'Running') THROW 51022, 'Only a stopped run can be resumed.', 1;

    IF @Status = 'Stopped'
    BEGIN
        UPDATE j SET Status = 'Ready', AvailableAt = SYSUTCDATETIME(), FinishedAt = NULL,
            AttemptBase = Attempts, Error = NULL
        FROM batch.Node n JOIN batch.Job j ON j.NodeId = n.Id
        WHERE n.RunId = @RunId AND n.Status = 'Stopped' AND j.Status = 'Failed' AND j.Kind = n.Phase;
        UPDATE batch.Node SET Status = 'Running', TimedOutAt = NULL,
            FailedTasks = CASE WHEN Phase = 'Task' THEN 0 ELSE FailedTasks END,
            FailedRecords = CASE WHEN Phase = 'Task' THEN 0 ELSE FailedRecords END,
            Error = ErrorBeforeStop, ErrorBeforeStop = NULL
        WHERE RunId = @RunId AND Status = 'Stopped';
        UPDATE batch.Submission SET Status = 'Running', Error = NULL, ResumeCount += 1 WHERE Id = @RunId;
    END;
    COMMIT;
END;
GO
