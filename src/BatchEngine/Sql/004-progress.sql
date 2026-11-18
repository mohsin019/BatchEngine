CREATE OR ALTER VIEW batch.ProcessProgress AS
SELECT n.Id AS NodeId, n.RunId, n.NodeKey AS ProcessKey, n.Status, n.OwnStatus, n.Phase, n.Error,
    n.TotalTasks, completed.ProcessedTasks,
    n.TotalTasks - completed.ProcessedTasks - n.RunningTasks - n.FailedTasks - skipped.SkippedTasks AS PendingTasks,
    n.RunningTasks, n.SucceededTasks, n.InvalidTasks, n.FailedTasks, skipped.SkippedTasks
FROM batch.Node n
CROSS APPLY (VALUES (n.SucceededTasks + n.InvalidTasks)) completed(ProcessedTasks)
CROSS APPLY (VALUES (
    CASE WHEN n.TasksReleased = 0 AND n.OwnStatus IS NOT NULL THEN n.TotalTasks ELSE 0 END
)) skipped(SkippedTasks)
WHERE n.Kind = 'Process';
GO

CREATE OR ALTER VIEW batch.RunProgress AS
SELECT s.Id AS RunId, s.Status, s.Error,
    COALESCE(t.TotalTasks, 0) AS TotalTasks,
    COALESCE(t.ProcessedTasks, 0) AS ProcessedTasks,
    COALESCE(t.PendingTasks, 0) AS PendingTasks,
    COALESCE(t.RunningTasks, 0) AS RunningTasks,
    COALESCE(t.SucceededTasks, 0) AS SucceededTasks,
    COALESCE(t.InvalidTasks, 0) AS InvalidTasks,
    COALESCE(t.FailedTasks, 0) AS FailedTasks,
    COALESCE(t.SkippedTasks, 0) AS SkippedTasks
FROM batch.Submission s
OUTER APPLY (
    SELECT SUM(p.TotalTasks) AS TotalTasks, SUM(p.ProcessedTasks) AS ProcessedTasks,
        SUM(p.PendingTasks) AS PendingTasks, SUM(CONVERT(bigint, p.RunningTasks)) AS RunningTasks,
        SUM(p.SucceededTasks) AS SucceededTasks, SUM(p.InvalidTasks) AS InvalidTasks,
        SUM(p.FailedTasks) AS FailedTasks, SUM(p.SkippedTasks) AS SkippedTasks
    FROM batch.ProcessProgress p
    WHERE p.RunId = s.Id
) t;
GO
