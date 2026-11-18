CREATE OR ALTER VIEW batch.ProcessProgress AS
SELECT n.Id AS NodeId, n.RunId, n.NodeKey AS ProcessKey, n.Status, n.OwnStatus, n.Phase, n.Error,
    n.TotalTasks, completed.ProcessedTasks,
    n.TotalTasks - completed.ProcessedTasks - n.RunningTasks - n.FailedTasks - skipped.SkippedTasks AS PendingTasks,
    n.RunningTasks, n.SucceededTasks, n.InvalidTasks, n.FailedTasks, skipped.SkippedTasks,
    n.TotalRecords, completed.ProcessedRecords,
    n.TotalRecords - completed.ProcessedRecords - n.RunningRecords - n.FailedRecords - skipped.SkippedRecords AS PendingRecords,
    n.RunningRecords, n.SucceededRecords, n.InvalidRecords, n.FailedRecords, skipped.SkippedRecords
FROM batch.Node n
CROSS APPLY (VALUES (n.SucceededTasks + n.InvalidTasks, n.SucceededRecords + n.InvalidRecords))
    completed(ProcessedTasks, ProcessedRecords)
CROSS APPLY (VALUES (
    CASE WHEN n.TasksReleased = 0 AND n.OwnStatus IS NOT NULL THEN n.TotalTasks ELSE 0 END,
    CASE WHEN n.TasksReleased = 0 AND n.OwnStatus IS NOT NULL THEN n.TotalRecords ELSE 0 END
)) skipped(SkippedTasks, SkippedRecords)
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
    COALESCE(t.SkippedTasks, 0) AS SkippedTasks,
    COALESCE(t.TotalRecords, 0) AS TotalRecords,
    COALESCE(t.ProcessedRecords, 0) AS ProcessedRecords,
    COALESCE(t.PendingRecords, 0) AS PendingRecords,
    COALESCE(t.RunningRecords, 0) AS RunningRecords,
    COALESCE(t.SucceededRecords, 0) AS SucceededRecords,
    COALESCE(t.InvalidRecords, 0) AS InvalidRecords,
    COALESCE(t.FailedRecords, 0) AS FailedRecords,
    COALESCE(t.SkippedRecords, 0) AS SkippedRecords
FROM batch.Submission s
OUTER APPLY (
    SELECT SUM(p.TotalTasks) AS TotalTasks, SUM(p.ProcessedTasks) AS ProcessedTasks,
        SUM(p.PendingTasks) AS PendingTasks, SUM(CONVERT(bigint, p.RunningTasks)) AS RunningTasks,
        SUM(p.SucceededTasks) AS SucceededTasks, SUM(p.InvalidTasks) AS InvalidTasks,
        SUM(p.FailedTasks) AS FailedTasks, SUM(p.SkippedTasks) AS SkippedTasks,
        SUM(p.TotalRecords) AS TotalRecords, SUM(p.ProcessedRecords) AS ProcessedRecords,
        SUM(p.PendingRecords) AS PendingRecords, SUM(p.RunningRecords) AS RunningRecords,
        SUM(p.SucceededRecords) AS SucceededRecords, SUM(p.InvalidRecords) AS InvalidRecords,
        SUM(p.FailedRecords) AS FailedRecords, SUM(p.SkippedRecords) AS SkippedRecords
    FROM batch.ProcessProgress p
    WHERE p.RunId = s.Id
) t;
GO
