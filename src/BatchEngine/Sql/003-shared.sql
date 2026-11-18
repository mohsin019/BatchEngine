CREATE OR ALTER PROCEDURE batch.Share
    @JobId bigint, @Token uniqueidentifier, @RunId uniqueidentifier, @Operation varchar(5),
    @Keys nvarchar(max) = NULL, @Key nvarchar(200) = NULL, @ValueJson nvarchar(max) = NULL,
    @ExpectedVersion bigint = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @Reply nvarchar(max);
    IF NOT EXISTS (
        SELECT 1 FROM batch.Job j WITH (UPDLOCK, HOLDLOCK) JOIN batch.Node n ON n.Id = j.NodeId
        WHERE j.Id = @JobId AND j.Status = 'Running' AND j.LeaseToken = @Token
            AND j.LeaseUntil > SYSUTCDATETIME() AND n.RunId = @RunId
    )
        SET @Reply = N'{"Error":"This attempt no longer owns its lease"}';
    ELSE IF @Operation = 'get'
        SET @Reply = (
            SELECT JSON_QUERY((
                SELECT d.[Key], d.ValueJson, d.Version
                FROM batch.SharedData d JOIN OPENJSON(@Keys) k
                    ON d.[Key] = k.value COLLATE Latin1_General_100_BIN2
                WHERE d.RunId = @RunId FOR JSON PATH
            )) AS [Values] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );
    ELSE
    BEGIN
        DECLARE @Current bigint;
        SELECT @Current = Version FROM batch.SharedData WITH (UPDLOCK, HOLDLOCK)
        WHERE RunId = @RunId AND [Key] = @Key;

        IF @ExpectedVersion IS NOT NULL AND @ExpectedVersion <> COALESCE(@Current, 0)
            SET @Reply = N'{"Conflict":true,"Error":"The shared value has changed"}';
        ELSE
        BEGIN
            IF @Current IS NULL
                INSERT batch.SharedData (RunId, [Key], ValueJson, Version) VALUES (@RunId, @Key, @ValueJson, 1);
            ELSE
                UPDATE batch.SharedData SET ValueJson = @ValueJson, Version = @Current + 1,
                    UpdatedAt = SYSUTCDATETIME() WHERE RunId = @RunId AND [Key] = @Key;
            SET @Reply = (SELECT COALESCE(@Current, 0) + 1 AS Version FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
        END;
    END;
    COMMIT TRANSACTION;
    SELECT @Reply;
END;
GO
