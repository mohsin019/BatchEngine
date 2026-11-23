IF OBJECT_ID('batch.ProcessGroup', 'U') IS NULL
BEGIN
    CREATE TABLE batch.ProcessGroup (
        GroupKey nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY
            CHECK (GroupKey LIKE '[A-Za-z0-9]%' AND GroupKey NOT LIKE '%[^-A-Za-z0-9_.]%'
                AND DATALENGTH(GroupKey) = 2 * LEN(GroupKey)),
        DisplayName nvarchar(120) NOT NULL CHECK (LEN(TRIM(DisplayName)) > 0),
        DefinitionJson nvarchar(max) NOT NULL,
        Revision bigint NOT NULL CHECK (Revision > 0),
        UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CHECK (ISJSON(DefinitionJson, OBJECT) = 1
            AND COALESCE(JSON_VALUE(DefinitionJson, '$.type'), '') = 'group'
            AND COALESCE(JSON_VALUE(DefinitionJson, '$.key'), '') COLLATE Latin1_General_100_BIN2 = GroupKey)
    );
END;
GO

IF COL_LENGTH('batch.Submission', 'GroupKey') IS NULL
BEGIN
    ALTER TABLE batch.Submission ADD GroupKey nvarchar(64) COLLATE Latin1_General_100_BIN2 NULL,
        GroupRevision bigint NULL;
END;
GO

IF OBJECT_ID('batch.CK_BatchSubmission_GroupRevision', 'C') IS NULL
    ALTER TABLE batch.Submission ADD CONSTRAINT CK_BatchSubmission_GroupRevision CHECK (
        (GroupKey IS NULL AND GroupRevision IS NULL) OR
        (GroupKey IS NOT NULL AND GroupRevision IS NOT NULL AND GroupRevision > 0));
GO

IF COL_LENGTH('batch.Submission', 'SelectedProcessKeysJson') IS NULL
    ALTER TABLE batch.Submission ADD SelectedProcessKeysJson nvarchar(max) NULL
        CHECK (SelectedProcessKeysJson IS NULL OR ISJSON(SelectedProcessKeysJson, ARRAY) = 1);
GO

CREATE OR ALTER PROCEDURE batch.SaveProcessGroup
    @GroupKey nvarchar(64), @DisplayName nvarchar(120), @DefinitionJson nvarchar(max), @ExpectedRevision bigint
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;

    DECLARE @Revision bigint;
    SELECT @Revision = Revision FROM batch.ProcessGroup WITH (UPDLOCK, HOLDLOCK) WHERE GroupKey = @GroupKey;
    IF @ExpectedRevision IS NULL OR @ExpectedRevision <> COALESCE(@Revision, 0)
        THROW 51017, 'Group revision changed. Reload before saving.', 1;

    IF @Revision IS NULL
    BEGIN
        SET @Revision = 1;
        INSERT batch.ProcessGroup (GroupKey, DisplayName, DefinitionJson, Revision)
        VALUES (@GroupKey, @DisplayName, @DefinitionJson, @Revision);
    END
    ELSE
    BEGIN
        SET @Revision += 1;
        UPDATE batch.ProcessGroup
        SET DisplayName = @DisplayName, DefinitionJson = @DefinitionJson, Revision = @Revision,
            UpdatedAt = SYSUTCDATETIME()
        WHERE GroupKey = @GroupKey;
    END;

    COMMIT TRANSACTION;
    SELECT @Revision;
END;
GO

CREATE OR ALTER PROCEDURE batch.Submit
    @DefinitionJson nvarchar(max) = NULL, @InputJson nvarchar(max) = N'{}', @SubmissionKey nvarchar(200) = NULL,
    @GroupKey nvarchar(64) = NULL, @GroupRevision bigint = NULL,
    @SelectedProcessKeysJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;

    DECLARE @Id uniqueidentifier, @ExistingGroup nvarchar(64), @ExistingRevision bigint, @ExistingInput nvarchar(max), @ExistingSelection nvarchar(max);
    IF @SubmissionKey IS NOT NULL
        SELECT @Id = Id, @ExistingGroup = GroupKey, @ExistingRevision = GroupRevision, @ExistingInput = InputJson,
            @ExistingSelection = SelectedProcessKeysJson
        FROM batch.Submission WITH (UPDLOCK, HOLDLOCK)
        WHERE SubmissionKey = @SubmissionKey AND SubmissionKey IS NOT NULL;

    IF @Id IS NULL
    BEGIN
        IF @GroupKey IS NOT NULL
        BEGIN
            DECLARE @Revision bigint;
            SELECT @DefinitionJson = DefinitionJson, @Revision = Revision
            FROM batch.ProcessGroup WHERE GroupKey = @GroupKey;
            IF @Revision IS NULL THROW 51018, 'Process group was not found.', 1;
            IF @GroupRevision IS NULL OR @GroupRevision <> @Revision
                THROW 51019, 'Group revision changed. Reload before submitting.', 1;
        END;

        IF @SelectedProcessKeysJson IS NOT NULL
        BEGIN
            IF @GroupKey IS NULL OR ISJSON(@SelectedProcessKeysJson, ARRAY) <> 1
                THROW 51021, 'Select processes from a saved group.', 1;

            DECLARE @Selection table(ProcessKey nvarchar(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY);
            IF NOT EXISTS (SELECT 1 FROM OPENJSON(@SelectedProcessKeysJson)) OR EXISTS (
                SELECT 1 FROM OPENJSON(@SelectedProcessKeysJson)
                WHERE type <> 1 OR DATALENGTH(value) NOT BETWEEN 2 AND 128 OR DATALENGTH(value) <> 2 * LEN(value)
            )
                THROW 51021, 'Provide a nonempty list of process keys.', 1;

            INSERT @Selection SELECT DISTINCT value COLLATE Latin1_General_100_BIN2
            FROM OPENJSON(@SelectedProcessKeysJson);
            DECLARE @Matched int;
            ;WITH Tree AS (
                SELECT @DefinitionJson AS NodeJson
                UNION ALL
                SELECT child.value
                FROM Tree CROSS APPLY OPENJSON(Tree.NodeJson, '$.children') child
                WHERE child.type = 5
            )
            SELECT @Matched = COUNT(*) FROM @Selection s
            JOIN Tree t ON JSON_VALUE(t.NodeJson, '$.key') COLLATE Latin1_General_100_BIN2 = s.ProcessKey
            WHERE JSON_VALUE(t.NodeJson, '$.type') = 'process'
            OPTION (MAXRECURSION 32);
            IF @Matched <> (SELECT COUNT(*) FROM @Selection)
                THROW 51021, 'Every selected key must identify a process in the saved group.', 1;
        END;

        DECLARE @Inserted table(Id uniqueidentifier);
        INSERT batch.Submission (SubmissionKey, DefinitionJson, InputJson, GroupKey, GroupRevision, SelectedProcessKeysJson)
        OUTPUT inserted.Id INTO @Inserted
        VALUES (@SubmissionKey, @DefinitionJson, @InputJson, @GroupKey, @GroupRevision, @SelectedProcessKeysJson);
        SELECT @Id = Id FROM @Inserted;
    END
    ELSE IF @GroupKey IS NOT NULL AND (
        @ExistingGroup IS NULL OR @ExistingGroup COLLATE Latin1_General_100_BIN2 <> @GroupKey
        OR @GroupRevision IS NULL OR @ExistingRevision IS NULL OR @ExistingRevision <> @GroupRevision
        OR @InputJson IS NULL OR @ExistingInput COLLATE Latin1_General_100_BIN2 <> @InputJson
        OR COALESCE(@ExistingSelection, N'') COLLATE Latin1_General_100_BIN2 <> COALESCE(@SelectedProcessKeysJson, N''))
        THROW 51020, 'Submission key already belongs to a different request.', 1;

    COMMIT TRANSACTION;
    SELECT @Id;
END;
GO
