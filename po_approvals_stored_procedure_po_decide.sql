USE [WebappsDev]
GO

/****** Object:  StoredProcedure [dbo].[PO_Decide]    Script Date: 10/24/2025 10:15:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* 1.4 Single point of truth for decisions (atomic, optimistic) */
CREATE   PROCEDURE [dbo].[PO_Decide]
  @PoNumber        nvarchar(20),
  @NewStatus       char(1),           -- 'A' or 'D'
  @ChangedBy       nvarchar(100),
  @DecisionNote    nvarchar(4000) = NULL,
  @ExpectedStatus  char(1) = 'W',     -- safety: only decide W
  @RowVersion      varbinary(8) = NULL  -- optional optimistic token from UI
AS
BEGIN
  SET NOCOUNT ON; SET XACT_ABORT ON;
  IF (@NewStatus NOT IN ('A','D')) THROW 50000, 'NewStatus must be A or D.', 1;

  BEGIN TRAN;

  DECLARE @OldStatus char(1);
  SELECT @OldStatus = [Status]
  FROM dbo.PO_Header WITH (UPDLOCK, ROWLOCK)
  WHERE PoNumber = @PoNumber
    AND (@RowVersion IS NULL OR RowVersion = @RowVersion);

  IF @OldStatus IS NULL THROW 50001, 'PO not found or rowversion mismatch.', 1;
  IF @OldStatus <> @ExpectedStatus THROW 50002, 'PO not in expected status.', 1;

  UPDATE dbo.PO_Header
    SET [Status] = @NewStatus,
        StatusChangedAtUtc = sysutcdatetime(),
        StatusChangedBy = @ChangedBy,
        DecisionNote = @DecisionNote,
        PrmsSyncStatus = 'Pending',
        PrmsSyncError = NULL
  WHERE PoNumber = @PoNumber;

  INSERT dbo.PO_Approval_Audit (PoNumber, OldStatus, NewStatus, ChangedBy, DecisionNote)
  VALUES (@PoNumber, @OldStatus, @NewStatus, @ChangedBy, @DecisionNote);

  COMMIT;
END
GO


