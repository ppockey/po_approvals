USE [WebappsDev];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

CREATE OR ALTER PROCEDURE dbo.PO_Merge
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @err nvarchar(4000) = NULL;

  BEGIN TRY
    BEGIN TRAN;

    /* =========================================
       Prep: capture header-merge changes (OUTPUT)
       ========================================= */
    DECLARE @HdrChanges TABLE
    (
      PoNumber     nvarchar(20),
      Action       nvarchar(10),  -- INSERT / UPDATE
      WasActive    bit,           -- prior IsActive (deleted)
      IsActiveNow  bit            -- new IsActive (inserted)
    );

    /* =========================
       HEADER: newest-wins source
       ========================= */
    ;WITH H AS (
      SELECT *
      FROM (
        SELECT
          PoNumber, PoDate, VendorNumber, VendorName, VendorAddr1, VendorAddr2, VendorAddr3,
          VendorState, VendorPostalCode, BuyerCode, BuyerName, HouseCode,
          DirectAmount, IndirectAmount, CreatedAtUtc,
          ROW_NUMBER() OVER (
            PARTITION BY PoNumber
            ORDER BY CreatedAtUtc DESC
          ) rn
        FROM dbo.PO_Stg_Header
        WHERE PoNumber IS NOT NULL
      ) x
      WHERE rn = 1
    )
    MERGE dbo.PO_Header AS tgt
    USING H AS src
      ON tgt.PoNumber = src.PoNumber
    WHEN MATCHED THEN
      UPDATE SET
         PoDate             = src.PoDate,
         VendorNumber       = COALESCE(src.VendorNumber, tgt.VendorNumber),
         VendorName         = src.VendorName,
         VendorAddr1        = src.VendorAddr1,
         VendorAddr2        = src.VendorAddr2,
         VendorAddr3        = src.VendorAddr3,
         VendorState        = src.VendorState,
         VendorPostalCode   = src.VendorPostalCode,
         BuyerCode          = src.BuyerCode,
         BuyerName          = src.BuyerName,
         HouseCode          = src.HouseCode,
         DirectAmount       = src.DirectAmount,
         IndirectAmount     = src.IndirectAmount,
         CreatedAtUtc       = src.CreatedAtUtc,
         -- Reactivate if soft-deleted previously
         IsActive           = 1,
         DeactivatedAtUtc   = NULL,
         DeactivatedBy      = NULL,
         DeactivationReason = NULL
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        PoNumber, PoDate, VendorNumber, VendorName, VendorAddr1, VendorAddr2, VendorAddr3,
        VendorState, VendorPostalCode, BuyerCode, BuyerName, HouseCode,
        DirectAmount, IndirectAmount, CreatedAtUtc, IsActive, [Status]
      )
      VALUES (
        src.PoNumber, src.PoDate, src.VendorNumber, src.VendorName, src.VendorAddr1, src.VendorAddr2, src.VendorAddr3,
        src.VendorState, src.VendorPostalCode, src.BuyerCode, src.BuyerName, src.HouseCode,
        src.DirectAmount, src.IndirectAmount, src.CreatedAtUtc, 1, 'W'
      )
    -- SOFT-DELETE headers missing from the snapshot (only while waiting)
    WHEN NOT MATCHED BY SOURCE AND tgt.[Status] = 'W' THEN
      UPDATE SET
        IsActive           = 0,
        DeactivatedAtUtc   = SYSUTCDATETIME(),
        DeactivatedBy      = 'PO_Merge',
        DeactivationReason = 'Header absent from PRMS waiting snapshot'
    OUTPUT
      inserted.PoNumber,
      $action,
      CAST(COALESCE(deleted.IsActive, 0) AS bit),
      CAST(inserted.IsActive AS bit)
    INTO @HdrChanges(PoNumber, Action, WasActive, IsActiveNow)
    ;

    /* ======================================================
       Emit outbox events for NEW or REACTIVATED waiting POs
       ====================================================== */
    INSERT INTO dbo.PO_ApprovalOutbox (EventType, PoNumber, OccurredAtUtc, PayloadJson)
    SELECT
      'PO_NEW_WAITING' AS EventType,
      hc.PoNumber,
      SYSUTCDATETIME() AS OccurredAtUtc,
      NULL             AS PayloadJson  -- optional JSON payload
    FROM @HdrChanges hc
    JOIN dbo.PO_Header h
      ON h.PoNumber = hc.PoNumber
    LEFT JOIN dbo.PO_ApprovalOutbox o
      ON o.EventType = 'PO_NEW_WAITING'
     AND o.PoNumber = hc.PoNumber
     AND o.ProcessedAtUtc IS NULL
    WHERE o.OutboxId IS NULL                 -- avoid duplicate unprocessed events
      AND h.[Status] = 'W'
      AND (hc.Action = 'INSERT' OR (hc.WasActive = 0 AND hc.IsActiveNow = 1));

    /* ==========================================
       LINES: newest-wins + resolve PoHeaderId FK
       ========================================== */
    ;WITH L AS (
      SELECT *
      FROM (
        SELECT
          PoNumber, LineNumber, HouseCode, ItemNumber, ItemDescription, ItemShortDescription,
          QuantityOrdered, OrderUom, UnitCost, ExtendedCost, RequiredDate, GlAccount, CreatedAtUtc,
          ROW_NUMBER() OVER (
            PARTITION BY PoNumber, LineNumber
            ORDER BY CreatedAtUtc DESC
          ) rn
        FROM dbo.PO_Stg_Line
        WHERE PoNumber IS NOT NULL AND LineNumber IS NOT NULL
      ) x
      WHERE rn = 1
    ),
    LS AS (  -- staged lines + resolved header FK
      SELECT
        L.*,
        H.PoHeaderId AS PoHeaderIdFk
      FROM L
      JOIN dbo.PO_Header H
        ON H.PoNumber = L.PoNumber
    )
    MERGE dbo.PO_Line AS tgt
    USING LS AS src
      ON tgt.PoNumber = src.PoNumber
     AND tgt.LineNumber = src.LineNumber
    WHEN MATCHED THEN
      UPDATE SET
         PoHeaderId           = COALESCE(tgt.PoHeaderId, src.PoHeaderIdFk), -- backfill if null
         HouseCode            = src.HouseCode,
         ItemNumber           = src.ItemNumber,
         ItemDescription      = src.ItemDescription,
         ItemShortDescription = src.ItemShortDescription,
         QuantityOrdered      = src.QuantityOrdered,
         OrderUom             = src.OrderUom,
         UnitCost             = src.UnitCost,
         ExtendedCost         = src.ExtendedCost,
         RequiredDate         = src.RequiredDate,
         GlAccount            = src.GlAccount,
         IsActive             = 1,
         DeactivatedAtUtc     = NULL,
         DeactivatedBy        = NULL,
         DeactivationReason   = NULL
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        PoHeaderId, PoNumber, LineNumber, HouseCode, ItemNumber, ItemDescription, ItemShortDescription,
        QuantityOrdered, OrderUom, UnitCost, ExtendedCost, RequiredDate, GlAccount, IsActive
      )
      VALUES (
        src.PoHeaderIdFk, src.PoNumber, src.LineNumber, src.HouseCode, src.ItemNumber, src.ItemDescription, src.ItemShortDescription,
        src.QuantityOrdered, src.OrderUom, src.UnitCost, src.ExtendedCost, src.RequiredDate, src.GlAccount, 1
      )
    -- SOFT-DELETE lines missing from snapshot, scoped to headers still waiting
    WHEN NOT MATCHED BY SOURCE
         AND EXISTS (SELECT 1
                     FROM dbo.PO_Header h
                     WHERE h.PoNumber = tgt.PoNumber
                       AND h.[Status] = 'W')
    THEN
      UPDATE SET
        IsActive           = 0,
        DeactivatedAtUtc   = SYSUTCDATETIME(),
        DeactivatedBy      = 'PO_Merge',
        DeactivationReason = 'Line absent from PRMS waiting snapshot'
    ;

    /* ----------------------------------------------
       Cascade line soft-delete if header soft-deleted
       ---------------------------------------------- */
    UPDATE Ln
      SET IsActive            = 0,
          DeactivatedAtUtc    = COALESCE(Ln.DeactivatedAtUtc, SYSUTCDATETIME()),
          DeactivatedBy       = COALESCE(Ln.DeactivatedBy, 'PO_Merge'),
          DeactivationReason  = COALESCE(Ln.DeactivationReason, 'Header soft-deleted')
    FROM dbo.PO_Line Ln
    JOIN dbo.PO_Header H
      ON H.PoNumber = Ln.PoNumber
    WHERE H.IsActive = 0
      AND Ln.IsActive = 1;

    COMMIT;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    SET @err = ERROR_MESSAGE();
  END CATCH;

  /* =========================
     Cleanup staging ALWAYS
     ========================= */
  BEGIN TRY
    TRUNCATE TABLE dbo.PO_Stg_Line;
    TRUNCATE TABLE dbo.PO_Stg_Header;
  END TRY
  BEGIN CATCH
    BEGIN TRY
      DELETE FROM dbo.PO_Stg_Line   WITH (TABLOCK);
      DELETE FROM dbo.PO_Stg_Header WITH (TABLOCK);
    END TRY
    BEGIN CATCH
      SET @err = COALESCE(@err + N'; ', N'') + N'Cleanup failed: ' + ERROR_MESSAGE();
    END CATCH;
  END CATCH;

  IF @err IS NOT NULL
    THROW 51001, @err, 1;
END
GO
