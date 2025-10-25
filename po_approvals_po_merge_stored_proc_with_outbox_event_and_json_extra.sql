USE [WebappsDev];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

CREATE OR ALTER PROCEDURE dbo.PO_Merge
AS
BEGIN
  /* ============================================================
     Purpose
     -------
     - Transactionally upsert (newest-wins) PO headers and lines
       from staging into operational tables.
     - Maintain soft-deletes for items missing from the *current
       waiting* snapshot.
     - Emit one outbox event per PO when newly waiting or reactivated.
     - Always clear staging after merge.
     Key behavior retained:
       * Newest-wins via CreatedAtUtc.
       * Soft-deletes only for headers with Status='W'.
       * Event type: PO_NEW_WAITING; dedupe against unprocessed rows.
     Change in this revision:
       * Outbox emission occurs AFTER the line MERGE.
       * Payload drops line preview; includes lineCount only.
       * Outbox row includes DirectAmount/IndirectAmount columns.
     ============================================================ */

  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @err nvarchar(4000) = NULL;

  BEGIN TRY
    BEGIN TRAN;

    /* ========================= HEADER MERGE (newest-wins) ========================= */
    DECLARE @HdrChanges TABLE
    (
      PoNumber     nvarchar(20),
      Action       nvarchar(10),  -- 'INSERT' / 'UPDATE'
      WasActive    bit,           -- prior IsActive (deleted)
      IsActiveNow  bit            -- new IsActive (inserted)
    );

    ;WITH H AS (
      SELECT *
      FROM (
        SELECT
          PoNumber, PoDate, VendorNumber, VendorName, VendorAddr1, VendorAddr2, VendorAddr3,
          VendorState, VendorPostalCode, BuyerCode, BuyerName, HouseCode,
          DirectAmount, IndirectAmount, CreatedAtUtc,
          ROW_NUMBER() OVER (PARTITION BY PoNumber ORDER BY CreatedAtUtc DESC) rn
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

    /* ========================= LINE MERGE (newest-wins) + FK ========================= */
    ;WITH L AS (
      SELECT *
      FROM (
        SELECT
          PoNumber, LineNumber, HouseCode, ItemNumber, ItemDescription, ItemShortDescription,
          QuantityOrdered, OrderUom, UnitCost, ExtendedCost, RequiredDate, GlAccount, CreatedAtUtc,
          ROW_NUMBER() OVER (PARTITION BY PoNumber, LineNumber ORDER BY CreatedAtUtc DESC) rn
        FROM dbo.PO_Stg_Line
        WHERE PoNumber IS NOT NULL AND LineNumber IS NOT NULL
      ) x
      WHERE rn = 1
    ),
    LS AS (
      SELECT
        L.*,
        H.PoHeaderId AS PoHeaderIdFk
      FROM L
      JOIN dbo.PO_Header H ON H.PoNumber = L.PoNumber
    )
    MERGE dbo.PO_Line AS tgt
    USING LS AS src
      ON tgt.PoNumber = src.PoNumber
     AND tgt.LineNumber = src.LineNumber
    WHEN MATCHED THEN
      UPDATE SET
         PoHeaderId           = COALESCE(tgt.PoHeaderId, src.PoHeaderIdFk),
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
    WHEN NOT MATCHED BY SOURCE
         AND EXISTS (SELECT 1 FROM dbo.PO_Header h2 WHERE h2.PoNumber = tgt.PoNumber AND h2.[Status] = 'W')
    THEN
      UPDATE SET
        IsActive           = 0,
        DeactivatedAtUtc   = SYSUTCDATETIME(),
        DeactivatedBy      = 'PO_Merge',
        DeactivationReason = 'Line absent from PRMS waiting snapshot'
    ;

    /* ========================= Cascade soft-delete ========================= */
    UPDATE Ln
      SET IsActive            = 0,
          DeactivatedAtUtc    = COALESCE(Ln.DeactivatedAtUtc, SYSUTCDATETIME()),
          DeactivatedBy       = COALESCE(Ln.DeactivatedBy, 'PO_Merge'),
          DeactivationReason  = COALESCE(Ln.DeactivationReason, 'Header soft-deleted')
    FROM dbo.PO_Line Ln
    JOIN dbo.PO_Header H
      ON H.PoNumber = Ln.PoNumber
    WHERE H.IsActive = 0 AND Ln.IsActive = 1;

    /* ========================= OUTBOX EMISSION (after line merge) =========================
       - Emits PO_NEW_WAITING for INSERTs or reactivations where Status='W'.
       - Payload: header summary + total active line count (no line preview).
       - Columns DirectAmount / IndirectAmount set on the row.
       - Dedupe: skip if an unprocessed identical event exists.
    ======================================================================== */
    ;WITH H2 AS (
      SELECT h.*
      FROM @HdrChanges hc
      JOIN dbo.PO_Header h ON h.PoNumber = hc.PoNumber
      WHERE (hc.Action = 'INSERT' OR (hc.WasActive = 0 AND hc.IsActiveNow = 1))
        AND h.[Status] = 'W'
    ),
    P AS (
      SELECT
        H2.PoNumber,
        (SELECT COUNT(*) FROM dbo.PO_Line l WHERE l.PoNumber = H2.PoNumber AND l.IsActive = 1) AS LineCount,
        (SELECT
            H2.PoNumber       AS poNumber,
            H2.PoDate         AS poDate,
            H2.VendorNumber   AS vendorNumber,
            H2.VendorName     AS vendorName,
            H2.BuyerCode      AS buyerCode,
            H2.BuyerName      AS buyerName,
            H2.HouseCode      AS houseCode,
            H2.DirectAmount   AS directAmount,
            H2.IndirectAmount AS indirectAmount,
            (COALESCE(H2.DirectAmount,0)+COALESCE(H2.IndirectAmount,0)) AS total,
            1                 AS schemaVersion
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS HeaderJson
      FROM H2
    )
    INSERT INTO dbo.PO_ApprovalOutbox
      (EventType, PoNumber, OccurredAtUtc, DirectAmount, IndirectAmount, PayloadJson)
    SELECT
      'PO_NEW_WAITING',
      H2.PoNumber,
      SYSUTCDATETIME(),
      H2.DirectAmount,
      H2.IndirectAmount,
      COALESCE(
        (
          SELECT
            JSON_VALUE(P.HeaderJson,'$.poNumber')         AS poNumber,
            JSON_VALUE(P.HeaderJson,'$.poDate')           AS poDate,
            JSON_VALUE(P.HeaderJson,'$.vendorNumber')     AS vendorNumber,
            JSON_VALUE(P.HeaderJson,'$.vendorName')       AS vendorName,
            JSON_VALUE(P.HeaderJson,'$.buyerCode')        AS buyerCode,
            JSON_VALUE(P.HeaderJson,'$.buyerName')        AS buyerName,
            JSON_VALUE(P.HeaderJson,'$.houseCode')        AS houseCode,
            JSON_VALUE(P.HeaderJson,'$.directAmount')     AS directAmount,
            JSON_VALUE(P.HeaderJson,'$.indirectAmount')   AS indirectAmount,
            JSON_VALUE(P.HeaderJson,'$.total')            AS total,
            JSON_VALUE(P.HeaderJson,'$.schemaVersion')    AS schemaVersion,
            P.LineCount                                   AS lineCount
          FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        CONCAT(N'{"poNumber":"', H2.PoNumber, N'","schemaVersion":1}')
      ) AS PayloadJson
    FROM H2
    JOIN P ON P.PoNumber = H2.PoNumber
    LEFT JOIN dbo.PO_ApprovalOutbox o
      ON o.EventType = 'PO_NEW_WAITING'
     AND o.PoNumber = H2.PoNumber
     AND o.ProcessedAtUtc IS NULL
    WHERE o.OutboxId IS NULL
    ;

    COMMIT;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    SET @err = ERROR_MESSAGE();
  END CATCH;

  /* ========================= Staging cleanup (always) ========================= */
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
