USE [WebappsDev]
GO

/****** Object:  StoredProcedure [dbo].[PO_Merge]    Script Date: 10/26/2025 9:21:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[PO_Merge]
AS
BEGIN
  /* ============================================================
     Purpose
     -------
     - Transactionally upsert (newest-wins) PO headers and lines
       from staging into operational tables.
     - Maintain soft-deletes for items missing from the current
       waiting snapshot.
     - Emit one outbox event per PO when newly waiting or reactivated.
     - Always clear staging after merge.
     Key behavior:
       * Newest-wins via CreatedAtUtc.
       * Soft-deletes only for headers with Status='W'.
       * Event type: PO_NEW_WAITING; dedupe against unprocessed rows.
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

    /* ========================= OUTBOX EMISSION =========================
       - Emits PO_NEW_WAITING for INSERTs or reactivations where Status='W'.
       - Payload: header summary + total active line count.
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

  /* ========================= Staging cleanup ========================= */
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

-- Ensures the TVP used to pass a set of PoNumbers exists (safe to run repeatedly)
IF TYPE_ID(N'dbo.PoNumberList') IS NULL
BEGIN
  EXEC(N'CREATE TYPE dbo.PoNumberList AS TABLE (
    PoNumber nvarchar(20) NOT NULL PRIMARY KEY
  );');
END
GO

/*==============================================================================
  Procedure: dbo.PO_BuildApprovalStages
  Purpose  : Build (or rebuild) approval stage rows for a set of POs.

  Inputs
  ------
  @PoNumbers : dbo.PoNumberList (table type with column PoNumber nvarchar(20))
               The specific PO numbers to build stages for.
  @Rebuild   : bit
               0 = merge/update stages in place (preserve prior A/D/S statuses)
               1 = delete existing stages for these POs and recompute from scratch

  Policy (dynamic thresholds)
  ---------------------------
  Reads the single active row from dbo.PO_ApprovalPolicy to obtain:
    - IndirectSplitAt : decimal(19,4) ex. 2000
        The indirect "pivot" amount. If IndAmt < this, include all
        Indirect tiers strictly below pivot and not above the amount.
        If IndAmt ≥ this, include all Indirect tiers at/above pivot up to IndAmt.

    - DirectMinAt     : decimal(19,4)
        Minimum Direct threshold to even consider Direct approvals. ex. 50000
        If DirAmt < DirectMinAt → no Direct stages at all.

    - DirectStartAt   : decimal(19,4)
        The first Direct tier that should appear if Direct approvals are needed
        (ex. 100000). We include all Direct tiers from DirectStartAt up to the
        calculated ceiling (see below). This intentionally excludes the "Buyer"
        tier even when DirAmt ≥ DirectMinAt.

  Indirect rule (cumulative)
  -------------------------
  - If IndAmt <  IndirectSplitAt: include tiers with Amount <  IndirectSplitAt
                                   and Amount ≤ IndAmt (i.e., below pivot only).
  - If IndAmt ≥ IndirectSplitAt:  include tiers with Amount ≥ IndirectSplitAt
                                   and Amount ≤ IndAmt (i.e., pivot and up).

  Direct rule (ceiling, excludes Buyer tier)
  ------------------------------------------
  - If DirAmt < DirectMinAt → no Direct stages.
  - Else find the smallest Direct tier Amount ≥ DirAmt (the "ceiling").
    If none exists (DirAmt beyond max ladder), use the max ladder Amount.
  - Then include all Direct tiers where Amount BETWEEN DirectStartAt AND ceiling.
    (ex. yields:
       50k–<100k  → LPM only
       100k–≤249,999.99 → LPM + FC
       etc., depending on your DoA ladder.)

  De-dupe (role appearing in both ladders)
  ----------------------------------------
  - If a RoleCode is required by both Indirect and Direct, we emit a single stage
    with Category = NULL (meaning "applies to both"), and we order such rows
    alongside the Indirect group.

  Approver resolution order
  -------------------------
  For each RoleCode of a PO:
    1) (RoleCode, HouseCode, BuyerCode) exact
    2) (RoleCode, HouseCode, '')        buyer-agnostic within house
    3) (RoleCode, 'GLOBAL',   '')       global default (fallback)

  Guardrails, idempotency, and behavior
  -------------------------------------
  - Throws if no active policy row is present.
  - Seeds missing PO_ApprovalChain rows as 'P'.
  - When @Rebuild=0, MERGE preserves A/D/S statuses already taken on a stage.
  - When @Rebuild=1, all stages for scope POs are re-created cleanly.

==============================================================================*/
CREATE OR ALTER PROCEDURE dbo.PO_BuildApprovalStages
  @PoNumbers dbo.PoNumberList READONLY,
  @Rebuild   bit = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRAN;

  /* 1) Load the active policy gates (must exist) */
  ;WITH ActivePolicy AS (
    SELECT TOP (1)
           IndirectSplitAt,
           DirectMinAt,
           DirectStartAt
    FROM dbo.PO_ApprovalPolicy
    WHERE IsActive = 1
    ORDER BY EffectiveDate DESC, UpdatedAtUtc DESC
  )
  SELECT *
  INTO #AP
  FROM ActivePolicy;

  IF NOT EXISTS (SELECT 1 FROM #AP)
  BEGIN
    ROLLBACK;
    THROW 52001, 'PO_BuildApprovalStages: No active row found in dbo.PO_ApprovalPolicy.', 1;
  END

  /* 2) Ensure a chain "container" exists for each scoped PO */
  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
  FROM dbo.PO_Header H
  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
  WHERE C.PoNumber IS NULL;

  /* 3) Optional hard rebuild of stages for these POs */
  IF @Rebuild = 1
  BEGIN
    DELETE S
    FROM dbo.PO_ApprovalStage S
    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
  END

  /* 4) Scratch table for computed stages prior to MERGE */
  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
  CREATE TABLE #Build(
    PoNumber       nvarchar(20)  NOT NULL,
    RoleCode       nvarchar(50)  NOT NULL,
    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
    ThresholdAmt   decimal(19,4) NULL,  -- ladder Amount used for ordering within group
    ApproverUserId nvarchar(100) NULL
  );

  /* 5) Scope: resolve PO attributes used in rule computation */
  ;WITH ScopePO AS (
    SELECT H.PoNumber,
           H.HouseCode,
           COALESCE(NULLIF(H.BuyerCode, ''), N'') AS BuyerCode,
           COALESCE(H.IndirectAmount, 0) AS IndAmt,
           COALESCE(H.DirectAmount,   0) AS DirAmt
    FROM dbo.PO_Header H
    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
    WHERE H.IsActive = 1 AND H.[Status] = 'W'
  ),

  /* 6) INDIRECT: apply policy pivot to include proper cumulative tiers */
  I_Ladder AS (
    SELECT S.PoNumber,
           D.[Level]            AS RoleCode,
           CAST('I' AS char(1)) AS Category,
           D.Amount             AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON (
           /* Below pivot: include tiers strictly below pivot and ≤ IndAmt */
           (S.IndAmt <  AP.IndirectSplitAt AND D.Amount <  AP.IndirectSplitAt AND D.Amount <= S.IndAmt)
           /* At/above pivot: include tiers at/above pivot up to IndAmt */
        OR (S.IndAmt >= AP.IndirectSplitAt AND D.Amount >= AP.IndirectSplitAt AND D.Amount <= S.IndAmt)
         )
  ),

  /* 7) DIRECT: compute ceiling tier if Direct is in-scope (DirAmt ≥ DirectMinAt) */
  D_Ceil AS (
    /* Find smallest tier >= DirAmt */
    SELECT S.PoNumber, MIN(D.Amount) AS CeilingAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= AP.DirectMinAt
     AND D.Amount >= S.DirAmt
    GROUP BY S.PoNumber

    UNION ALL

    /* If DirAmt exceeds defined ladder: use the maximum tier as ceiling */
    SELECT S.PoNumber, MAX(D.Amount) AS CeilingAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= AP.DirectMinAt
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.PO_DelegationOfAuthority_Direct_Material D2
      WHERE D2.Amount >= S.DirAmt
    )
    GROUP BY S.PoNumber
  ),

  /* 8) DIRECT: include tiers from DirectStartAt .. ceiling (excludes Buyer tier) */
  D_Ladder AS (
    SELECT S.PoNumber,
           D.[Level]            AS RoleCode,
           CAST('D' AS char(1)) AS Category,
           D.Amount             AS ThresholdAmt
    FROM ScopePO S
    JOIN D_Ceil C ON C.PoNumber = S.PoNumber
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON D.Amount BETWEEN AP.DirectStartAt AND C.CeilingAmt
  ),

  /* 9) Combine + order within each category; compute category precedence (I before D) */
  Ladders AS (
    SELECT PoNumber, RoleCode, Category, ThresholdAmt,
           ROW_NUMBER() OVER (PARTITION BY PoNumber, Category
                              ORDER BY ThresholdAmt ASC, RoleCode) AS OrdInCat,
           CASE WHEN Category = 'I' THEN 1 ELSE 2 END AS CatOrder
    FROM (
      SELECT * FROM I_Ladder
      UNION ALL
      SELECT * FROM D_Ladder
    ) X
  ),

  /* 10) De-dupe: if a RoleCode appears in both I and D, keep one (I takes precedence) */
  OrderedUnion AS (
    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
           ROW_NUMBER() OVER (
             PARTITION BY L.PoNumber, L.RoleCode
             ORDER BY L.CatOrder, L.OrdInCat
           ) AS KeepOne
    FROM Ladders L
  ),

  /* 11) Normalize category: if present in both ladders → Category = NULL ("both") */
  Kept AS (
    SELECT K.PoNumber,
           K.RoleCode,
           CASE
             WHEN EXISTS (
               SELECT 1
               FROM Ladders L2
               WHERE L2.PoNumber = K.PoNumber
                 AND L2.RoleCode = K.RoleCode
                 AND L2.Category <> K.Category
             ) THEN NULL
             ELSE K.Category
           END AS Category,
           K.ThresholdAmt
    FROM OrderedUnion K
    WHERE K.KeepOne = 1
  ),

  /* 12) Resolve approver identity with precedence:
         (Role, House, Buyer) → (Role, House, '') → (Role, 'GLOBAL', '') */
  ResolvedApprover AS (
    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
    FROM Kept K
    JOIN ScopePO S ON S.PoNumber = K.PoNumber
    OUTER APPLY (
      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = S.HouseCode
        AND BuyerCode = S.BuyerCode
      ORDER BY UpdatedAtUtc DESC
    ) D1
    OUTER APPLY (
      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = S.HouseCode
        AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D2
    OUTER APPLY (
      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = N'GLOBAL'
        AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D3
  )
  INSERT INTO #Build (PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
  FROM ResolvedApprover;

  /* 13) Final ordering across categories:
         - Category I first, then D
         - Category NULL (both) grouped with I
         - Within group: by ThresholdAmt then RoleCode
         - Assign 1..N sequence per PO
  */
  ;WITH SeqBase AS (
    SELECT B.*,
           CASE WHEN B.Category = 'I' THEN 1
                WHEN B.Category = 'D' THEN 2
                ELSE 1  -- “both” (NULL) sits with Indirect group
           END AS CatOrder
    FROM #Build B
  ),
  Ordered AS (
    SELECT PoNumber, RoleCode, Category, ApproverUserId,
           ROW_NUMBER() OVER (
             PARTITION BY PoNumber
             ORDER BY CatOrder, ISNULL(ThresholdAmt,0), RoleCode
           ) AS Seq
    FROM SeqBase
  )

  /* 14) MERGE into PO_ApprovalStage */
  MERGE dbo.PO_ApprovalStage AS T
  USING Ordered AS S
    ON T.PoNumber  = S.PoNumber
   AND T.[Sequence] = S.Seq
  WHEN MATCHED THEN
    UPDATE SET
      T.RoleCode       = S.RoleCode,
      T.ApproverUserId = S.ApproverUserId,
      T.Category       = S.Category,
      T.ThresholdFrom  = NULL,
      T.ThresholdTo    = NULL,
      T.[Status]       = CASE WHEN T.[Status] IN ('A','D','S') THEN T.[Status] ELSE 'P' END
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (PoNumber, [Sequence], RoleCode, ApproverUserId, Category, ThresholdFrom, ThresholdTo, [Status], DecidedAtUtc)
    VALUES (S.PoNumber, S.Seq, S.RoleCode, S.ApproverUserId, S.Category, NULL, NULL, 'P', NULL)
  WHEN NOT MATCHED BY SOURCE
       AND T.PoNumber IN (SELECT PoNumber FROM @PoNumbers)
  THEN
    DELETE;

  COMMIT;
END
GO

-- =============================================================================
-- Procedure: dbo.PO_IngestAndBuild
-- Purpose  : Orchestrator that:
--            1) Executes stored procedure dbo.PO_Merge to upsert headers/lines and
--               emit PO_NEW_WAITING outbox rows.
--            2) Collects newly waiting PoNumbers from the outbox snapshot.
--            3) Builds approval stages for just those POs.
-- Notes    : Leaves Outbox.ProcessedAtUtc untouched (the async worker owns it).
-- =============================================================================
CREATE OR ALTER PROCEDURE dbo.PO_IngestAndBuild
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @New dbo.PoNumberList;

  BEGIN TRAN;

  EXEC dbo.PO_Merge;

  INSERT INTO @New(PoNumber)
  SELECT DISTINCT O.PoNumber
  FROM dbo.PO_ApprovalOutbox O
  JOIN dbo.PO_Header H ON H.PoNumber = O.PoNumber
  WHERE O.EventType = 'PO_NEW_WAITING'
    AND O.ProcessedAtUtc IS NULL
    AND H.IsActive = 1
    AND H.[Status] = 'W';

  COMMIT;

  IF EXISTS (SELECT 1 FROM @New)
  BEGIN
    EXEC dbo.PO_BuildApprovalStages @PoNumbers = @New, @Rebuild = 0;
  END
END
GO
