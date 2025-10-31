-- Ensure table type exists (safe to run repeatedly)
IF TYPE_ID(N'dbo.PoNumberList') IS NULL
BEGIN
  EXEC(N'CREATE TYPE dbo.PoNumberList AS TABLE (
    PoNumber nvarchar(20) NOT NULL PRIMARY KEY
  );');
END
GO

CREATE OR ALTER PROCEDURE dbo.PO_BuildApprovalStages
  @PoNumbers dbo.PoNumberList READONLY,
  @Rebuild   bit = 0  -- when 1, wipe existing stages for supplied POs before rebuilding
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  /*
    Policy implemented:
      - CUMULATIVE per ladder.
      - Order: Indirect ladder first (ascending threshold), then Direct ladder (ascending).
      - If a role appears in both ladders for the PO, keep a single stage with Category = NULL.
      - Resolve approver user via PO_ApproverDirectory with the following precedence:
            (RoleCode, HouseCode, BuyerCode)
        then (RoleCode, HouseCode, 'GLOBAL' buyer '')
        then (RoleCode, 'GLOBAL', '')
        If no match, stage is kept with ApproverUserId = NULL.
  */

  BEGIN TRAN;

  --------------------------------------------------------------------------------
  -- Ensure a chain row exists for each PO in scope (seed if missing)
  --------------------------------------------------------------------------------
  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
  FROM dbo.PO_Header H
  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
  WHERE C.PoNumber IS NULL;

  IF @Rebuild = 1
  BEGIN
    DELETE S
    FROM dbo.PO_ApprovalStage S
    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
  END

  --------------------------------------------------------------------------------
  -- Build stages into a temp table, then MERGE into PO_ApprovalStage
  --------------------------------------------------------------------------------
  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
  CREATE TABLE #Build(
    PoNumber       nvarchar(20) NOT NULL,
    RoleCode       nvarchar(40) NOT NULL,
    Category       char(1) NULL,           -- 'I','D', or NULL (both)
    ThresholdAmt   decimal(19,4) NULL,
    ApproverUserId nvarchar(100) NULL
  );

  ;WITH ScopePO AS (
    SELECT H.PoNumber, H.HouseCode, COALESCE(NULLIF(H.BuyerCode,''), N'') AS BuyerCode,
           COALESCE(H.IndirectAmount, 0) AS IndAmt,
           COALESCE(H.DirectAmount,   0) AS DirAmt
    FROM dbo.PO_Header H
    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
    WHERE H.IsActive = 1 AND H.[Status] IN ('W')  -- waiting
  ),
  I_Ladder AS (
    -- Indirect cumulative: all entries with Amount <= IndAmt
    SELECT S.PoNumber,
           D.[Level]   AS RoleCode,
           CAST('I' AS char(1)) AS Category,
           D.Amount    AS ThresholdAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON D.Amount <= S.IndAmt
  ),
  D_Ladder AS (
    -- Direct cumulative: all entries with Amount <= DirAmt
    SELECT S.PoNumber,
           D.[Level]   AS RoleCode,
           CAST('D' AS char(1)) AS Category,
           D.Amount    AS ThresholdAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON D.Amount <= S.DirAmt
  ),
  Ladders AS (
    -- Order within each ladder by ascending threshold (seniority)
    SELECT PoNumber, RoleCode, Category, ThresholdAmt,
           ROW_NUMBER() OVER (PARTITION BY PoNumber, Category ORDER BY ThresholdAmt ASC, RoleCode) AS OrdInCat,
           CASE WHEN Category = 'I' THEN 1 ELSE 2 END AS CatOrder
    FROM (
      SELECT * FROM I_Ladder
      UNION ALL
      SELECT * FROM D_Ladder
    ) X
  ),
  OrderedUnion AS (
    -- Concatenate I then D, then dedupe by RoleCode per PoNumber
    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
           ROW_NUMBER() OVER (PARTITION BY L.PoNumber, L.RoleCode
                              ORDER BY L.CatOrder, L.OrdInCat) AS KeepOne
    FROM Ladders L
  ),
  Kept AS (
    SELECT K.PoNumber, K.RoleCode,
           -- If role exists in both ladders, Category becomes NULL (meaning “both”)
           CASE
             WHEN EXISTS (
               SELECT 1 FROM Ladders L2
               WHERE L2.PoNumber = K.PoNumber AND L2.RoleCode = K.RoleCode AND L2.Category <> K.Category
             ) THEN NULL
             ELSE K.Category
           END AS Category,
           K.ThresholdAmt
    FROM OrderedUnion K
    WHERE K.KeepOne = 1
  ),
  ResolvedApprover AS (
    -- Resolve ApproverUserId using directory precedence
    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
    FROM Kept K
    JOIN ScopePO S ON S.PoNumber = K.PoNumber
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = S.HouseCode
        AND BuyerCode = S.BuyerCode
      ORDER BY UpdatedAtUtc DESC
    ) D1
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = S.HouseCode
        AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D2
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1
        AND RoleCode = K.RoleCode
        AND HouseCode = N'GLOBAL'
        AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D3
  )
  INSERT INTO #Build(PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
  FROM ResolvedApprover;

  -- Assign sequences: keep I-first then D, within each ascending threshold, after dedupe above
  ;WITH SeqBase AS (
    SELECT B.*,
           CASE WHEN B.Category = 'I' THEN 1
                WHEN B.Category = 'D' THEN 2
                ELSE 3 -- “both” goes after I/D that fed it; you can set 0 to push it to the very top if desired
           END AS CatOrder
    FROM #Build B
  ),
  Ordered AS (
    SELECT PoNumber, RoleCode, Category, ApproverUserId,
           ROW_NUMBER() OVER (
             PARTITION BY PoNumber
             ORDER BY CatOrder, ISNULL(ThresholdAmt, 0), RoleCode
           ) AS Seq
    FROM SeqBase
  )
  MERGE dbo.PO_ApprovalStage AS T
  USING Ordered AS S
    ON T.PoNumber = S.PoNumber
   AND T.[Sequence] = S.Seq
  WHEN MATCHED THEN
    UPDATE SET
      T.RoleCode       = S.RoleCode,
      T.ApproverUserId = S.ApproverUserId,
      T.Category       = S.Category,
      T.ThresholdFrom  = NULL,            -- not used in this build
      T.ThresholdTo    = NULL,            -- not used in this build
      T.[Status]       = CASE WHEN T.[Status] IN ('A','D','S') THEN T.[Status] ELSE 'P' END
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (PoNumber, [Sequence], RoleCode, ApproverUserId, Category, ThresholdFrom, ThresholdTo, [Status], DecidedAtUtc)
    VALUES (S.PoNumber, S.Seq, S.RoleCode, S.ApproverUserId, S.Category, NULL, NULL, 'P', NULL)
  WHEN NOT MATCHED BY SOURCE
       AND T.PoNumber IN (SELECT PoNumber FROM @PoNumbers)
  THEN
    DELETE
  ;

  COMMIT;
END

