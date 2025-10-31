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
  @Rebuild   bit = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;
 
  BEGIN TRAN;
 
  /* Seed chain rows if missing */
  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
  FROM dbo.PO_Header H
  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
  WHERE C.PoNumber IS NULL;
 
  /* Optional rebuild */
  IF @Rebuild = 1
  BEGIN
    DELETE S
    FROM dbo.PO_ApprovalStage S
    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
  END
 
  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
  CREATE TABLE #Build(
    PoNumber       nvarchar(20)  NOT NULL,
    RoleCode       nvarchar(50)  NOT NULL,
    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
    ThresholdAmt   decimal(19,4) NULL,
    ApproverUserId nvarchar(100) NULL
  );
 
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
 
  /* ---------- INDIRECT: your rules ---------- */
  I_Ladder AS (
    SELECT S.PoNumber,
           D.[Level]               AS RoleCode,
           CAST('I' AS char(1))    AS Category,
           D.Amount                AS ThresholdAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON (
           (S.IndAmt < 2000  AND D.Amount < 2000  AND D.Amount <= S.IndAmt)
        OR (S.IndAmt >= 2000 AND D.Amount >= 2000 AND D.Amount <= S.IndAmt)
         )
  ),
 
  /* ---------- DIRECT: cumulative up to ceiling, excluding Buyer ---------- */
  D_Ceil AS (
    -- If DirAmt >= 50k, find ceiling; otherwise no row -> no Direct stages
    SELECT S.PoNumber, MIN(D.Amount) AS CeilingAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= 50000 AND D.Amount >= S.DirAmt
    GROUP BY S.PoNumber
 
    UNION ALL
 
    -- Above max tier → use max Amount
    SELECT S.PoNumber, MAX(D.Amount) AS CeilingAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= 50000
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.PO_DelegationOfAuthority_Direct_Material D2
      WHERE D2.Amount >= S.DirAmt
    )
    GROUP BY S.PoNumber
  ),
  D_Ladder AS (
    /* Include ALL direct tiers from LPM upward up to ceiling.
       - Excludes Buyer (50,000)
       - 50k–<100k → Ceiling=100k → LPM only
       - 100k–≤249,999.99 → LPM + FC
       - etc. */
    SELECT S.PoNumber,
           D.[Level]               AS RoleCode,
           CAST('D' AS char(1))    AS Category,
           D.Amount                AS ThresholdAmt
    FROM ScopePO S
    JOIN D_Ceil C ON C.PoNumber = S.PoNumber
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON D.Amount BETWEEN 100000 AND C.CeilingAmt
  ),
 
  /* ---------- Order & de-dupe ---------- */
  Ladders AS (
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
    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
           ROW_NUMBER() OVER (
             PARTITION BY L.PoNumber, L.RoleCode
             ORDER BY L.CatOrder, L.OrdInCat
           ) AS KeepOne
    FROM Ladders L
  ),
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
             ) THEN NULL   -- appears in both ladders → show once
             ELSE K.Category
           END AS Category,
           K.ThresholdAmt
    FROM OrderedUnion K
    WHERE K.KeepOne = 1
  ),
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
 
  ;WITH SeqBase AS (
    SELECT B.*,
           CASE WHEN B.Category = 'I' THEN 1
                WHEN B.Category = 'D' THEN 2
                ELSE 1  -- Category=NULL (both) groups with Indirect
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
  MERGE dbo.PO_ApprovalStage AS T
  USING Ordered AS S
    ON T.PoNumber = S.PoNumber
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


--CREATE OR ALTER PROCEDURE dbo.PO_BuildApprovalStages
--  @PoNumbers dbo.PoNumberList READONLY,
--  @Rebuild   bit = 0
--AS
--BEGIN
--  SET NOCOUNT ON;
--  SET XACT_ABORT ON;
 
--  BEGIN TRAN;
 
--  /* Ensure chain row exists for each PO in scope */
--  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
--  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
--  FROM dbo.PO_Header H
--  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
--  WHERE C.PoNumber IS NULL;
 
--  /* Optional full rebuild of stages for supplied POs */
--  IF @Rebuild = 1
--  BEGIN
--    DELETE S
--    FROM dbo.PO_ApprovalStage S
--    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
--  END
 
--  /* Build into temp table */
--  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
--  CREATE TABLE #Build(
--    PoNumber       nvarchar(20)  NOT NULL,
--    RoleCode       nvarchar(50)  NOT NULL,
--    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
--    ThresholdAmt   decimal(19,4) NULL,
--    ApproverUserId nvarchar(100) NULL
--  );
 
--  ;WITH ScopePO AS (
--    SELECT H.PoNumber,
--           H.HouseCode,
--           COALESCE(NULLIF(H.BuyerCode, ''), N'') AS BuyerCode,
--           COALESCE(H.IndirectAmount, 0) AS IndAmt,
--           COALESCE(H.DirectAmount,   0) AS DirAmt
--    FROM dbo.PO_Header H
--    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--    WHERE H.IsActive = 1 AND H.[Status] = 'W'
--  ),
 
--  /* ---------------- Indirect per your rules ----------------
--     < 2000 : tiers with Amount <= IndAmt AND Amount < 2000
--     ≥ 2000 : tiers with 2000 <= Amount <= IndAmt
--  */
--  I_Ladder AS (
--    SELECT S.PoNumber,
--           D.[Level] AS RoleCode,
--           CAST('I' AS char(1)) AS Category,
--           D.Amount AS ThresholdAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
--      ON (
--           (S.IndAmt < 2000  AND D.Amount < 2000  AND D.Amount <= S.IndAmt)
--        OR (S.IndAmt >= 2000 AND D.Amount >= 2000 AND D.Amount <= S.IndAmt)
--         )
--  ),
 
--  /* ---------------- Direct fixed to 'ceiling only' ----------------
--     < 50000 : no Direct stage.
--     ≥ 50000 : pick ONE tier:
--       - the smallest Amount >= DirAmt (ceiling), or
--       - if none (DirAmt above max), the max Amount.
--  */
--  D_Ceil AS (
--    -- ceiling exists
--    SELECT S.PoNumber, MIN(D.Amount) AS CeilingAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON S.DirAmt >= 50000 AND D.Amount >= S.DirAmt
--    GROUP BY S.PoNumber
 
--    UNION ALL
 
--    -- fallback: DirAmt above max tier → choose max tier
--    SELECT S.PoNumber, MAX(D.Amount) AS CeilingAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON S.DirAmt >= 50000
--    WHERE NOT EXISTS (
--      SELECT 1
--      FROM dbo.PO_DelegationOfAuthority_Direct_Material D2
--      WHERE D2.Amount >= S.DirAmt
--    )
--    GROUP BY S.PoNumber
--  ),
--  D_Ladder AS (
--    SELECT S.PoNumber,
--           D.[Level] AS RoleCode,
--           CAST('D' AS char(1)) AS Category,
--           D.Amount AS ThresholdAmt
--    FROM ScopePO S
--    JOIN D_Ceil C ON C.PoNumber = S.PoNumber
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON D.Amount = C.CeilingAmt
--  ),
 
--  /* Order: Indirect (ascending) then Direct (ascending) */
--  Ladders AS (
--    SELECT PoNumber, RoleCode, Category, ThresholdAmt,
--           ROW_NUMBER() OVER (PARTITION BY PoNumber, Category ORDER BY ThresholdAmt ASC, RoleCode) AS OrdInCat,
--           CASE WHEN Category = 'I' THEN 1 ELSE 2 END AS CatOrder
--    FROM (
--      SELECT * FROM I_Ladder
--      UNION ALL
--      SELECT * FROM D_Ladder
--    ) X
--  ),
 
--  /* De-duplicate roles in both ladders */
--  OrderedUnion AS (
--    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
--           ROW_NUMBER() OVER (
--             PARTITION BY L.PoNumber, L.RoleCode
--             ORDER BY L.CatOrder, L.OrdInCat
--           ) AS KeepOne
--    FROM Ladders L
--  ),
--  Kept AS (
--    SELECT K.PoNumber,
--           K.RoleCode,
--           CASE
--             WHEN EXISTS (
--               SELECT 1
--               FROM Ladders L2
--               WHERE L2.PoNumber = K.PoNumber
--                 AND L2.RoleCode = K.RoleCode
--                 AND L2.Category <> K.Category
--             ) THEN NULL  -- role appears in both → show once with Category=NULL
--             ELSE K.Category
--           END AS Category,
--           K.ThresholdAmt
--    FROM OrderedUnion K
--    WHERE K.KeepOne = 1
--  ),
 
--  /* Resolve approver by precedence */
--  ResolvedApprover AS (
--    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
--           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
--    FROM Kept K
--    JOIN ScopePO S ON S.PoNumber = K.PoNumber
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = S.BuyerCode
--      ORDER BY UpdatedAtUtc DESC
--    ) D1
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D2
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = N'GLOBAL'
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D3
--  )
--  INSERT INTO #Build (PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
--  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
--  FROM ResolvedApprover;
 
--  /* Sequence: I first, then D */
--  ;WITH SeqBase AS (
--    SELECT B.*,
--           CASE WHEN B.Category = 'I' THEN 1
--                WHEN B.Category = 'D' THEN 2
--                ELSE 1  -- Category=NULL (both) groups with I
--           END AS CatOrder
--    FROM #Build B
--  ),
--  Ordered AS (
--    SELECT PoNumber, RoleCode, Category, ApproverUserId,
--           ROW_NUMBER() OVER (
--             PARTITION BY PoNumber
--             ORDER BY CatOrder, ISNULL(ThresholdAmt, 0), RoleCode
--           ) AS Seq
--    FROM SeqBase
--  )
--  MERGE dbo.PO_ApprovalStage AS T
--  USING Ordered AS S
--    ON T.PoNumber = S.PoNumber
--   AND T.[Sequence] = S.Seq
--  WHEN MATCHED THEN
--    UPDATE SET
--      T.RoleCode       = S.RoleCode,
--      T.ApproverUserId = S.ApproverUserId,
--      T.Category       = S.Category,
--      T.ThresholdFrom  = NULL,
--      T.ThresholdTo    = NULL,
--      T.[Status]       = CASE WHEN T.[Status] IN ('A','D','S') THEN T.[Status] ELSE 'P' END
--  WHEN NOT MATCHED BY TARGET THEN
--    INSERT (PoNumber, [Sequence], RoleCode, ApproverUserId, Category, ThresholdFrom, ThresholdTo, [Status], DecidedAtUtc)
--    VALUES (S.PoNumber, S.Seq, S.RoleCode, S.ApproverUserId, S.Category, NULL, NULL, 'P', NULL)
--  WHEN NOT MATCHED BY SOURCE
--       AND T.PoNumber IN (SELECT PoNumber FROM @PoNumbers)
--  THEN
--    DELETE;
 
--  COMMIT;
--END
--GO




--CREATE OR ALTER PROCEDURE dbo.PO_BuildApprovalStages
--  @PoNumbers dbo.PoNumberList READONLY,
--  @Rebuild   bit = 0  -- when 1, wipe existing stages for supplied POs before rebuilding
--AS
--BEGIN
--  SET NOCOUNT ON;
--  SET XACT_ABORT ON;
 
--  BEGIN TRAN;
 
--  /* Ensure chain row exists for each PO in scope */
--  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
--  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
--  FROM dbo.PO_Header H
--  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
--  WHERE C.PoNumber IS NULL;
 
--  /* Optional full rebuild of stages for supplied POs */
--  IF @Rebuild = 1
--  BEGIN
--    DELETE S
--    FROM dbo.PO_ApprovalStage S
--    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
--  END
 
--  /* Build into temp table (RoleCode width = 40 to match schema) */
--  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
--  CREATE TABLE #Build(
--    PoNumber       nvarchar(20)  NOT NULL,
--    RoleCode       nvarchar(50)  NOT NULL,
--    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
--    ThresholdAmt   decimal(19,4) NULL,
--    ApproverUserId nvarchar(100) NULL
--  );
 
--  ;WITH ScopePO AS (
--    SELECT H.PoNumber,
--           H.HouseCode,
--           COALESCE(NULLIF(H.BuyerCode, ''), N'') AS BuyerCode,
--           COALESCE(H.IndirectAmount, 0) AS IndAmt,
--           COALESCE(H.DirectAmount,   0) AS DirAmt
--    FROM dbo.PO_Header H
--    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--    WHERE H.IsActive = 1 AND H.[Status] = 'W'
--  ),
 
--  /* -------- Indirect selection per rules --------
--     - If IndAmt < 2000: include tiers with Amount <= IndAmt AND Amount < 2000.
--     - If IndAmt >= 2000: include tiers with 2000 <= Amount <= IndAmt.
--  */
--  I_Ladder AS (
--    SELECT S.PoNumber,
--           D.[Level] AS RoleCode,
--           CAST('I' AS char(1)) AS Category,
--           D.Amount AS ThresholdAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
--      ON (
--           (S.IndAmt < 2000  AND D.Amount < 2000  AND D.Amount <= S.IndAmt)
--        OR (S.IndAmt >= 2000 AND D.Amount >= 2000 AND D.Amount <= S.IndAmt)
--         )
--  ),
 
--  /* -------- Direct selection per rules --------
--     - If DirAmt < 50000: include nothing.
--     - Else include tiers with 50000 <= Amount <= DirAmt.
--  */
--  D_Ladder AS (
--    SELECT S.PoNumber,
--           D.[Level] AS RoleCode,
--           CAST('D' AS char(1)) AS Category,
--           D.Amount AS ThresholdAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON (S.DirAmt >= 50000 AND D.Amount >= 50000 AND D.Amount <= S.DirAmt)
--  ),
 
--  /* Order: Indirect (ascending), then Direct (ascending) */
--  Ladders AS (
--    SELECT PoNumber, RoleCode, Category, ThresholdAmt,
--           ROW_NUMBER() OVER (PARTITION BY PoNumber, Category ORDER BY ThresholdAmt ASC, RoleCode) AS OrdInCat,
--           CASE WHEN Category = 'I' THEN 1 ELSE 2 END AS CatOrder
--    FROM (
--      SELECT * FROM I_Ladder
--      UNION ALL
--      SELECT * FROM D_Ladder
--    ) X
--  ),
 
--  /* De-duplicate roles that appear in both ladders for the PO.
--     Keep the first occurrence by CatOrder (I before D) and OrdInCat. */
--  OrderedUnion AS (
--    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
--           ROW_NUMBER() OVER (
--             PARTITION BY L.PoNumber, L.RoleCode
--             ORDER BY L.CatOrder, L.OrdInCat
--           ) AS KeepOne
--    FROM Ladders L
--  ),
--  Kept AS (
--    SELECT K.PoNumber,
--           K.RoleCode,
--           CASE
--             WHEN EXISTS (
--               SELECT 1
--               FROM Ladders L2
--               WHERE L2.PoNumber = K.PoNumber
--                 AND L2.RoleCode = K.RoleCode
--                 AND L2.Category <> K.Category
--             ) THEN NULL  -- appears in both ladders → treat as “both”
--             ELSE K.Category
--           END AS Category,
--           K.ThresholdAmt
--    FROM OrderedUnion K
--    WHERE K.KeepOne = 1
--  ),
 
--  /* Resolve approver by precedence: (RoleCode, HouseCode, BuyerCode) → (RoleCode, HouseCode, '') → (RoleCode, 'GLOBAL', '') */
--  ResolvedApprover AS (
--    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
--           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
--    FROM Kept K
--    JOIN ScopePO S ON S.PoNumber = K.PoNumber
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = S.BuyerCode
--      ORDER BY UpdatedAtUtc DESC
--    ) D1
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D2
--    OUTER APPLY (
--      SELECT TOP (1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = N'GLOBAL'
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D3
--  )
--  INSERT INTO #Build (PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
--  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
--  FROM ResolvedApprover;
 
--  /* Sequence: Indirect first, then Direct; both ascending by ThresholdAmt, then RoleCode */
--  ;WITH SeqBase AS (
--    SELECT B.*,
--           CASE WHEN B.Category = 'I' THEN 1
--                WHEN B.Category = 'D' THEN 2
--                ELSE 1  -- if Category=NULL (“both”), keep with Indirect block
--           END AS CatOrder
--    FROM #Build B
--  ),
--  Ordered AS (
--    SELECT PoNumber, RoleCode, Category, ApproverUserId,
--           ROW_NUMBER() OVER (
--             PARTITION BY PoNumber
--             ORDER BY CatOrder, ISNULL(ThresholdAmt, 0), RoleCode
--           ) AS Seq
--    FROM SeqBase
--  )
--  MERGE dbo.PO_ApprovalStage AS T
--  USING Ordered AS S
--    ON T.PoNumber = S.PoNumber
--   AND T.[Sequence] = S.Seq
--  WHEN MATCHED THEN
--    UPDATE SET
--      T.RoleCode       = S.RoleCode,
--      T.ApproverUserId = S.ApproverUserId,
--      T.Category       = S.Category,
--      T.ThresholdFrom  = NULL,
--      T.ThresholdTo    = NULL,
--      T.[Status]       = CASE WHEN T.[Status] IN ('A','D','S') THEN T.[Status] ELSE 'P' END
--  WHEN NOT MATCHED BY TARGET THEN
--    INSERT (PoNumber, [Sequence], RoleCode, ApproverUserId, Category, ThresholdFrom, ThresholdTo, [Status], DecidedAtUtc)
--    VALUES (S.PoNumber, S.Seq, S.RoleCode, S.ApproverUserId, S.Category, NULL, NULL, 'P', NULL)
--  WHEN NOT MATCHED BY SOURCE
--       AND T.PoNumber IN (SELECT PoNumber FROM @PoNumbers)
--  THEN
--    DELETE;
 
--  COMMIT;
--END
--GO


--CREATE OR ALTER PROCEDURE dbo.PO_BuildApprovalStages
--  @PoNumbers dbo.PoNumberList READONLY,
--  @Rebuild   bit = 1  -- when 1, wipe existing stages for supplied POs before rebuilding
--AS
--BEGIN
--  SET NOCOUNT ON;
--  SET XACT_ABORT ON;
 
--  /*
--    PURPOSE
--    -------
--    Build approval stages for supplied POs using:
--      • Indirect ladder = cumulative up to (but NOT including) the next breakpoint above IndirectAmount.
--      • Direct ladder   = single “ceiling” tier (the smallest tier ≥ DirectAmount; if above max, take the max tier).
--      • Ordering        = all Indirect stages first (ascending threshold), then the Direct stage.
--      • Dedupe          = if a Role appears in both ladders, keep a single stage (Category = NULL).
 
--    This corrects the case where a PO with Indirect < 2,000 was erroneously including the “Regional VP/GM”
--    tier (2,000) as an indirect stage.
--  */
 
--  BEGIN TRAN;
 
--  --------------------------------------------------------------------
--  -- Seed chain rows for any missing POs in scope
--  --------------------------------------------------------------------
--  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
--  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
--  FROM dbo.PO_Header H
--  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
--  WHERE C.PoNumber IS NULL;
 
--  IF @Rebuild = 1
--  BEGIN
--    DELETE S
--    FROM dbo.PO_ApprovalStage S
--    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
--  END
 
--  --------------------------------------------------------------------
--  -- Build into temp table; keep column sizes aligned to target table
--  --------------------------------------------------------------------
--  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
--  CREATE TABLE #Build(
--    PoNumber       nvarchar(20)  NOT NULL,
--    RoleCode       nvarchar(50)  NOT NULL,
--    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
--    ThresholdAmt   decimal(19,4) NULL,
--    ApproverUserId nvarchar(100) NULL
--  );
 
--  ;WITH ScopePO AS (
--    SELECT H.PoNumber,
--           H.HouseCode,
--           COALESCE(NULLIF(H.BuyerCode,''), N'') AS BuyerCode,
--           COALESCE(H.IndirectAmount, 0) AS IndAmt,
--           COALESCE(H.DirectAmount,   0) AS DirAmt
--    FROM dbo.PO_Header H
--    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
--    WHERE H.IsActive = 1 AND H.[Status] = 'W'  -- waiting
--  ),
 
--  /* ------------------- INDIRECT: cumulative BELOW next breakpoint ------------------- */
--  -- Next indirect breakpoint above the PO's IndAmt
--  I_Limits AS (
--    SELECT
--      S.PoNumber,
--      /* next tier strictly greater than IndAmt (the “ceiling” NOT to include) */
--      (SELECT MIN(D.Amount)
--         FROM dbo.PO_DelegationOfAuthority_Indirect_Expense D
--        WHERE D.Amount > S.IndAmt) AS NextAmt,
--      /* also keep IndAmt to support the “at/above max” case */
--      S.IndAmt
--    FROM ScopePO S
--  ),
--  I_Ladder AS (
--    /*
--      If NextAmt exists → include all tiers with Amount < NextAmt.
--      If NextAmt is NULL (i.e., IndAmt ≥ max tier) → include all tiers with Amount <= IndAmt.
--      This prevents adding the “VP at 2,000” when IndAmt is 200 (your case on PO 714004).
--    */
--    SELECT S.PoNumber,
--           D.[Level]                AS RoleCode,
--           CAST('I' AS char(1))     AS Category,
--           D.Amount                 AS ThresholdAmt
--    FROM ScopePO S
--    JOIN I_Limits L ON L.PoNumber = S.PoNumber
--    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
--      ON (
--           (L.NextAmt IS NOT NULL AND D.Amount <  L.NextAmt)
--        OR (L.NextAmt IS     NULL AND D.Amount <= L.IndAmt)
--      )
--    WHERE S.IndAmt > 0  -- suppress indirect chain entirely when IndAmt = 0
--  ),
 
--  /* ------------------- DIRECT: single “ceiling” tier ------------------- */
--  D_Ceil AS (
--    -- normal case: the smallest tier >= DirAmt
--    SELECT S.PoNumber, MIN(D.Amount) AS CeilingAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON S.DirAmt > 0 AND D.Amount >= S.DirAmt
--    GROUP BY S.PoNumber
 
--    UNION ALL
 
--    -- if DirAmt exceeds max tier → choose the max tier
--    SELECT S.PoNumber, MAX(D.Amount) AS CeilingAmt
--    FROM ScopePO S
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON S.DirAmt > 0
--    WHERE NOT EXISTS (
--      SELECT 1
--      FROM dbo.PO_DelegationOfAuthority_Direct_Material D2
--      WHERE D2.Amount >= S.DirAmt
--    )
--    GROUP BY S.PoNumber
--  ),
--  D_Ladder AS (
--    SELECT S.PoNumber,
--           D.[Level]               AS RoleCode,
--           CAST('D' AS char(1))    AS Category,
--           D.Amount                AS ThresholdAmt
--    FROM ScopePO S
--    JOIN D_Ceil C ON C.PoNumber = S.PoNumber
--    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
--      ON D.Amount = C.CeilingAmt
--  ),
 
--  /* ------------------- UNION, order, and dedupe cross-ladder ------------------- */
--  Ladders AS (
--    SELECT PoNumber, RoleCode, Category, ThresholdAmt,
--           ROW_NUMBER() OVER (PARTITION BY PoNumber, Category
--                              ORDER BY ThresholdAmt ASC, RoleCode) AS OrdInCat,
--           CASE WHEN Category = 'I' THEN 1 ELSE 2 END AS CatOrder
--    FROM (
--      SELECT * FROM I_Ladder
--      UNION ALL
--      SELECT * FROM D_Ladder
--    ) X
--  ),
--  OrderedUnion AS (
--    -- if the same RoleCode appears in both ladders, we’ll keep a single copy
--    SELECT L.PoNumber, L.RoleCode, L.Category, L.ThresholdAmt,
--           ROW_NUMBER() OVER (PARTITION BY L.PoNumber, L.RoleCode
--                              ORDER BY L.CatOrder, L.OrdInCat) AS KeepOne
--    FROM Ladders L
--  ),
--  Kept AS (
--    SELECT K.PoNumber,
--           K.RoleCode,
--           CASE
--             WHEN EXISTS (
--               SELECT 1
--               FROM Ladders L2
--               WHERE L2.PoNumber = K.PoNumber
--                 AND L2.RoleCode = K.RoleCode
--                 AND L2.Category <> K.Category
--             )
--             THEN NULL  -- present in both ladders → single stage with Category = NULL
--             ELSE K.Category
--           END AS Category,
--           K.ThresholdAmt
--    FROM OrderedUnion K
--    WHERE K.KeepOne = 1
--  ),
--  ResolvedApprover AS (
--    -- resolve approver user with precedence: (Role, House, Buyer) → (Role, House, '') → (Role, 'GLOBAL', '')
--    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
--           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
--    FROM Kept K
--    JOIN ScopePO S ON S.PoNumber = K.PoNumber
--    OUTER APPLY (
--      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = S.BuyerCode
--      ORDER BY UpdatedAtUtc DESC
--    ) D1
--    OUTER APPLY (
--      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = S.HouseCode
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D2
--    OUTER APPLY (
--      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
--      WHERE IsActive = 1
--        AND RoleCode = K.RoleCode
--        AND HouseCode = N'GLOBAL'
--        AND BuyerCode = N''
--      ORDER BY UpdatedAtUtc DESC
--    ) D3
--  )
--  INSERT INTO #Build(PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
--  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
--  FROM ResolvedApprover;
 
--  --------------------------------------------------------------------
--  -- Assign sequence: Indirect first (ascending), then Direct
--  --------------------------------------------------------------------
--  ;WITH SeqBase AS (
--    SELECT B.*,
--           CASE WHEN B.Category = 'I' THEN 1
--                WHEN B.Category = 'D' THEN 2
--                ELSE 3 -- “both” (if ever produced) comes after I and D
--           END AS CatOrder
--    FROM #Build B
--  ),
--  Ordered AS (
--    SELECT PoNumber, RoleCode, Category, ApproverUserId,
--           ROW_NUMBER() OVER (
--             PARTITION BY PoNumber
--             ORDER BY CatOrder, ISNULL(ThresholdAmt, 0), RoleCode
--           ) AS Seq
--    FROM SeqBase
--  )
--  MERGE dbo.PO_ApprovalStage AS T
--  USING Ordered AS S
--    ON T.PoNumber  = S.PoNumber
--   AND T.[Sequence]= S.Seq
--  WHEN MATCHED THEN
--    UPDATE SET
--      T.RoleCode       = S.RoleCode,
--      T.ApproverUserId = S.ApproverUserId,
--      T.Category       = S.Category,
--      T.ThresholdFrom  = NULL,  -- not used in this build
--      T.ThresholdTo    = NULL,  -- not used in this build
--      T.[Status]       = CASE WHEN T.[Status] IN ('A','D','S') THEN T.[Status] ELSE 'P' END
--  WHEN NOT MATCHED BY TARGET THEN
--    INSERT (PoNumber, [Sequence], RoleCode, ApproverUserId, Category, ThresholdFrom, ThresholdTo, [Status], DecidedAtUtc)
--    VALUES (S.PoNumber, S.Seq, S.RoleCode, S.ApproverUserId, S.Category, NULL, NULL, 'P', NULL)
--  WHEN NOT MATCHED BY SOURCE
--       AND T.PoNumber IN (SELECT PoNumber FROM @PoNumbers)
--  THEN
--    DELETE
--  ;
 
--  COMMIT;
--END
--GO


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

/* ----- OPTIONAL: a GLOBAL fallback if a house entry is missing ----- */
IF NOT EXISTS (SELECT 1 FROM dbo.PO_ApproverDirectory WHERE HouseCode='GLOBAL')
BEGIN
  INSERT INTO dbo.PO_ApproverDirectory (RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email) VALUES
  ('Buyer',                          'GLOBAL','', 'u.buyer.global',        'Buyer (Global)',                         'buyer.global@example.com'),
  ('Local Purchasing Manager',       'GLOBAL','', 'u.lpm.global',           'Local Purchasing Manager (Global)',      'lpm.global@example.com'),
  ('Financial Controller',           'GLOBAL','', 'u.fc.global',            'Financial Controller (Global)',          'fc.global@example.com'),
  ('Plant Manager (General Manager)','GLOBAL','', 'u.gm.global',            'Plant Manager / GM (Global)',            'gm.global@example.com'),
  ('Regional Supply Chain Manager',  'GLOBAL','', 'u.rscm.global',          'Regional Supply Chain Manager (Global)', 'rscm.global@example.com'),
  ('IP CFO Direct Reports',          'GLOBAL','', 'u.ipcfo.dr.global',      'IP CFO Direct Reports (Global)',         'ipcfo.dr.global@example.com'),
  ('Regional VP/GM',                 'GLOBAL','', 'u.rvp.global',           'Regional VP/GM (Global)',                'rvp.global@example.com'),
  ('IP CFO',                         'GLOBAL','', 'u.ipcfo.global',         'IP CFO (Global)',                        'ipcfo.global@example.com'),
  ('IP President',                   'GLOBAL','', 'u.ippres.global',        'IP President (Global)',                  'ippres.global@example.com'),
  ('ITT CFO',                        'GLOBAL','', 'u.ittcfo.global',        'ITT CFO (Global)',                       'ittcfo.global@example.com'),
  ('ITT CEO',                        'GLOBAL','', 'u.ittceo.global',        'ITT CEO (Global)',                       'ittceo.global@example.com'),
  ('Cost Center Owner/Supervisor',   'GLOBAL','', 'u.cco.global',           'Cost Center Owner/Supervisor (Global)',  'cco.global@example.com'),
  ('Local Department Manager',       'GLOBAL','', 'u.ldm.global',           'Local Department Manager (Global)',      'ldm.global@example.com');
END;

/* ----- Helper to insert the same role set for a specific HouseCode ----- */
DECLARE @houses TABLE(HouseCode nvarchar(10));
INSERT INTO @houses(HouseCode) VALUES (N'E0'),(N'E1'),(N'E2'),(N'E4');

;WITH R(RoleCode, UserIdPrefix, DisplayName)
AS (
  SELECT 'Buyer',                          'buyer',       'Buyer' UNION ALL
  SELECT 'Local Purchasing Manager',       'lpm',         'Local Purchasing Manager' UNION ALL
  SELECT 'Financial Controller',           'fc',          'Financial Controller' UNION ALL
  SELECT 'Plant Manager (General Manager)','gm',          'Plant Manager / GM' UNION ALL
  SELECT 'Regional Supply Chain Manager',  'rscm',        'Regional Supply Chain Manager' UNION ALL
  SELECT 'IP CFO Direct Reports',          'ipcfo.dr',    'IP CFO Direct Reports' UNION ALL
  SELECT 'Regional VP/GM',                 'rvp',         'Regional VP/GM' UNION ALL
  SELECT 'IP CFO',                         'ipcfo',       'IP CFO' UNION ALL
  SELECT 'IP President',                   'ippres',      'IP President' UNION ALL
  SELECT 'ITT CFO',                        'ittcfo',      'ITT CFO' UNION ALL
  SELECT 'ITT CEO',                        'ittceo',      'ITT CEO' UNION ALL
  SELECT 'Cost Center Owner/Supervisor',   'cco',         'Cost Center Owner/Supervisor' UNION ALL
  SELECT 'Local Department Manager',       'ldm',         'Local Department Manager'
)
INSERT INTO dbo.PO_ApproverDirectory (RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
SELECT
  R.RoleCode,
  H.HouseCode,
  N'',  -- keep BuyerCode blank for now
  CONCAT('u.', R.UserIdPrefix, '.', LOWER(H.HouseCode)),
  CONCAT(R.DisplayName, ' (', H.HouseCode, ')'),
  CONCAT(R.UserIdPrefix, '.', LOWER(H.HouseCode), '@example.com')
FROM R
CROSS JOIN @houses H
WHERE NOT EXISTS (
  SELECT 1
  FROM dbo.PO_ApproverDirectory d
  WHERE d.RoleCode = R.RoleCode
    AND d.HouseCode = H.HouseCode
    AND d.BuyerCode = N''
);
