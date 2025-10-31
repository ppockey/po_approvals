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

  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
  CREATE TABLE #Build(
    PoNumber       nvarchar(20) NOT NULL,
    RoleCode       nvarchar(40) NOT NULL,
    Category       char(1) NULL,
    ThresholdAmt   decimal(19,4) NULL,
    ApproverUserId nvarchar(100) NULL
  );

  ;WITH ScopePO AS (
    SELECT H.PoNumber, H.HouseCode, COALESCE(NULLIF(H.BuyerCode,''), N'') AS BuyerCode,
           COALESCE(H.IndirectAmount, 0) AS IndAmt,
           COALESCE(H.DirectAmount,   0) AS DirAmt
    FROM dbo.PO_Header H
    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
    WHERE H.IsActive = 1 AND H.[Status] IN ('W')
  ),
  I_Ladder AS (
    SELECT S.PoNumber, D.[Level] AS RoleCode, CAST('I' AS char(1)) AS Category, D.Amount AS ThresholdAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D ON D.Amount <= S.IndAmt
  ),
  D_Ladder AS (
    SELECT S.PoNumber, D.[Level] AS RoleCode, CAST('D' AS char(1)) AS Category, D.Amount AS ThresholdAmt
    FROM ScopePO S
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D ON D.Amount <= S.DirAmt
  ),
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
           ROW_NUMBER() OVER (PARTITION BY L.PoNumber, L.RoleCode ORDER BY L.CatOrder, L.OrdInCat) AS KeepOne
    FROM Ladders L
  ),
  Kept AS (
    SELECT K.PoNumber, K.RoleCode,
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
    SELECT K.PoNumber, K.RoleCode, K.Category, K.ThresholdAmt,
           COALESCE(D1.UserId, D2.UserId, D3.UserId) AS ApproverUserId
    FROM Kept K
    JOIN ScopePO S ON S.PoNumber = K.PoNumber
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1 AND RoleCode = K.RoleCode AND HouseCode = S.HouseCode AND BuyerCode = S.BuyerCode
      ORDER BY UpdatedAtUtc DESC
    ) D1
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1 AND RoleCode = K.RoleCode AND HouseCode = S.HouseCode AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D2
    OUTER APPLY (
      SELECT TOP(1) * FROM dbo.PO_ApproverDirectory
      WHERE IsActive = 1 AND RoleCode = K.RoleCode AND HouseCode = N'GLOBAL' AND BuyerCode = N''
      ORDER BY UpdatedAtUtc DESC
    ) D3
  )
  INSERT INTO #Build(PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
  SELECT PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId
  FROM ResolvedApprover;

  ;WITH SeqBase AS (
    SELECT B.*,
           CASE WHEN B.Category = 'I' THEN 1
                WHEN B.Category = 'D' THEN 2
                ELSE 3
           END AS CatOrder
    FROM #Build B
  ),
  Ordered AS (
    SELECT PoNumber, RoleCode, Category, ApproverUserId,
           ROW_NUMBER() OVER (PARTITION BY PoNumber ORDER BY CatOrder, ISNULL(ThresholdAmt, 0), RoleCode) AS Seq
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
  THEN DELETE;

  COMMIT;
END
GO

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

-- Seed directory (run once) --------------------------
-- EITHER keep your original INSERTs and run once,
-- OR use idempotent upserts (shown here) to be safe on re-runs.

MERGE dbo.PO_ApproverDirectory AS T
USING (VALUES
('Buyer',                          'GLOBAL', '', 'u.buyer.global',        'Default Buyer',                         'buyer.global@example.com'),
('Local Purchasing Manager',       'GLOBAL', '', 'u.lpm.global',           'Local Purchasing Manager (Global)',     'lpm.global@example.com'),
('Financial Controller',           'GLOBAL', '', 'u.fc.global',            'Financial Controller (Global)',         'fc.global@example.com'),
('Plant Manager (General Manager)','GLOBAL', '', 'u.gm.global',            'Plant Manager / GM (Global)',           'gm.global@example.com'),
('Regional Supply Chain Manager',  'GLOBAL', '', 'u.rscm.global',          'Regional Supply Chain Manager (Global)','rscm.global@example.com'),
('IP CFO Direct Reports',          'GLOBAL', '', 'u.ipcfo.dr.global',      'IP CFO Direct Reports (Global)',        'ipcfo.dr.global@example.com'),
('Regional VP/GM',                 'GLOBAL', '', 'u.rvp.global',           'Regional VP/GM (Global)',               'rvp.global@example.com'),
('IP CFO',                         'GLOBAL', '', 'u.ipcfo.global',         'IP CFO (Global)',                        'ipcfo.global@example.com'),
('IP President',                   'GLOBAL', '', 'u.ippres.global',        'IP President (Global)',                 'ippres.global@example.com'),
('ITT CFO',                        'GLOBAL', '', 'u.ittcfo.global',        'ITT CFO (Global)',                      'ittcfo.global@example.com'),
('ITT CEO',                        'GLOBAL', '', 'u.ittceo.global',        'ITT CEO (Global)',                      'ittceo.global@example.com'),
('Cost Center Owner/Supervisor',   'GLOBAL', '', 'u.cco.global',           'Cost Center Owner/Supervisor (Global)', 'cco.global@example.com'),
('Local Department Manager',       'GLOBAL', '', 'u.ldm.global',           'Local Department Manager (Global)',     'ldm.global@example.com')
) AS S(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
ON (T.RoleCode = S.RoleCode AND T.HouseCode = S.HouseCode AND T.BuyerCode = S.BuyerCode)
WHEN MATCHED THEN
  UPDATE SET UserId = S.UserId, DisplayName = S.DisplayName, Email = S.Email, UpdatedAtUtc = SYSUTCDATETIME(), IsActive = 1
WHEN NOT MATCHED THEN
  INSERT(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
  VALUES(S.RoleCode, S.HouseCode, S.BuyerCode, S.UserId, S.DisplayName, S.Email);
GO

MERGE dbo.PO_ApproverDirectory AS T
USING (VALUES
('Local Purchasing Manager',       'IP', '',   'u.lpm.ip',     'Local Purchasing Manager (IP)',       'lpm.ip@example.com'),
('Financial Controller',           'IP', '',   'u.fc.ip',      'Financial Controller (IP)',           'fc.ip@example.com'),
('Plant Manager (General Manager)','IP', '',   'u.gm.ip',      'Plant Manager / GM (IP)',             'gm.ip@example.com'),
('Regional VP/GM',                 'IP', '',   'u.rvp.ip',     'Regional VP/GM (IP)',                 'rvp.ip@example.com'),
('Regional Supply Chain Manager',  'IP', '',   'u.rscm.ip',    'Regional Supply Chain Manager (IP)',  'rscm.ip@example.com')
) AS S(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
ON (T.RoleCode = S.RoleCode AND T.HouseCode = S.HouseCode AND T.BuyerCode = S.BuyerCode)
WHEN MATCHED THEN
  UPDATE SET UserId = S.UserId, DisplayName = S.DisplayName, Email = S.Email, UpdatedAtUtc = SYSUTCDATETIME(), IsActive = 1
WHEN NOT MATCHED THEN
  INSERT(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
  VALUES(S.RoleCode, S.HouseCode, S.BuyerCode, S.UserId, S.DisplayName, S.Email);
GO

MERGE dbo.PO_ApproverDirectory AS T
USING (VALUES
('Local Purchasing Manager', 'IP', 'EPM109', 'u.lpm.ip.epm109', 'Local Purchasing Manager (IP/EPM109)', 'lpm.ip.epm109@example.com'),
('Buyer',                    'IP', 'EPM109', 'u.buyer.ip.epm109','Buyer (IP/EPM109)',                   'buyer.ip.epm109@example.com')
) AS S(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
ON (T.RoleCode = S.RoleCode AND T.HouseCode = S.HouseCode AND T.BuyerCode = S.BuyerCode)
WHEN MATCHED THEN
  UPDATE SET UserId = S.UserId, DisplayName = S.DisplayName, Email = S.Email, UpdatedAtUtc = SYSUTCDATETIME(), IsActive = 1
WHEN NOT MATCHED THEN
  INSERT(RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
  VALUES(S.RoleCode, S.HouseCode, S.BuyerCode, S.UserId, S.DisplayName, S.Email);
GO
