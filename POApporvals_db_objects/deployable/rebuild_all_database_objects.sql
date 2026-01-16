/*=============================================================================
  PO Approvals - DEV Rebuild Everything
  Schema: dbo
  Database: WebappsDev (change if needed)
=============================================================================*/

USE [WebappsDev];
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/*=============================================================================
  1) DROP in dependency order
=============================================================================*/

-- Views
IF OBJECT_ID(N'dbo.vw_PO_Line_Active', N'V') IS NOT NULL DROP VIEW dbo.vw_PO_Line_Active;
GO
IF OBJECT_ID(N'dbo.vw_PO_Header_WaitingActive', N'V') IS NOT NULL DROP VIEW dbo.vw_PO_Header_WaitingActive;
GO

-- Stored procedures
IF OBJECT_ID(N'dbo.PO_DoA_Indirect_Replace', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_DoA_Indirect_Replace;
GO
IF OBJECT_ID(N'dbo.PO_DoA_Direct_Replace', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_DoA_Direct_Replace;
GO
IF OBJECT_ID(N'dbo.PO_Staging_Truncate', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_Staging_Truncate;
GO
IF OBJECT_ID(N'dbo.PO_IngestAndBuild', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_IngestAndBuild;
GO
IF OBJECT_ID(N'dbo.PO_BuildApprovalStages', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_BuildApprovalStages;
GO
IF OBJECT_ID(N'dbo.PO_Merge', N'P') IS NOT NULL DROP PROCEDURE dbo.PO_Merge;
GO

-- Types
IF TYPE_ID(N'dbo.DoA_IndirectType') IS NOT NULL DROP TYPE dbo.DoA_IndirectType;
GO
IF TYPE_ID(N'dbo.DoA_DirectType') IS NOT NULL DROP TYPE dbo.DoA_DirectType;
GO
IF TYPE_ID(N'dbo.PoNumberList') IS NOT NULL DROP TYPE dbo.PoNumberList;
GO

-- Tables (children before parents)
IF OBJECT_ID(N'dbo.PO_ApprovalStage', N'U') IS NOT NULL DROP TABLE dbo.PO_ApprovalStage;
GO
IF OBJECT_ID(N'dbo.PO_ApprovalChain', N'U') IS NOT NULL DROP TABLE dbo.PO_ApprovalChain;
GO
IF OBJECT_ID(N'dbo.PO_Approval_Audit', N'U') IS NOT NULL DROP TABLE dbo.PO_Approval_Audit;
GO
IF OBJECT_ID(N'dbo.PO_ApprovalOutbox', N'U') IS NOT NULL DROP TABLE dbo.PO_ApprovalOutbox;
GO
IF OBJECT_ID(N'dbo.PO_ApproverDirectory', N'U') IS NOT NULL DROP TABLE dbo.PO_ApproverDirectory;
GO

IF OBJECT_ID(N'dbo.PO_Line', N'U') IS NOT NULL DROP TABLE dbo.PO_Line;
GO
IF OBJECT_ID(N'dbo.PO_Header', N'U') IS NOT NULL DROP TABLE dbo.PO_Header;
GO

IF OBJECT_ID(N'dbo.PO_DelegationOfAuthority_Direct_Material', N'U') IS NOT NULL DROP TABLE dbo.PO_DelegationOfAuthority_Direct_Material;
GO
IF OBJECT_ID(N'dbo.PO_DelegationOfAuthority_Indirect_Expense', N'U') IS NOT NULL DROP TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense;
GO
IF OBJECT_ID(N'dbo.PO_ApprovalPolicy', N'U') IS NOT NULL DROP TABLE dbo.PO_ApprovalPolicy;
GO

IF OBJECT_ID(N'dbo.PO_Stg_Line', N'U') IS NOT NULL DROP TABLE dbo.PO_Stg_Line;
GO
IF OBJECT_ID(N'dbo.PO_Stg_Header', N'U') IS NOT NULL DROP TABLE dbo.PO_Stg_Header;
GO


/*=============================================================================
  2) CREATE Tables
=============================================================================*/

-- =========================
-- TABLE: dbo.PO_Header
-- =========================
CREATE TABLE dbo.PO_Header (
    PoHeaderId         bigint IDENTITY(1,1) NOT NULL,
    PoNumber           nvarchar(20) NOT NULL,
    PoDate             date NULL,
    VendorNumber       nvarchar(20) NULL,
    VendorName         nvarchar(80) NULL,
    VendorAddr1        nvarchar(80) NULL,
    VendorAddr2        nvarchar(80) NULL,
    VendorAddr3        nvarchar(80) NULL,
    VendorState        nvarchar(20) NULL,
    VendorPostalCode   nvarchar(20) NULL,
    BuyerCode          nvarchar(10) NULL,
    BuyerName          nvarchar(60) NULL,
    HouseCode          nvarchar(10) NULL,
    DirectAmount       decimal(18,2) NULL,
    IndirectAmount     decimal(18,2) NULL,
    CreatedAtUtc       datetime2(0) NULL,

    -- computed from first usable GLAccount on active lines
    CostCenterKey      char(6) NULL,

    IsActive           bit NOT NULL CONSTRAINT DF_PO_Header_IsActive DEFAULT(1),
    DeactivatedAtUtc   datetime2(0) NULL,
    DeactivatedBy      nvarchar(64) NULL,
    DeactivationReason nvarchar(200) NULL,
    [Status]           char(1) NOT NULL CONSTRAINT DF_PO_Header_Status DEFAULT('W'),

    CONSTRAINT PK_PO_Header PRIMARY KEY CLUSTERED (PoHeaderId),
    CONSTRAINT UQ_PO_Header_PoNumber UNIQUE (PoNumber),
    CONSTRAINT CK_PO_Header_Status CHECK ([Status] IN ('W','A','D'))
);
GO

CREATE INDEX IX_PO_Header_StatusActive
    ON dbo.PO_Header ([Status], IsActive)
    INCLUDE (PoNumber, PoDate, VendorName, BuyerName, HouseCode, DirectAmount, IndirectAmount, CostCenterKey);
GO


-- =========================
-- TABLE: dbo.PO_Line
-- =========================
CREATE TABLE dbo.PO_Line (
    PoLineId           bigint IDENTITY(1,1) NOT NULL,
    PoHeaderId         bigint NOT NULL,
    PoNumber           nvarchar(20) NOT NULL,
    LineNumber         int NOT NULL,
    HouseCode          nvarchar(10) NULL,
    ItemNumber         nvarchar(40) NULL,
    ItemDescription    nvarchar(120) NULL,
    ItemShortDescription nvarchar(60) NULL,
    QuantityOrdered    decimal(18,4) NULL,
    OrderUom           nvarchar(12) NULL,
    UnitCost           decimal(18,4) NULL,
    ExtendedCost       decimal(18,4) NULL,
    RequiredDate       date NULL,
    GlAccount          nvarchar(40) NULL,
    IsActive           bit NOT NULL CONSTRAINT DF_PO_Line_IsActive DEFAULT(1),
    DeactivatedAtUtc   datetime2(0) NULL,
    DeactivatedBy      nvarchar(64) NULL,
    DeactivationReason nvarchar(200) NULL,

    CONSTRAINT PK_PO_Line PRIMARY KEY CLUSTERED (PoLineId),
    CONSTRAINT FK_PO_Line_Header_PoHeaderId FOREIGN KEY (PoHeaderId) REFERENCES dbo.PO_Header(PoHeaderId)
);
GO

CREATE UNIQUE INDEX UX_PO_Line_Po_LineNumber ON dbo.PO_Line (PoNumber, LineNumber);
GO
CREATE INDEX IX_PO_Line_PoHeaderId ON dbo.PO_Line (PoHeaderId);
GO
CREATE INDEX IX_PO_Line_Po_IsActive
    ON dbo.PO_Line (PoNumber, IsActive)
    INCLUDE (LineNumber, ItemDescription, ExtendedCost);
GO


-- =========================
-- TABLE: dbo.PO_Approval_Audit
-- =========================
CREATE TABLE dbo.PO_Approval_Audit (
    AuditId      bigint IDENTITY(1,1) NOT NULL,
    PoNumber     nvarchar(20) NOT NULL,
    OldStatus    char(1) NOT NULL,
    NewStatus    char(1) NOT NULL,
    ChangedBy    nvarchar(100) NOT NULL,
    ChangedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_PO_Approval_Audit_ChangedAtUtc DEFAULT (sysutcdatetime()),
    DecisionNote nvarchar(4000) NULL,
    [Sequence]   int NULL,
    RoleCode     nvarchar(40) NULL,
    Category     char(1) NULL,

    CONSTRAINT PK_PO_Approval_Audit PRIMARY KEY CLUSTERED (AuditId),
    CONSTRAINT CK_PO_Approval_Audit_Status CHECK (OldStatus IN ('P','A','D','S',' ') AND NewStatus IN ('P','A','D','S'))
);
GO

CREATE INDEX IX_PO_Approval_Audit_PO_Stage
    ON dbo.PO_Approval_Audit (PoNumber, [Sequence], RoleCode, ChangedAtUtc);
GO


-- =========================
-- TABLE: dbo.PO_ApprovalChain
-- =========================
CREATE TABLE dbo.PO_ApprovalChain (
    PoNumber       nvarchar(20) NOT NULL,
    CreatedAtUtc   datetime2(0) NOT NULL CONSTRAINT DF_PO_ApprovalChain_CreatedAtUtc DEFAULT (sysutcdatetime()),
    [Status]       char(1) NOT NULL CONSTRAINT DF_PO_ApprovalChain_Status DEFAULT('P'),
    FinalizedAtUtc datetime2(0) NULL,

    CONSTRAINT PK_PO_ApprovalChain PRIMARY KEY CLUSTERED (PoNumber),
    CONSTRAINT CK_PO_ApprovalChain_Status CHECK ([Status] IN ('P','A','D'))
);
GO


-- =========================
-- TABLE: dbo.PO_ApprovalStage
-- =========================
CREATE TABLE dbo.PO_ApprovalStage (
    PoNumber        nvarchar(20) NOT NULL,
    [Sequence]      int NOT NULL,
    RoleCode        nvarchar(50) NOT NULL,
    ApproverUserId  nvarchar(256) NULL,
    Category        char(1) NULL, -- 'I','D', or NULL
    ThresholdFrom   decimal(18,2) NULL,
    ThresholdTo     decimal(18,2) NULL,
    [Status]        char(1) NOT NULL CONSTRAINT DF_PO_ApprovalStage_Status DEFAULT('P'),
    DecidedAtUtc    datetime2(0) NULL,

    CONSTRAINT PK_PO_ApprovalStage PRIMARY KEY CLUSTERED (PoNumber, [Sequence]),
    CONSTRAINT CK_PO_ApprovalStage_Status CHECK ([Status] IN ('P','A','D','S')),
    CONSTRAINT CK_PO_ApprovalStage_Category CHECK (Category IN ('I','D') OR Category IS NULL),
    CONSTRAINT FK_PO_ApprovalStage_Chain FOREIGN KEY (PoNumber) REFERENCES dbo.PO_ApprovalChain(PoNumber)
);
GO

CREATE INDEX IX_PO_ApprovalStage_Po_Status_Seq
    ON dbo.PO_ApprovalStage (PoNumber, [Status], [Sequence]);
GO
CREATE INDEX IX_PO_ApprovalStage_Po_Status_Role
    ON dbo.PO_ApprovalStage (PoNumber, [Status], RoleCode);
GO


-- =========================
-- TABLE: dbo.PO_ApprovalOutbox
-- =========================
CREATE TABLE dbo.PO_ApprovalOutbox (
    OutboxId       bigint IDENTITY(1,1) NOT NULL,
    EventType      nvarchar(40) NOT NULL,
    PoNumber       nvarchar(20) NOT NULL,
    OccurredAtUtc  datetime2(0) NOT NULL CONSTRAINT DF_PO_ApprovalOutbox_Occurred DEFAULT (sysutcdatetime()),
    PayloadJson    nvarchar(max) NULL,
    Attempts       int NOT NULL CONSTRAINT DF_PO_ApprovalOutbox_Attempts DEFAULT (0),
    ProcessedAtUtc datetime2(0) NULL,
    DirectAmount   decimal(18,2) NULL,
    IndirectAmount decimal(18,2) NULL,

    CONSTRAINT PK_PO_ApprovalOutbox PRIMARY KEY CLUSTERED (OutboxId)
);
GO

CREATE UNIQUE INDEX UX_PO_ApprovalOutbox_Unprocessed
    ON dbo.PO_ApprovalOutbox (EventType, PoNumber)
    WHERE ProcessedAtUtc IS NULL;
GO

CREATE INDEX IX_PO_ApprovalOutbox_Queued
    ON dbo.PO_ApprovalOutbox (ProcessedAtUtc, Attempts);
GO


-- =========================
-- TABLE: dbo.PO_ApproverDirectory
-- =========================
CREATE TABLE dbo.PO_ApproverDirectory
(
    Id            int IDENTITY(1,1) NOT NULL,

    Email         nvarchar(256) NOT NULL,
    RoleCode      nvarchar(50)  NOT NULL,

    -- Optional routing scopes (kept for future; nullable)
    HouseCode     nvarchar(10)  NULL,
    BuyerCode     nvarchar(10)  NULL,

    -- Only used for COST CENTER OWNER/SUPERVISOR role routing
    CostCenterKey char(6)       NULL,

    DisplayName   nvarchar(120) NULL,

    IsActive      bit           NOT NULL CONSTRAINT DF_PO_ApproverDirectory_IsActive DEFAULT (1),
    UpdatedAtUtc  datetime2(0)  NOT NULL CONSTRAINT DF_PO_ApproverDirectory_UpdatedAtUtc DEFAULT (SYSUTCDATETIME()),

    CONSTRAINT PK_PO_ApproverDirectory PRIMARY KEY CLUSTERED (Id),

    CONSTRAINT CK_PO_ApproverDirectory_CostCenterKey
        CHECK (CostCenterKey IS NULL OR LEN(CostCenterKey) = 6)
);
GO

-- Fast lookup for routing: RoleCode (+ CostCenterKey when applicable)
CREATE INDEX IX_PO_ApproverDirectory_Role_Key_Active
    ON dbo.PO_ApproverDirectory (RoleCode, CostCenterKey, IsActive)
    INCLUDE (Email, DisplayName, HouseCode, BuyerCode, UpdatedAtUtc);
GO

-- One ACTIVE global mapping per RoleCode (CostCenterKey NULL)
CREATE UNIQUE INDEX UX_PO_ApproverDirectory_Active_Role_Global
    ON dbo.PO_ApproverDirectory (RoleCode)
    WHERE IsActive = 1 AND CostCenterKey IS NULL;
GO

-- One ACTIVE mapping per RoleCode + CostCenterKey (CostCenterKey present)
CREATE UNIQUE INDEX UX_PO_ApproverDirectory_Active_Role_CostCenter
    ON dbo.PO_ApproverDirectory (RoleCode, CostCenterKey)
    WHERE IsActive = 1 AND CostCenterKey IS NOT NULL;
GO

-- Prevent exact duplicates across all states (active/inactive)
CREATE UNIQUE INDEX UX_PO_ApproverDirectory_Unique_Row
    ON dbo.PO_ApproverDirectory (Email, RoleCode, HouseCode, BuyerCode, CostCenterKey);
GO


-- =========================
-- TABLE: dbo.PO_DelegationOfAuthority_Direct_Material
-- =========================
CREATE TABLE dbo.PO_DelegationOfAuthority_Direct_Material (
    Id     int IDENTITY(1,1) NOT NULL,
    [Level] nvarchar(50) NOT NULL,
    Amount  decimal(19,4) NOT NULL,
    CONSTRAINT PK_PO_DoA_Direct PRIMARY KEY CLUSTERED (Id)
);
GO

CREATE UNIQUE INDEX UX_PO_DoA_Direct_Level
    ON dbo.PO_DelegationOfAuthority_Direct_Material ([Level]);
GO


-- =========================
-- TABLE: dbo.PO_DelegationOfAuthority_Indirect_Expense
-- =========================
CREATE TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense (
    Id     int IDENTITY(1,1) NOT NULL,
    [Level] nvarchar(50) NOT NULL,
    Amount  decimal(19,4) NOT NULL,
    CONSTRAINT PK_PO_DoA_Indirect PRIMARY KEY CLUSTERED (Id)
);
GO

CREATE UNIQUE INDEX UX_PO_DoA_Indirect_Level
    ON dbo.PO_DelegationOfAuthority_Indirect_Expense ([Level]);
GO


-- =========================
-- TABLE: dbo.PO_Stg_Header (staging)
-- =========================
CREATE TABLE dbo.PO_Stg_Header (
    PoNumber         nvarchar(20) NULL,
    PoDate           date NULL,
    VendorNumber     nvarchar(20) NULL,
    VendorName       nvarchar(80) NULL,
    VendorAddr1      nvarchar(80) NULL,
    VendorAddr2      nvarchar(80) NULL,
    VendorAddr3      nvarchar(80) NULL,
    VendorState      nvarchar(20) NULL,
    VendorPostalCode nvarchar(20) NULL,
    BuyerCode        nvarchar(10) NULL,
    BuyerName        nvarchar(60) NULL,
    HouseCode        nvarchar(10) NULL,
    DirectAmount     decimal(18,2) NULL,
    IndirectAmount   decimal(18,2) NULL,
    CreatedAtUtc     datetime2(0) NULL
);
GO


-- =========================
-- TABLE: dbo.PO_Stg_Line (staging)
-- =========================
CREATE TABLE dbo.PO_Stg_Line (
    PoNumber             nvarchar(20) NULL,
    LineNumber           int NULL,
    HouseCode            nvarchar(10) NULL,
    ItemNumber           nvarchar(40) NULL,
    ItemDescription      nvarchar(120) NULL,
    ItemShortDescription nvarchar(60) NULL,
    QuantityOrdered      decimal(18,4) NULL,
    OrderUom             nvarchar(12) NULL,
    UnitCost             decimal(18,4) NULL,
    ExtendedCost         decimal(18,4) NULL,
    RequiredDate         date NULL,
    GlAccount            nvarchar(40) NULL,
    CreatedAtUtc         datetime2(0) NULL
);
GO


-- =========================
-- TABLE: dbo.PO_ApprovalPolicy
-- =========================
CREATE TABLE dbo.PO_ApprovalPolicy
(
    PolicyId         int            IDENTITY(1,1) NOT NULL
      CONSTRAINT PK_PO_ApprovalPolicy PRIMARY KEY,

    -- Scope columns reserved for future use; keep NULLs for a global policy
    HouseCode        nvarchar(10)   NULL,
    BuyerCode        nvarchar(10)   NULL,

    -- Three gate values used by the stage builder
    IndirectSplitAt  decimal(19,4)  NOT NULL,
    DirectMinAt      decimal(19,4)  NOT NULL,
    DirectStartAt    decimal(19,4)  NOT NULL,

    -- Row state & audit
    IsActive         bit            NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_IsActive DEFAULT(1),
    EffectiveDate    date           NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_EffectiveDate DEFAULT (CONVERT(date, SYSUTCDATETIME())),
    UpdatedBy        nvarchar(100)  NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_UpdatedBy DEFAULT (SUSER_SNAME()),
    UpdatedAtUtc     datetime2(0)   NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_UpdatedAtUtc DEFAULT (SYSUTCDATETIME()),
    Notes            nvarchar(400)  NULL,

    CONSTRAINT CK_PO_ApprovalPolicy_IndirectSplitAt_Positive CHECK (IndirectSplitAt >= 0),
    CONSTRAINT CK_PO_ApprovalPolicy_DirectMinAt_Positive     CHECK (DirectMinAt     >= 0),
    CONSTRAINT CK_PO_ApprovalPolicy_DirectStart_GTE_Min      CHECK (DirectStartAt   >= DirectMinAt)
);
GO

CREATE UNIQUE INDEX UX_PO_ApprovalPolicy_OneActive
    ON dbo.PO_ApprovalPolicy (IsActive)
    WHERE IsActive = 1;
GO


/*=============================================================================
  3) CREATE Views
=============================================================================*/

-- Active headers (waiting + active only)
CREATE VIEW dbo.vw_PO_Header_WaitingActive
AS
SELECT
  h.PoNumber,
  h.PoDate,
  h.VendorName,
  h.BuyerName,
  h.HouseCode,
  h.DirectAmount,
  h.IndirectAmount,
  (ISNULL(h.DirectAmount,0)+ISNULL(h.IndirectAmount,0)) AS TotalAmount,
  h.Status,
  h.IsActive,
  h.CreatedAtUtc,
  (SELECT COUNT(*) FROM dbo.PO_Line l WHERE l.PoNumber = h.PoNumber AND l.IsActive = 1) AS ActiveLineCount
FROM dbo.PO_Header h
WHERE h.Status = 'W' AND h.IsActive = 1;
GO

-- Active lines (for detail page)
CREATE VIEW dbo.vw_PO_Line_Active
AS
SELECT
  l.PoNumber,
  l.LineNumber,
  l.ItemNumber,
  l.ItemDescription,
  l.ItemShortDescription as 'SpecialDescription',
  l.QuantityOrdered,
  l.OrderUom,
  l.UnitCost,
  l.ExtendedCost,
  l.RequiredDate,
  l.GlAccount,
  l.IsActive
FROM dbo.PO_Line l
WHERE l.IsActive = 1;
GO


/*=============================================================================
  4) CREATE Types
=============================================================================*/

CREATE TYPE dbo.PoNumberList AS TABLE (
  PoNumber nvarchar(20) NOT NULL PRIMARY KEY
);
GO

CREATE TYPE dbo.DoA_DirectType AS TABLE
(
    [Level] nvarchar(50) NOT NULL,
    Amount  decimal(19,4) NOT NULL
);
GO

CREATE TYPE dbo.DoA_IndirectType AS TABLE
(
    [Level] nvarchar(50) NOT NULL,
    Amount  decimal(19,4) NOT NULL
);
GO


/*=============================================================================
  5) CREATE Procedures
=============================================================================*/

-- =============================================================================
-- Procedure: dbo.PO_Merge
-- Purpose  : Upsert headers/lines from staging, compute PO_Header.CostCenterKey,
--            emit PO_NEW_WAITING outbox (deduped).
-- =============================================================================
CREATE PROCEDURE dbo.PO_Merge
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @err nvarchar(4000) = NULL;

  BEGIN TRY
    BEGIN TRAN;

    /* ========================= HEADER MERGE ========================= */
    DECLARE @HdrChanges TABLE
    (
      PoNumber     nvarchar(20),
      Action       nvarchar(10),
      WasActive    bit,
      IsActiveNow  bit
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
         -- NOTE: CostCenterKey computed later (after line merge)
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        PoNumber, PoDate, VendorNumber, VendorName, VendorAddr1, VendorAddr2, VendorAddr3,
        VendorState, VendorPostalCode, BuyerCode, BuyerName, HouseCode,
        DirectAmount, IndirectAmount, CreatedAtUtc,
        CostCenterKey,
        IsActive, [Status]
      )
      VALUES (
        src.PoNumber, src.PoDate, src.VendorNumber, src.VendorName, src.VendorAddr1, src.VendorAddr2, src.VendorAddr3,
        src.VendorState, src.VendorPostalCode, src.BuyerCode, src.BuyerName, src.HouseCode,
        src.DirectAmount, src.IndirectAmount, src.CreatedAtUtc,
        NULL,
        1, 'W'
      )
    OUTPUT
      inserted.PoNumber,
      $action,
      CAST(COALESCE(deleted.IsActive, 0) AS bit),
      CAST(inserted.IsActive AS bit)
    INTO @HdrChanges(PoNumber, Action, WasActive, IsActiveNow)
    ;

    /* ========================= LINE MERGE ========================= */
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
    ;

    /* ========================= COMPUTE CostCenterKey =========================
       Rule: FIRST usable GLAccount from ACTIVE lines:
             - lowest LineNumber
             - trimmed GLAccount length >= 6
             - CostCenterKey = LEFT(trimmed, 6)
       Applies to POs touched in this run (@HdrChanges).
    ======================================================================== */
    ;WITH Candidates AS (
      SELECT
        l.PoNumber,
        LEFT(LTRIM(RTRIM(l.GlAccount)), 6) AS CostCenterKey,
        ROW_NUMBER() OVER (
          PARTITION BY l.PoNumber
          ORDER BY l.LineNumber ASC
        ) AS rn
      FROM dbo.PO_Line l
      JOIN @HdrChanges hc ON hc.PoNumber = l.PoNumber
      WHERE l.IsActive = 1
        AND l.GlAccount IS NOT NULL
        AND LTRIM(RTRIM(l.GlAccount)) <> ''
        AND LEN(LTRIM(RTRIM(l.GlAccount))) >= 6
    ),
    FirstKey AS (
      SELECT PoNumber, CostCenterKey
      FROM Candidates
      WHERE rn = 1
    )
    UPDATE h
      SET h.CostCenterKey = fk.CostCenterKey
    FROM dbo.PO_Header h
    JOIN @HdrChanges hc ON hc.PoNumber = h.PoNumber
    LEFT JOIN FirstKey fk ON fk.PoNumber = h.PoNumber;

    /* ========================= OUTBOX EMISSION =========================
       - Emits PO_NEW_WAITING for INSERTs or reactivations where Status='W'.
       - Payload: header summary + total active line count.
       - CostCenterKey comes from dbo.PO_Header.CostCenterKey.
       - Dedupe: skip if an unprocessed event exists for same EventType+PoNumber.
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
            H2.CostCenterKey  AS costCenterKey,
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
            JSON_VALUE(P.HeaderJson,'$.costCenterKey')    AS costCenterKey,
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
    WHERE o.OutboxId IS NULL;

    COMMIT;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    SET @err = ERROR_MESSAGE();
  END CATCH;

  /* ========================= Staging cleanup ========================= */
  IF @err IS NULL
  BEGIN
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
  END

  IF @err IS NOT NULL
    THROW 51001, @err, 1;
END
GO


/*==============================================================================
  Procedure: dbo.PO_BuildApprovalStages
==============================================================================*/
CREATE PROCEDURE dbo.PO_BuildApprovalStages
  @PoNumbers dbo.PoNumberList READONLY,
  @Rebuild   bit = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRAN;

  /* 1) Load active policy (must exist) */
  ;WITH ActivePolicy AS (
    SELECT TOP (1)
           IndirectSplitAt,
           DirectMinAt,
           DirectStartAt
    FROM dbo.PO_ApprovalPolicy
    WHERE IsActive = 1
    ORDER BY EffectiveDate DESC, UpdatedAtUtc DESC
  )
  SELECT * INTO #AP FROM ActivePolicy;

  IF NOT EXISTS (SELECT 1 FROM #AP)
  BEGIN
    ROLLBACK;
    THROW 52001, 'PO_BuildApprovalStages: No active row in dbo.PO_ApprovalPolicy.', 1;
  END

  /* 2) Ensure chain container exists */
  INSERT INTO dbo.PO_ApprovalChain (PoNumber, CreatedAtUtc, [Status], FinalizedAtUtc)
  SELECT H.PoNumber, SYSUTCDATETIME(), 'P', NULL
  FROM dbo.PO_Header H
  JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
  LEFT JOIN dbo.PO_ApprovalChain C ON C.PoNumber = H.PoNumber
  WHERE C.PoNumber IS NULL;

  /* 3) Optional hard rebuild */
  IF @Rebuild = 1
  BEGIN
    DELETE S
    FROM dbo.PO_ApprovalStage S
    JOIN @PoNumbers P ON P.PoNumber = S.PoNumber;
  END

  /* 4) Scratch */
  IF OBJECT_ID('tempdb..#Build') IS NOT NULL DROP TABLE #Build;
  CREATE TABLE #Build(
    PoNumber       nvarchar(20)  NOT NULL,
    RoleCode       nvarchar(50)  NOT NULL,
    Category       char(1)       NULL,  -- 'I','D', or NULL (both)
    ThresholdAmt   decimal(19,4) NULL,
    ApproverUserId nvarchar(256) NULL
  );

  /* 5) Scope rows */
  ;WITH ScopePO AS (
    SELECT H.PoNumber,
           H.HouseCode,
           H.BuyerCode,
           COALESCE(H.IndirectAmount, 0) AS IndAmt,
           COALESCE(H.DirectAmount,   0) AS DirAmt
    FROM dbo.PO_Header H
    JOIN @PoNumbers P ON P.PoNumber = H.PoNumber
    WHERE H.IsActive = 1 AND H.[Status] = 'W'
  ),

  /* =========================
     INDIRECT
     ========================= */
  I_Params AS (
    SELECT AP.IndirectSplitAt AS PivotAmt FROM #AP AP
  ),
  I_Low AS (
    SELECT S.PoNumber, D.[Level] AS RoleCode, CAST('I' AS char(1)) AS Category, D.Amount AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN I_Params IP
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON D.Amount < IP.PivotAmt
     AND (
           (S.IndAmt <  IP.PivotAmt AND D.Amount <= S.IndAmt)
        OR (S.IndAmt >= IP.PivotAmt)
         )
    WHERE S.IndAmt > 0
  ),
  I_Main AS (
    SELECT S.PoNumber, D.[Level] AS RoleCode, CAST('I' AS char(1)) AS Category, D.Amount AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN I_Params IP
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON S.IndAmt >= IP.PivotAmt
     AND D.Amount BETWEEN IP.PivotAmt AND S.IndAmt
  ),
  I_Promote AS (
    SELECT S.PoNumber,
           DN.[Level]           AS RoleCode,
           CAST('I' AS char(1)) AS Category,
           DN.Amount            AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN I_Params IP
    OUTER APPLY (
      SELECT MIN(Amount) AS NextAmt
      FROM dbo.PO_DelegationOfAuthority_Indirect_Expense
      WHERE Amount > S.IndAmt
    ) NX
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense DN
      ON DN.Amount = NX.NextAmt
    WHERE S.IndAmt >= IP.PivotAmt
      AND NX.NextAmt IS NOT NULL
      AND (
            CONVERT(decimal(19,2), NX.NextAmt - S.IndAmt) = 0.01
         OR S.IndAmt >= 135000.00
          )
  ),
  I_CFOBoost AS (
    SELECT S.PoNumber,
           D.[Level]            AS RoleCode,
           CAST('I' AS char(1)) AS Category,
           D.Amount             AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN I_Params IP
    JOIN dbo.PO_DelegationOfAuthority_Indirect_Expense D
      ON D.Amount = 135000.00
    WHERE S.IndAmt >= IP.PivotAmt
      AND S.IndAmt < 135000.00
  ),
  I_Ladder AS (
    SELECT * FROM I_Low
    UNION ALL
    SELECT * FROM I_Main
    UNION ALL
    SELECT * FROM I_Promote
    UNION ALL
    SELECT * FROM I_CFOBoost
  ),

  /* =======================
     DIRECT
     ======================= */
  D_Ceil AS (
    SELECT S.PoNumber, MIN(D.Amount) AS CeilingAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= AP.DirectMinAt
     AND D.Amount >= S.DirAmt
    GROUP BY S.PoNumber

    UNION ALL

    SELECT S.PoNumber, MAX(D.Amount) AS CeilingAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material D
      ON S.DirAmt >= AP.DirectMinAt
    WHERE NOT EXISTS (SELECT 1 FROM dbo.PO_DelegationOfAuthority_Direct_Material D2 WHERE D2.Amount >= S.DirAmt)
    GROUP BY S.PoNumber
  ),
  D_Base AS (
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
  D_Promote AS (
    SELECT S.PoNumber,
           DN.[Level]           AS RoleCode,
           CAST('D' AS char(1)) AS Category,
           DN.Amount            AS ThresholdAmt
    FROM ScopePO S
    CROSS JOIN #AP AP
    OUTER APPLY (
      SELECT MIN(Amount) AS NextAboveCeil
      FROM dbo.PO_DelegationOfAuthority_Direct_Material
      WHERE Amount >
        (
          SELECT MIN(Amount)
          FROM dbo.PO_DelegationOfAuthority_Direct_Material
          WHERE Amount >= S.DirAmt
        )
    ) NX
    JOIN dbo.PO_DelegationOfAuthority_Direct_Material DN
      ON DN.Amount = NX.NextAboveCeil
    WHERE S.DirAmt >= AP.DirectMinAt
      AND NX.NextAboveCeil IS NOT NULL
      AND (
            CONVERT(decimal(19,2),
              (SELECT MIN(Amount) FROM dbo.PO_DelegationOfAuthority_Direct_Material WHERE Amount >= S.DirAmt) - S.DirAmt
          ) = 0.01
           OR S.DirAmt >= AP.DirectStartAt
          )
  ),
  D_Ladder AS (
    SELECT * FROM D_Base
    UNION ALL
    SELECT * FROM D_Promote
  ),

  /* ---------- Order & de-dupe ---------- */
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
             ) THEN NULL
             ELSE K.Category
           END AS Category,
           K.ThresholdAmt
    FROM OrderedUnion K
    WHERE K.KeepOne = 1
  )
  INSERT INTO #Build (PoNumber, RoleCode, Category, ThresholdAmt, ApproverUserId)
  SELECT
      K.PoNumber,
      K.RoleCode,
      K.Category,
      K.ThresholdAmt,
      NULL AS ApproverUserId
  FROM Kept K;

  /* Final ordering & MERGE */
  ;WITH SeqBase AS (
    SELECT B.*,
           CASE WHEN B.Category = 'I' THEN 1
                WHEN B.Category = 'D' THEN 2
                ELSE 1
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
    ON T.PoNumber   = S.PoNumber
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
-- =============================================================================
CREATE PROCEDURE dbo.PO_IngestAndBuild
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @New dbo.PoNumberList;
  DECLARE @MaxOutboxIdBefore bigint;

  -- Snapshot outbox state BEFORE merge
  SELECT @MaxOutboxIdBefore = ISNULL(MAX(O.OutboxId), 0)
  FROM dbo.PO_ApprovalOutbox O;

  EXEC dbo.PO_Merge;

  /* 1) Collect ONLY newly created outbox rows from this run */
  INSERT INTO @New(PoNumber)
  SELECT DISTINCT O.PoNumber
  FROM dbo.PO_ApprovalOutbox O
  JOIN dbo.PO_Header H ON H.PoNumber = O.PoNumber
  WHERE O.OutboxId > @MaxOutboxIdBefore
    AND O.EventType = 'PO_NEW_WAITING'
    AND O.ProcessedAtUtc IS NULL
    AND H.IsActive = 1
    AND H.[Status] = 'W';

  /* 2) Catch-up: outbox exists but stages are missing */
  ;WITH MissingStages AS
  (
    SELECT DISTINCT O.PoNumber
    FROM dbo.PO_ApprovalOutbox O
    JOIN dbo.PO_Header H ON H.PoNumber = O.PoNumber
    WHERE O.EventType = 'PO_NEW_WAITING'
      AND O.ProcessedAtUtc IS NULL
      AND H.IsActive = 1
      AND H.[Status] = 'W'
      AND NOT EXISTS
      (
        SELECT 1
        FROM dbo.PO_ApprovalStage S
        WHERE S.PoNumber = O.PoNumber
      )
  )
  INSERT INTO @New(PoNumber)
  SELECT MS.PoNumber
  FROM MissingStages MS
  WHERE NOT EXISTS
  (
    SELECT 1
    FROM @New N
    WHERE N.PoNumber = MS.PoNumber
  );

  /* 3) Build approval stages */
  IF EXISTS (SELECT 1 FROM @New)
  BEGIN
    EXEC dbo.PO_BuildApprovalStages @PoNumbers = @New, @Rebuild = 0;
  END
END
GO


-- Staging truncate helper
CREATE PROCEDURE dbo.PO_Staging_Truncate
AS
BEGIN
  SET NOCOUNT ON;
  TRUNCATE TABLE dbo.PO_Stg_Line;
  TRUNCATE TABLE dbo.PO_Stg_Header;
END
GO


-- Direct replace
CREATE PROCEDURE dbo.PO_DoA_Direct_Replace
  @Rows dbo.DoA_DirectType READONLY
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRAN;
    TRUNCATE TABLE dbo.PO_DelegationOfAuthority_Direct_Material;
    INSERT INTO dbo.PO_DelegationOfAuthority_Direct_Material ([Level], Amount)
    SELECT [Level], Amount FROM @Rows;
  COMMIT;
END
GO

-- Indirect replace
CREATE PROCEDURE dbo.PO_DoA_Indirect_Replace
  @Rows dbo.DoA_IndirectType READONLY
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRAN;
    TRUNCATE TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense;
    INSERT INTO dbo.PO_DelegationOfAuthority_Indirect_Expense ([Level], Amount)
    SELECT [Level], Amount FROM @Rows;
  COMMIT;
END
GO


/*=============================================================================
  6) Final message
=============================================================================*/
PRINT 'DEV rebuild complete: PO approvals objects created in dbo schema.';
GO
