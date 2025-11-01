USE [WebappsDev];
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* =========================
   TABLE: dbo.PO_Header
   ========================= */
IF OBJECT_ID('dbo.PO_Header','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Header (
        PoHeaderId         bigint IDENTITY(1,1) NOT NULL,
        PoNumber           nvarchar(20) NOT NULL,
        PoDate             date NULL,
        VendorNumber       nvarchar(20) NOT NULL,
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
        IsActive           bit NOT NULL,
        DeactivatedAtUtc   datetime2(0) NULL,
        DeactivatedBy      nvarchar(64) NULL,
        DeactivationReason nvarchar(200) NULL,
        [Status]           char(1) NOT NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_Header')
    ALTER TABLE dbo.PO_Header ADD CONSTRAINT PK_PO_Header PRIMARY KEY CLUSTERED (PoHeaderId);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_Header_IsActive')
    ALTER TABLE dbo.PO_Header ADD CONSTRAINT DF_PO_Header_IsActive DEFAULT(1) FOR IsActive;
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_Header_Status')
    ALTER TABLE dbo.PO_Header ADD CONSTRAINT DF_PO_Header_Status DEFAULT('W') FOR [Status];
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='UQ' AND [name]='UQ_PO_Header_PoNumber')
    ALTER TABLE dbo.PO_Header ADD CONSTRAINT UQ_PO_Header_PoNumber UNIQUE (PoNumber);
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE [name]='CK_PO_Header_Status')
    ALTER TABLE dbo.PO_Header ADD CONSTRAINT CK_PO_Header_Status CHECK ([Status] IN ('W','A','D'));
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_Header_StatusActive')
    CREATE INDEX IX_PO_Header_StatusActive ON dbo.PO_Header ([Status], IsActive)
    INCLUDE (PoNumber, PoDate, VendorName, BuyerName, HouseCode, DirectAmount, IndirectAmount);
GO

/* =========================
   TABLE: dbo.PO_Line
   ========================= */
IF OBJECT_ID('dbo.PO_Line','U') IS NULL
BEGIN
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
        IsActive           bit NOT NULL,
        DeactivatedAtUtc   datetime2(0) NULL,
        DeactivatedBy      nvarchar(64) NULL,
        DeactivationReason nvarchar(200) NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_Line')
    ALTER TABLE dbo.PO_Line ADD CONSTRAINT PK_PO_Line PRIMARY KEY CLUSTERED (PoLineId);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_Line_IsActive')
    ALTER TABLE dbo.PO_Line ADD CONSTRAINT DF_PO_Line_IsActive DEFAULT(1) FOR IsActive;
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name]='FK_PO_Line_Header_PoHeaderId')
    ALTER TABLE dbo.PO_Line ADD CONSTRAINT FK_PO_Line_Header_PoHeaderId
        FOREIGN KEY (PoHeaderId) REFERENCES dbo.PO_Header(PoHeaderId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='UX_PO_Line_Po_LineNumber')
    CREATE UNIQUE INDEX UX_PO_Line_Po_LineNumber ON dbo.PO_Line (PoNumber, LineNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_Line_PoHeaderId')
    CREATE INDEX IX_PO_Line_PoHeaderId ON dbo.PO_Line (PoHeaderId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_Line_Po_IsActive')
    CREATE INDEX IX_PO_Line_Po_IsActive ON dbo.PO_Line (PoNumber, IsActive)
    INCLUDE (LineNumber, ItemDescription, ExtendedCost);
GO

/* =========================
   TABLE: dbo.PO_Approval_Audit
   ========================= */
IF OBJECT_ID('dbo.PO_Approval_Audit','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Approval_Audit (
        AuditId      bigint IDENTITY(1,1) NOT NULL,
        PoNumber     nvarchar(20) NOT NULL,
        OldStatus    char(1) NOT NULL,
        NewStatus    char(1) NOT NULL,
        ChangedBy    nvarchar(100) NOT NULL,
        ChangedAtUtc datetime2(0) NOT NULL,
        DecisionNote nvarchar(4000) NULL,
        [Sequence]   int NULL,
        RoleCode     nvarchar(40) NULL,
        Category     char(1) NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_Approval_Audit')
    ALTER TABLE dbo.PO_Approval_Audit ADD CONSTRAINT PK_PO_Approval_Audit PRIMARY KEY CLUSTERED (AuditId);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_Approval_Audit_ChangedAtUtc')
    ALTER TABLE dbo.PO_Approval_Audit ADD CONSTRAINT DF_PO_Approval_Audit_ChangedAtUtc DEFAULT (sysutcdatetime()) FOR ChangedAtUtc;
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE [name]='CK_PO_Approval_Audit_Status')
    ALTER TABLE dbo.PO_Approval_Audit ADD CONSTRAINT CK_PO_Approval_Audit_Status
        CHECK (OldStatus IN ('P','A','D','S',' ') AND NewStatus IN ('P','A','D','S'));
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_Approval_Audit_PO_Stage')
    CREATE INDEX IX_PO_Approval_Audit_PO_Stage
        ON dbo.PO_Approval_Audit (PoNumber, [Sequence], RoleCode, ChangedAtUtc);
GO

/* =========================
   TABLE: dbo.PO_ApprovalChain
   ========================= */
IF OBJECT_ID('dbo.PO_ApprovalChain','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_ApprovalChain (
        PoNumber       nvarchar(20) NOT NULL,
        CreatedAtUtc   datetime2(0) NOT NULL,
        [Status]       char(1) NOT NULL,
        FinalizedAtUtc datetime2(0) NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_ApprovalChain')
    ALTER TABLE dbo.PO_ApprovalChain ADD CONSTRAINT PK_PO_ApprovalChain PRIMARY KEY CLUSTERED (PoNumber);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_ApprovalChain_CreatedAtUtc')
    ALTER TABLE dbo.PO_ApprovalChain ADD CONSTRAINT DF_PO_ApprovalChain_CreatedAtUtc DEFAULT (sysutcdatetime()) FOR CreatedAtUtc;
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_ApprovalChain_Status')
    ALTER TABLE dbo.PO_ApprovalChain ADD CONSTRAINT DF_PO_ApprovalChain_Status DEFAULT('P') FOR [Status];
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE [name]='CK_PO_ApprovalChain_Status')
    ALTER TABLE dbo.PO_ApprovalChain ADD CONSTRAINT CK_PO_ApprovalChain_Status CHECK ([Status] IN ('P','A','D'));
GO

/* =========================
   TABLE: dbo.PO_ApprovalStage
   ========================= */
IF OBJECT_ID('dbo.PO_ApprovalStage','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_ApprovalStage (
        PoNumber        nvarchar(20) NOT NULL,
        [Sequence]      int NOT NULL,
        RoleCode        nvarchar(50) NOT NULL,
        ApproverUserId  nvarchar(100) NULL,
        Category        char(1) NULL, -- 'I','D', or NULL
        ThresholdFrom   decimal(18,2) NULL,
        ThresholdTo     decimal(18,2) NULL,
        [Status]        char(1) NOT NULL,
        DecidedAtUtc    datetime2(0) NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_ApprovalStage')
    ALTER TABLE dbo.PO_ApprovalStage ADD CONSTRAINT PK_PO_ApprovalStage PRIMARY KEY CLUSTERED (PoNumber, [Sequence]);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_ApprovalStage_Status')
    ALTER TABLE dbo.PO_ApprovalStage ADD CONSTRAINT DF_PO_ApprovalStage_Status DEFAULT('P') FOR [Status];
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE [name]='CK_PO_ApprovalStage_Status')
    ALTER TABLE dbo.PO_ApprovalStage ADD CONSTRAINT CK_PO_ApprovalStage_Status CHECK ([Status] IN ('P','A','D','S'));
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE [name]='CK_PO_ApprovalStage_Category')
    ALTER TABLE dbo.PO_ApprovalStage ADD CONSTRAINT CK_PO_ApprovalStage_Category CHECK (Category IN ('I','D') OR Category IS NULL);
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name]='FK_PO_ApprovalStage_Chain')
    ALTER TABLE dbo.PO_ApprovalStage ADD CONSTRAINT FK_PO_ApprovalStage_Chain
        FOREIGN KEY (PoNumber) REFERENCES dbo.PO_ApprovalChain(PoNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_ApprovalStage_Po_Status_Seq')
    CREATE INDEX IX_PO_ApprovalStage_Po_Status_Seq
        ON dbo.PO_ApprovalStage (PoNumber, [Status], [Sequence]);
GO

/* =========================
   TABLE: dbo.PO_ApprovalOutbox
   ========================= */
IF OBJECT_ID('dbo.PO_ApprovalOutbox','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_ApprovalOutbox (
        OutboxId       bigint IDENTITY(1,1) NOT NULL,
        EventType      nvarchar(40) NOT NULL,
        PoNumber       nvarchar(20) NOT NULL,
        OccurredAtUtc  datetime2(0) NOT NULL,
        PayloadJson    nvarchar(max) NULL,
        Attempts       int NOT NULL,
        ProcessedAtUtc datetime2(0) NULL,
        DirectAmount   decimal(18,2) NULL,
        IndirectAmount decimal(18,2) NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_ApprovalOutbox')
    ALTER TABLE dbo.PO_ApprovalOutbox ADD CONSTRAINT PK_PO_ApprovalOutbox PRIMARY KEY CLUSTERED (OutboxId);
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_ApprovalOutbox_Occurred')
    ALTER TABLE dbo.PO_ApprovalOutbox ADD CONSTRAINT DF_PO_ApprovalOutbox_Occurred DEFAULT (sysutcdatetime()) FOR OccurredAtUtc;
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE [name]='DF_PO_ApprovalOutbox_Attempts')
    ALTER TABLE dbo.PO_ApprovalOutbox ADD CONSTRAINT DF_PO_ApprovalOutbox_Attempts DEFAULT (0) FOR Attempts;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='UX_PO_ApprovalOutbox_Unprocessed')
    CREATE UNIQUE INDEX UX_PO_ApprovalOutbox_Unprocessed
        ON dbo.PO_ApprovalOutbox (EventType, PoNumber)
        WHERE ProcessedAtUtc IS NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_ApprovalOutbox_Queued')
    CREATE INDEX IX_PO_ApprovalOutbox_Queued
        ON dbo.PO_ApprovalOutbox (ProcessedAtUtc, Attempts);
GO

/* =========================
   TABLE: dbo.PO_ApproverDirectory
   ========================= */
IF OBJECT_ID('dbo.PO_ApproverDirectory','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_ApproverDirectory (
        RoleCode     nvarchar(50) NOT NULL,
        HouseCode    nvarchar(10) NOT NULL CONSTRAINT DF_PO_ApproverDirectory_House DEFAULT ('GLOBAL'),
        BuyerCode    nvarchar(10) NOT NULL CONSTRAINT DF_PO_ApproverDirectory_Buyer DEFAULT (N''),
        UserId       nvarchar(100) NOT NULL,
        DisplayName  nvarchar(120) NULL,
        Email        nvarchar(256) NULL,
        IsActive     bit NOT NULL CONSTRAINT DF_PO_ApproverDirectory_IsActive DEFAULT(1),
        UpdatedAtUtc datetime2(0) NOT NULL CONSTRAINT DF_PO_ApproverDirectory_UpdatedAtUtc DEFAULT (sysutcdatetime())
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_ApproverDirectory')
    ALTER TABLE dbo.PO_ApproverDirectory ADD CONSTRAINT PK_PO_ApproverDirectory PRIMARY KEY CLUSTERED (RoleCode, HouseCode, BuyerCode);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='IX_PO_ApproverDirectory_User')
    CREATE INDEX IX_PO_ApproverDirectory_User
        ON dbo.PO_ApproverDirectory (UserId, RoleCode, HouseCode, BuyerCode)
        INCLUDE (IsActive, Email);
GO

/* =========================
   TABLE: dbo.PO_DelegationOfAuthority_Direct_Material
   ========================= */
IF OBJECT_ID('dbo.PO_DelegationOfAuthority_Direct_Material','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_DelegationOfAuthority_Direct_Material (
        Id     int IDENTITY(1,1) NOT NULL,
        [Level] nvarchar(50) NOT NULL,
        Amount  decimal(19,4) NOT NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_DoA_Direct')
    ALTER TABLE dbo.PO_DelegationOfAuthority_Direct_Material ADD CONSTRAINT PK_PO_DoA_Direct PRIMARY KEY CLUSTERED (Id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='UX_PO_DoA_Direct_Level')
    CREATE UNIQUE INDEX UX_PO_DoA_Direct_Level ON dbo.PO_DelegationOfAuthority_Direct_Material ([Level]);
GO

/* =========================
   TABLE: dbo.PO_DelegationOfAuthority_Indirect_Expense
   ========================= */
IF OBJECT_ID('dbo.PO_DelegationOfAuthority_Indirect_Expense','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense (
        Id     int IDENTITY(1,1) NOT NULL,
        [Level] nvarchar(50) NOT NULL,
        Amount  decimal(19,4) NOT NULL
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [type]='PK' AND [name]='PK_PO_DoA_Indirect')
    ALTER TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense ADD CONSTRAINT PK_PO_DoA_Indirect PRIMARY KEY CLUSTERED (Id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name]='UX_PO_DoA_Indirect_Level')
    CREATE UNIQUE INDEX UX_PO_DoA_Indirect_Level ON dbo.PO_DelegationOfAuthority_Indirect_Expense ([Level]);
GO

/* =========================
   TABLE: dbo.PO_Stg_Header (staging)
   ========================= */
IF OBJECT_ID('dbo.PO_Stg_Header','U') IS NULL
BEGIN
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
END;
GO

/* =========================
   TABLE: dbo.PO_Stg_Line (staging)
   ========================= */
IF OBJECT_ID('dbo.PO_Stg_Line','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Stg_Line (
        PoNumber            nvarchar(20) NULL,
        LineNumber          int NULL,
        HouseCode           nvarchar(10) NULL,
        ItemNumber          nvarchar(40) NULL,
        ItemDescription     nvarchar(120) NULL,
        ItemShortDescription nvarchar(60) NULL,
        QuantityOrdered     decimal(18,4) NULL,
        OrderUom            nvarchar(12) NULL,
        UnitCost            decimal(18,4) NULL,
        ExtendedCost        decimal(18,4) NULL,
        RequiredDate        date NULL,
        GlAccount           nvarchar(40) NULL,
        CreatedAtUtc        datetime2(0) NULL
    );
END;
GO

/*===============================================================================
  PURPOSE:  Store the three policy cut-offs the stage builder relies on so that
            when Finance adjusts Delegation of Authority (DoA) ladders, we avoid
            editing code.

  TABLE:    dbo.PO_ApprovalPolicy

  WHAT THESE VALUES MEAN
  ----------------------
  1) IndirectSplitAt
     - Pivot for INDIRECT approvals.
       * If IndAmt < IndirectSplitAt:
           include all INDIRECT tiers strictly below this split (stop before it).
       * If IndAmt >= IndirectSplitAt:
           include all INDIRECT tiers at/above this split up to the tier <= IndAmt.

  2) DirectMinAt
     - Minimum DIRECT amount for any direct approval to apply.
       * If DirAmt < DirectMinAt: no DIRECT stages.
       * If DirAmt >= DirectMinAt: evaluate direct tiers.

  3) DirectStartAt
     - First DIRECT tier to include (skips Buyer at 50k on purpose).
       * For DirAmt >= DirectMinAt, include all DIRECT tiers from DirectStartAt
         up to the ceiling for DirAmt.

  WHEN DO THESE CHANGE?
  ---------------------
  - Most DoA changes (adding/removing tiers or changing tier amounts/levels) happen
    in the DoA tables themselves and DO NOT require changing these three cut-offs.
  - Change these only when Finance moves a policy "gate", e.g.:
      * IndirectSplitAt moves (e.g., 2k → 5k).
      * DirectMinAt moves (e.g., 50k → 75k).
      * DirectStartAt moves (e.g., LPM begins at 120k instead of 100k).

  WHAT TO DO WHEN FINANCE CHANGES DoA AMOUNTS
  -------------------------------------------
  1) If Finance ONLY updates the DoA ladders (rows/levels in DoA tables):
     - Load the new ladder rows into:
         dbo.PO_DelegationOfAuthority_Indirect_Expense
         dbo.PO_DelegationOfAuthority_Direct_Material
     - No change to this policy table unless a gate moved.

  2) If Finance moves a gate (split/min/start):
     - Insert a NEW policy row here with the new values and mark it IsActive = 1.
     - Set the previous active row to IsActive = 0 (kept for audit).
     - The stage builder procedure will read the single active row and reflect the change
       without code edits or redeploys.

  ENFORCEMENT
  -----------
  - A filtered unique index guarantees exactly one active policy row (IsActive = 1).
  - CHECK constraints ensure sensible relationships:
      * IndirectSplitAt >= 0
      * DirectMinAt     >= 0
      * DirectStartAt   >= DirectMinAt

  EXTENSION
  ------------------
  - If later we need per-HouseCode or per-BuyerCode gates, add those columns
    (nullable) and change the unique filtered index to include them still keeping
    one active policy per scope.

===============================================================================*/

IF OBJECT_ID(N'dbo.PO_ApprovalPolicy', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.PO_ApprovalPolicy
  (
    PolicyId         int            IDENTITY(1,1) NOT NULL
      CONSTRAINT PK_PO_ApprovalPolicy PRIMARY KEY,
    -- Scope columns reserved for future use; keep NULLs for a global policy
    HouseCode        nvarchar(10)   NULL,
    BuyerCode        nvarchar(10)   NULL,

    -- Three gate values used by the stage builder
    IndirectSplitAt  decimal(19,4)  NOT NULL,  -- e.g., 2000.0000
    DirectMinAt      decimal(19,4)  NOT NULL,  -- e.g., 50000.0000
    DirectStartAt    decimal(19,4)  NOT NULL,  -- e.g., 100000.0000

    -- Row state & audit
    IsActive         bit            NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_IsActive DEFAULT(1),
    EffectiveDate    date           NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_EffectiveDate DEFAULT (CONVERT(date, SYSUTCDATETIME())),
    UpdatedBy        nvarchar(100)  NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_UpdatedBy DEFAULT (SUSER_SNAME()),
    UpdatedAtUtc     datetime2(0)   NOT NULL CONSTRAINT DF_PO_ApprovalPolicy_UpdatedAtUtc DEFAULT (SYSUTCDATETIME()),
    Notes            nvarchar(400)  NULL,

    -- Basic sanity checks
    CONSTRAINT CK_PO_ApprovalPolicy_IndirectSplitAt_Positive CHECK (IndirectSplitAt >= 0),
    CONSTRAINT CK_PO_ApprovalPolicy_DirectMinAt_Positive     CHECK (DirectMinAt     >= 0),
    CONSTRAINT CK_PO_ApprovalPolicy_DirectStart_GTE_Min      CHECK (DirectStartAt   >= DirectMinAt)
  );

  -- Ensure only one active policy (globally). If we later scope by House/Buyer,
  -- replace this with a composite filtered unique index per scope.
  CREATE UNIQUE INDEX UX_PO_ApprovalPolicy_OneActive
    ON dbo.PO_ApprovalPolicy (IsActive)
    WHERE IsActive = 1;
END
GO

-- Active headers (waiting + active only)
CREATE OR ALTER VIEW dbo.vw_PO_Header_WaitingActive
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
  h.Status,          -- 'W','A','D' (but filtered to 'W')
  h.IsActive,        -- 1 (filtered)
  h.CreatedAtUtc,
  (SELECT COUNT(*) FROM dbo.PO_Line l WHERE l.PoNumber = h.PoNumber AND l.IsActive = 1) AS ActiveLineCount
FROM dbo.PO_Header h
WHERE h.Status = 'W' AND h.IsActive = 1;
GO

-- Active lines (for detail page)
CREATE OR ALTER VIEW dbo.vw_PO_Line_Active
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
  l.IsActive -- 1 (filtered)
FROM dbo.PO_Line l
WHERE l.IsActive = 1;
GO

/* =========================
   Done
   ========================= */
PRINT 'PO approvals tables and views deployment complete.';
