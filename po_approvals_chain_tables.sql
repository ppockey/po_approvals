-- Why: Top-level container for a PO’s approval lifecycle (Pending/Approved/Denied). Lets you finalize the chain cleanly when all stages complete or a denial occurs.
CREATE TABLE dbo.PO_ApprovalChain
(
  PoNumber       nvarchar(20)  NOT NULL,
  CreatedAtUtc   datetime2(0)  NOT NULL CONSTRAINT DF_PO_ApprovalChain_CreatedAtUtc DEFAULT (sysutcdatetime()),
  Status         char(1)       NOT NULL CONSTRAINT DF_PO_ApprovalChain_Status DEFAULT ('P'), -- P/A/D
  FinalizedAtUtc datetime2(0)  NULL,
  CONSTRAINT PK_PO_ApprovalChain PRIMARY KEY (PoNumber)
);

-- Why: Stores the computed chain (roles in order). Your job inserts these once per PO; the frontend flips Status on user action, and you can advance to the next pending stage.
CREATE TABLE dbo.PO_ApprovalStage
(
  PoNumber        nvarchar(20)  NOT NULL,
  Sequence        int           NOT NULL,                          -- 1..N in approval order
  RoleCode        nvarchar(40)  NOT NULL,                          -- 'LPM','GM','SFC','VP', etc.
  ApproverUserId  nvarchar(100) NULL,                              -- resolved person (optional)
  Category        char(1)       NULL,                              -- 'I'/'D'/NULL (if you annotate)
  ThresholdFrom   decimal(18,2) NULL,
  ThresholdTo     decimal(18,2) NULL,
  Status          char(1)       NOT NULL CONSTRAINT DF_PO_ApprovalStage_Status DEFAULT ('P'), -- P/A/D/S
  DecidedAtUtc    datetime2(0)  NULL,

  CONSTRAINT PK_PO_ApprovalStage PRIMARY KEY (PoNumber, Sequence),
  CONSTRAINT FK_PO_ApprovalStage_Chain FOREIGN KEY (PoNumber)
    REFERENCES dbo.PO_ApprovalChain(PoNumber)
);

-- Fast "next stage" lookups and dashboards
CREATE INDEX IX_PO_ApprovalStage_Po_Status_Seq
  ON dbo.PO_ApprovalStage(PoNumber, Status, Sequence);

-- Optional: ensure sequence uniqueness per PO (covered by PK)
-- Optional: prevent duplicate roles per PO (uncomment if desired)
-- CREATE UNIQUE INDEX UX_PO_ApprovalStage_Po_Role ON dbo.PO_ApprovalStage(PoNumber, RoleCode);


-- Why: The chain stores roles; this table maps a role to the actual approver for a given site/house (and optionally buyer). Your job (or API) can resolve ApproverUserId and email.
CREATE TABLE dbo.PO_ApproverDirectory
(
  RoleCode     nvarchar(40)  NOT NULL,
  HouseCode    nvarchar(10)  NOT NULL DEFAULT('GLOBAL'),  -- sentinel for site-agnostic
  BuyerCode    nvarchar(10)  NOT NULL DEFAULT(''),
  UserId       nvarchar(100) NOT NULL,
  DisplayName  nvarchar(120) NULL,
  Email        nvarchar(256) NULL,
  IsActive     bit           NOT NULL CONSTRAINT DF_PO_ApproverDirectory_IsActive DEFAULT (1),
  UpdatedAtUtc datetime2(0)  NOT NULL CONSTRAINT DF_PO_ApproverDirectory_UpdatedAtUtc DEFAULT (sysutcdatetime()),
  CONSTRAINT PK_PO_ApproverDirectory PRIMARY KEY (RoleCode, HouseCode, BuyerCode)
);

CREATE INDEX IX_PO_ApproverDirectory_Role  ON dbo.PO_ApproverDirectory(RoleCode);
CREATE INDEX IX_PO_ApproverDirectory_Scope ON dbo.PO_ApproverDirectory(HouseCode, BuyerCode);

  

-- Why: The outbox triggers chain creation and holds the decisive amounts for rules. Your periodic job consumes unprocessed rows, builds the chain, and marks them processed.
CREATE TABLE dbo.PO_ApprovalOutbox
(
  OutboxId        bigint         IDENTITY(1,1) PRIMARY KEY,
  EventType       nvarchar(40)   NOT NULL,                -- 'PO_NEW_WAITING', etc.
  PoNumber        nvarchar(20)   NOT NULL,
  OccurredAtUtc   datetime2(0)   NOT NULL CONSTRAINT DF_PO_ApprovalOutbox_Occurred DEFAULT (sysutcdatetime()),
  PayloadJson     nvarchar(max)  NULL,
  Attempts        int            NOT NULL CONSTRAINT DF_PO_ApprovalOutbox_Attempts DEFAULT (0),
  ProcessedAtUtc  datetime2(0)   NULL,
  DirectAmount    decimal(18,2)  NULL,                    -- <— new
  IndirectAmount  decimal(18,2)  NULL                     -- <— new
);

-- One unprocessed event per (EventType, PoNumber)
CREATE UNIQUE INDEX UX_PO_ApprovalOutbox_Unprocessed
  ON dbo.PO_ApprovalOutbox(EventType, PoNumber)
  WHERE ProcessedAtUtc IS NULL;

-- Supports queue scans and retry policies
CREATE INDEX IX_PO_ApprovalOutbox_Queued
  ON dbo.PO_ApprovalOutbox(ProcessedAtUtc, Attempts);
