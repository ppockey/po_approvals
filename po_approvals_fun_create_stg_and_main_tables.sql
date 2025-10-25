USE [WebappsDev];
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

/* =========================
   DROP (idempotent, dev only)
   ========================= */
IF OBJECT_ID('dbo.PO_Line','U')   IS NOT NULL DROP TABLE dbo.PO_Line;
IF OBJECT_ID('dbo.PO_Header','U') IS NOT NULL DROP TABLE dbo.PO_Header;
IF OBJECT_ID('dbo.PO_Stg_Line','U')   IS NOT NULL DROP TABLE dbo.PO_Stg_Line;
IF OBJECT_ID('dbo.PO_Stg_Header','U') IS NOT NULL DROP TABLE dbo.PO_Stg_Header;
GO

/* =========================
   FINAL TABLES (dbo)
   ========================= */
CREATE TABLE dbo.PO_Header
(
    PoHeaderId        BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PO_Header PRIMARY KEY,
    PoNumber          NVARCHAR(20)  NOT NULL,
    PoDate            DATE          NULL,
    VendorNumber      NVARCHAR(20)  NOT NULL,
    VendorName        NVARCHAR(80)  NULL,
    VendorAddr1       NVARCHAR(80)  NULL,
    VendorAddr2       NVARCHAR(80)  NULL,
    VendorAddr3       NVARCHAR(80)  NULL,
    VendorState       NVARCHAR(20)  NULL,
    VendorPostalCode  NVARCHAR(20)  NULL,
    BuyerCode         NVARCHAR(10)  NULL,
    BuyerName         NVARCHAR(60)  NULL,
    HouseCode         NVARCHAR(10)  NULL,
    DirectAmount      DECIMAL(18,2) NULL,
    IndirectAmount    DECIMAL(18,2) NULL,
    CreatedAtUtc      DATETIME2(0)  NULL,

    -- Soft delete
    IsActive          BIT NOT NULL CONSTRAINT DF_PO_Header_IsActive DEFAULT(1),
    DeactivatedAtUtc  DATETIME2(0) NULL,
    DeactivatedBy     NVARCHAR(64) NULL,
    DeactivationReason NVARCHAR(200) NULL
);
GO

-- Filtered unique index enforces PoNumber uniqueness only for active rows
CREATE UNIQUE INDEX UX_PO_Header_PoNumber_Active
  ON dbo.PO_Header(PoNumber)
  WHERE IsActive = 1;
GO

-- Helpful nonclustered indexes
CREATE INDEX IX_PO_Header_VendorNumber ON dbo.PO_Header(VendorNumber);
CREATE INDEX IX_PO_Header_PoNumber     ON dbo.PO_Header(PoNumber); -- join helper for lookups
GO

CREATE TABLE dbo.PO_Line
(
    PoLineId             BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PO_Line PRIMARY KEY,

    -- FK to header via surrogate key (enables filtered unique index on PoNumber)
    PoHeaderId           BIGINT       NOT NULL,

    -- Keep PoNumber for convenience and filtered uniqueness among active lines
    PoNumber             NVARCHAR(20) NOT NULL,
    LineNumber           INT          NOT NULL,

    HouseCode            NVARCHAR(10)  NULL,
    ItemNumber           NVARCHAR(40)  NULL,
    ItemDescription      NVARCHAR(120) NULL,
    ItemShortDescription NVARCHAR(60)  NULL,
    QuantityOrdered      DECIMAL(18,4) NULL,
    OrderUom             NVARCHAR(12)  NULL,
    UnitCost             DECIMAL(18,4) NULL,
    ExtendedCost         DECIMAL(18,4) NULL,
    RequiredDate         DATE          NULL,
    GlAccount            NVARCHAR(40)  NULL,

    -- Soft delete
    IsActive             BIT NOT NULL CONSTRAINT DF_PO_Line_IsActive DEFAULT(1),
    DeactivatedAtUtc     DATETIME2(0) NULL,
    DeactivatedBy        NVARCHAR(64) NULL,
    DeactivationReason   NVARCHAR(200) NULL
);
GO

-- Referential integrity via surrogate key
ALTER TABLE dbo.PO_Line
  ADD CONSTRAINT FK_PO_Line_Header_PoHeaderId
  FOREIGN KEY (PoHeaderId) REFERENCES dbo.PO_Header(PoHeaderId);
GO

-- Filtered unique index: active (PoNumber, LineNumber) must be unique
CREATE UNIQUE INDEX UX_PO_Line_Po_Line_Active
  ON dbo.PO_Line(PoNumber, LineNumber)
  WHERE IsActive = 1;
GO

-- Helpful nonclustered indexes
CREATE INDEX IX_PO_Line_ItemNumber ON dbo.PO_Line(ItemNumber);
CREATE INDEX IX_PO_Line_GlAccount  ON dbo.PO_Line(GlAccount);
CREATE INDEX IX_PO_Line_PoNumber   ON dbo.PO_Line(PoNumber);
GO

/* =========================
   STAGING TABLES (dbo)
   ========================= */
CREATE TABLE dbo.PO_Stg_Header
(
    PoNumber          NVARCHAR(20)  NULL,
    PoDate            DATE          NULL,
    VendorNumber      NVARCHAR(20)  NULL,
    VendorName        NVARCHAR(80)  NULL,
    VendorAddr1       NVARCHAR(80)  NULL,
    VendorAddr2       NVARCHAR(80)  NULL,
    VendorAddr3       NVARCHAR(80)  NULL,
    VendorState       NVARCHAR(20)  NULL,
    VendorPostalCode  NVARCHAR(20)  NULL,
    BuyerCode         NVARCHAR(10)  NULL,
    BuyerName         NVARCHAR(60)  NULL,
    HouseCode         NVARCHAR(10)  NULL,
    DirectAmount      DECIMAL(18,2) NULL,
    IndirectAmount    DECIMAL(18,2) NULL,
    CreatedAtUtc      DATETIME2(0)  NULL   -- used for ROW_NUMBER() newest-wins
);
GO

CREATE TABLE dbo.PO_Stg_Line
(
    PoNumber             NVARCHAR(20)  NULL,
    LineNumber           INT           NULL,
    HouseCode            NVARCHAR(10)  NULL,
    ItemNumber           NVARCHAR(40)  NULL,
    ItemDescription      NVARCHAR(120) NULL,
    ItemShortDescription NVARCHAR(60)  NULL,
    QuantityOrdered      DECIMAL(18,4) NULL,
    OrderUom             NVARCHAR(12)  NULL,
    UnitCost             DECIMAL(18,4) NULL,
    ExtendedCost         DECIMAL(18,4) NULL,
    RequiredDate         DATE          NULL,
    GlAccount            NVARCHAR(40)  NULL,
    CreatedAtUtc         DATETIME2(0)  NULL   -- used for ROW_NUMBER() newest-wins
);
GO


ALTER TABLE dbo.PO_Header
  ADD Status char(1) NOT NULL CONSTRAINT DF_PO_Header_Status DEFAULT('W'); -- W=Waiting, A=Approved, D=Denied (example)

CREATE INDEX IX_PO_Header_Status ON dbo.PO_Header(Status);
