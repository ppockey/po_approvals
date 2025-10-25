USE [WebappsDev];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

/* =========================
   FINAL TABLES (dbo)
   ========================= */
IF OBJECT_ID('dbo.PO_Header','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Header
    (
        PoHeaderId        BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PO_Header PRIMARY KEY,
        PoNumber          NVARCHAR(20) NOT NULL,
        PoDate            DATE NULL,
        VendorNumber      NVARCHAR(20) NOT NULL,
        VendorName        NVARCHAR(80) NULL,
        VendorAddr1       NVARCHAR(80) NULL,
        VendorAddr2       NVARCHAR(80) NULL,
        VendorAddr3       NVARCHAR(80) NULL,
        VendorState       NVARCHAR(20) NULL,
        VendorPostalCode  NVARCHAR(20) NULL,
        BuyerCode         NVARCHAR(10) NULL,
        BuyerName         NVARCHAR(60) NULL,
        HouseCode         NVARCHAR(10) NULL,
        DirectAmount      DECIMAL(18,2) NULL,
        IndirectAmount    DECIMAL(18,2) NULL,
        CreatedAtUtc      DATETIME2(0) NULL,
        CONSTRAINT UQ_PO_Header_PoNumber UNIQUE (PoNumber)
    );
END;
GO

IF OBJECT_ID('dbo.PO_Line','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Line
    (
        PoLineId             BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_PO_Line PRIMARY KEY,
        PoNumber             NVARCHAR(20) NOT NULL,
        LineNumber           INT NOT NULL,
        HouseCode            NVARCHAR(10) NULL,
        ItemNumber           NVARCHAR(40) NULL,
        ItemDescription      NVARCHAR(120) NULL,
        ItemShortDescription NVARCHAR(60) NULL,
        QuantityOrdered      DECIMAL(18,4) NULL,
        OrderUom             NVARCHAR(12) NULL,
        UnitCost             DECIMAL(18,4) NULL,
        ExtendedCost         DECIMAL(18,4) NULL,
        RequiredDate         DATE NULL,
        GlAccount            NVARCHAR(40) NULL,
        CONSTRAINT UQ_PO_Line_Po_Line UNIQUE (PoNumber, LineNumber),
        CONSTRAINT FK_PO_Line_Header_PoNumber FOREIGN KEY (PoNumber) REFERENCES dbo.PO_Header(PoNumber)
    );
END;
GO

/* =========================
   STAGING TABLES (dbo)
   ========================= */
IF OBJECT_ID('dbo.PO_Stg_Header','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Stg_Header
    (
        PoNumber          NVARCHAR(20) NULL,
        PoDate            DATE NULL,
        VendorNumber      NVARCHAR(20) NULL,
        VendorName        NVARCHAR(80) NULL,
        VendorAddr1       NVARCHAR(80) NULL,
        VendorAddr2       NVARCHAR(80) NULL,
        VendorAddr3       NVARCHAR(80) NULL,
        VendorState       NVARCHAR(20) NULL,
        VendorPostalCode  NVARCHAR(20) NULL,
        BuyerCode         NVARCHAR(10) NULL,
        BuyerName         NVARCHAR(60) NULL,
        HouseCode         NVARCHAR(10) NULL,
        DirectAmount      DECIMAL(18,2) NULL,
        IndirectAmount    DECIMAL(18,2) NULL,
        CreatedAtUtc      DATETIME2(0) NULL
    );
END;
GO

IF OBJECT_ID('dbo.PO_Stg_Line','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_Stg_Line
    (
        PoNumber             NVARCHAR(20) NULL,
        LineNumber           INT NULL,
        HouseCode            NVARCHAR(10) NULL,
        ItemNumber           NVARCHAR(40) NULL,
        ItemDescription      NVARCHAR(120) NULL,
        ItemShortDescription NVARCHAR(60) NULL,
        QuantityOrdered      DECIMAL(18,4) NULL,
        OrderUom             NVARCHAR(12) NULL,
        UnitCost             DECIMAL(18,4) NULL,
        ExtendedCost         DECIMAL(18,4) NULL,
        RequiredDate         DATE NULL,
        GlAccount            NVARCHAR(40) NULL
    );
END;
GO

/* =========================
   Helper Indexes (dynamic SQL to avoid IntelliSense squiggles)
   ========================= */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PO_Header_VendorNumber' AND object_id=OBJECT_ID('dbo.PO_Header'))
    EXEC('CREATE INDEX IX_PO_Header_VendorNumber ON dbo.PO_Header(VendorNumber);');

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PO_Line_ItemNumber' AND object_id=OBJECT_ID('dbo.PO_Line'))
    EXEC('CREATE INDEX IX_PO_Line_ItemNumber ON dbo.PO_Line(ItemNumber);');

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_PO_Line_GlAccount' AND object_id=OBJECT_ID('dbo.PO_Line'))
    EXEC('CREATE INDEX IX_PO_Line_GlAccount ON dbo.PO_Line(GlAccount);');
GO



-- add soft delete
ALTER TABLE dbo.PO_Header ADD
    IsActive          bit NOT NULL CONSTRAINT DF_PO_Header_IsActive DEFAULT(1),
    DeactivatedAtUtc  datetime2(0) NULL,
    DeactivatedBy     nvarchar(64) NULL,
    DeactivationReason nvarchar(200) NULL;

ALTER TABLE dbo.PO_Line ADD
    IsActive          bit NOT NULL CONSTRAINT DF_PO_Line_IsActive DEFAULT(1),
    DeactivatedAtUtc  datetime2(0) NULL,
    DeactivatedBy     nvarchar(64) NULL,
    DeactivationReason nvarchar(200) NULL;






/* Quick verification */
SELECT name FROM sys.tables WHERE name IN ('PO_Header','PO_Line','PO_Stg_Header','PO_Stg_Line');
-- EXEC dbo.PO_Merge; -- Optional test run



-- Delegation of Authority look-up tables
IF OBJECT_ID('dbo.PO_DelegationOfAuthority_Direct_Material','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_DelegationOfAuthority_Direct_Material
    (
        Id     int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [Level] nvarchar(50) NOT NULL UNIQUE,
        Amount decimal(19,4) NOT NULL
    );
END

IF OBJECT_ID('dbo.PO_DelegationOfAuthority_Indirect_Expense','U') IS NULL
BEGIN
    CREATE TABLE dbo.PO_DelegationOfAuthority_Indirect_Expense
    (
        Id     int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [Level] nvarchar(50) NOT NULL UNIQUE,
        Amount decimal(19,4) NOT NULL
    );
END


---- prepare for frontend support
/* 1.1 Add decision + sync columns and optimistic concurrency */
ALTER TABLE dbo.PO_Header
ADD [Status]           char(1) NOT NULL CONSTRAINT DF_PO_Header_Status DEFAULT('W'), -- W|A|D
    StatusChangedAtUtc datetime2(0) NULL,
    StatusChangedBy    nvarchar(100) NULL,
    DecisionNote       nvarchar(4000) NULL,
    PrmsSyncStatus     varchar(20) NOT NULL CONSTRAINT DF_PO_Header_PrmsSyncStatus DEFAULT('Pending'), -- Pending|Succeeded|Failed
    PrmsSyncError      nvarchar(2000) NULL,
    RowVersion         rowversion;

CREATE INDEX IX_PO_Header_Status ON dbo.PO_Header([Status]);
CREATE INDEX IX_PO_Header_PrmsSyncStatus ON dbo.PO_Header(PrmsSyncStatus);

/* 1.2 Immutable audit trail */
IF OBJECT_ID('dbo.PO_Approval_Audit','U') IS NULL
BEGIN
  CREATE TABLE dbo.PO_Approval_Audit
  (
    AuditId          bigint IDENTITY(1,1) PRIMARY KEY,
    PoNumber         nvarchar(20) NOT NULL,
    OldStatus        char(1)      NOT NULL,
    NewStatus        char(1)      NOT NULL,
    ChangedBy        nvarchar(100) NOT NULL,
    ChangedAtUtc     datetime2(0)  NOT NULL CONSTRAINT DF_PO_Approval_Audit_ChangedAtUtc DEFAULT (sysutcdatetime()),
    DecisionNote     nvarchar(4000) NULL
  );
  CREATE INDEX IX_PO_Approval_Audit_Po ON dbo.PO_Approval_Audit(PoNumber, ChangedAtUtc DESC);
END
GO


truncate table dbo.PO_ApprovalOutbox
select * from dbo.PO_ApprovalOutbox

truncate table [dbo].[PO_DelegationOfAuthority_Direct_Material]
truncate table [dbo].[PO_DelegationOfAuthority_Indirect_Expense]

select [Amount], [Level] from dbo.PO_DelegationOfAuthority_Direct_Material
select [Amount], [Level] from dbo.PO_DelegationOfAuthority_Indirect_Expense order by Amount

select * from dbo.PO_Header a 
inner join  dbo.PO_Line b 
on a.PoNumber = b.PoNumber
order by a.PoNumber

select count(1) from dbo.PO_Header
select * from dbo.PO_Header

select count(1) from dbo.PO_Line

select count(1) from dbo.PO_Stg_Header
select count(1) from dbo.PO_Stg_Line


truncate table dbo.PO_Stg_Line
truncate table dbo.PO_Stg_Header

delete from dbo.PO_Line
delete from dbo.PO_Header

truncate table dbo.PO_Approval_Audit
select * from dbo.PO_Approval_Audit


-- Logs
select top 1000 * from Nlog.dbo.Logs where CreatedOn > '2025-10-04T00:00:00' order by Id desc

-- po and po lines logs
select [Message], * from Nlog.dbo.Logs where [Message] like '%ITTPortal.POApprovals%' or [Message] like '%ITTPortal.Infrastructure.Repositories.PoApprovalsStagingRepository%' order by Id asc
-- delegation of authority logs
select [Message], * from Nlog.dbo.Logs where [Message] like '%ITTPortal.Infrastructure.Repositories.PoApprovalsDelegationOfAuthorityRepository%' or [Message] like '%ITTPortal.POApprovals.Services.FetchDelegationOfAuthorityJob%' order by Id asc

delete from Nlog.dbo.Logs where [Message] like '%ITTPortal.POApprovals%' or [Message] like '%ITTPortal.Infrastructure.Repositories.PoApprovalsStagingRepository%' or [Message] like '%ITTPortal.Infrastructure.Repositories.PoApprovalsDelegationOfAuthorityRepository%'


select * from dbo.Usr where Email = 'Philip.Pockey@itt.com'

select * from dbo.UserRole where UserId = '5896'