USE [WebappsDev];
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

CREATE TABLE dbo.PO_ApprovalOutbox
(
  OutboxId        BIGINT IDENTITY(1,1) PRIMARY KEY,
  EventType       NVARCHAR(40)  NOT NULL,               -- e.g. 'PO_NEW_WAITING'
  PoNumber        NVARCHAR(20)  NOT NULL,
  OccurredAtUtc   DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
  PayloadJson     NVARCHAR(MAX) NULL,                   -- optional: buyer, house, amounts
  Attempts        INT           NOT NULL DEFAULT(0),
  ProcessedAtUtc  DATETIME2(0)  NULL
);
-- Idempotency for *unprocessed* events of same type/PO
CREATE UNIQUE INDEX UX_PO_ApprovalOutbox_Unprocessed
  ON dbo.PO_ApprovalOutbox(EventType, PoNumber)
  WHERE ProcessedAtUtc IS NULL;

CREATE INDEX IX_PO_ApprovalOutbox_Queued
  ON dbo.PO_ApprovalOutbox(ProcessedAtUtc, Attempts);
