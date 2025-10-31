/*
Notes/assumptions (kept brief)

Cumulative policy: include every role at or below the PO’s amount on each ladder.

Order: Indirect stages first (ascending threshold), then Direct (ascending). If a role is required by both ladders, it appears once with Category = NULL.

Approver resolution: tries (RoleCode, HouseCode, BuyerCode) → (RoleCode, HouseCode, '') → (RoleCode, 'GLOBAL', '').

Chain seeding: if PO_ApprovalChain row is missing, the builder seeds it with Status='P'.

Idempotent: Re-running the orchestrator only (re)builds for newly waiting POs as indicated by outbox; the builder itself can also be called with @Rebuild=1 when you wish to fully recompute stages for a set of POs.

If you want the orchestrator to force-rebuild stages every run, change the last line to @Rebuild = 1.
*/





CREATE OR ALTER PROCEDURE dbo.PO_IngestAndBuild
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  /*
    Orchestrates:
      1) Run PO_Merge (unchanged).
      2) Identify newly waiting / reactivated POs from the Outbox rows PO_Merge just wrote:
            EventType = 'PO_NEW_WAITING' AND ProcessedAtUtc IS NULL
         (and confirmed in PO_Header as Status='W' and IsActive=1).
      3) Build (or rebuild) stages for just those POs (idempotent).
      NOTE: We do NOT mark Outbox.ProcessedAtUtc here; leave that to your async dispatcher.
  */

  DECLARE @New dbo.PoNumberList;

  BEGIN TRAN;

  EXEC dbo.PO_Merge;

  INSERT INTO @New(PoNumber)
  SELECT DISTINCT O.PoNumber
  FROM dbo.PO_ApprovalOutbox O
  JOIN dbo.PO_Header H
    ON H.PoNumber = O.PoNumber
  WHERE O.EventType = 'PO_NEW_WAITING'
    AND O.ProcessedAtUtc IS NULL
    AND H.IsActive = 1
    AND H.[Status] = 'W';

  COMMIT;

  IF EXISTS (SELECT 1 FROM @New)
  BEGIN
    -- Build (not forced rebuild): preserves any Approvals already taken if you re-run
    EXEC dbo.PO_BuildApprovalStages @PoNumbers = @New, @Rebuild = 0;
  END
END
