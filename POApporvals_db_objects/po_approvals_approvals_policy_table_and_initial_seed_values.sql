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

/* Seed the current active policy (GLOBAL scope)
   Current production assumptions:
     IndirectSplitAt =  2,000.00
     DirectMinAt     = 50,000.00
     DirectStartAt   = 100,000.00
*/
IF NOT EXISTS (SELECT 1 FROM dbo.PO_ApprovalPolicy WHERE IsActive = 1)
BEGIN
  INSERT INTO dbo.PO_ApprovalPolicy
    (HouseCode, BuyerCode, IndirectSplitAt, DirectMinAt, DirectStartAt, IsActive, Notes)
  VALUES
    (NULL, NULL, 2000.0000, 50000.0000, 100000.0000, 1, N'Initial global policy for PO approvals');
END
GO

/* Example: activating a new policy when Finance moves a gate (RUN WHEN NEEDED)
   - Deactivate the current row, insert a new active row with changed values.
   NOTE: This block is an example; do not run unless you intend to change policy.

-- 1) Deactivate current active
UPDATE dbo.PO_ApprovalPolicy
SET IsActive = 0, UpdatedAtUtc = SYSUTCDATETIME(), UpdatedBy = SUSER_SNAME()
WHERE IsActive = 1;

-- 2) Insert new active with revised gates
INSERT INTO dbo.PO_ApprovalPolicy
  (HouseCode, BuyerCode, IndirectSplitAt, DirectMinAt, DirectStartAt, IsActive, Notes)
VALUES
  (NULL, NULL, 5000.0000, 75000.0000, 150000.0000, 1, N'Finance update: raise indirect split to 5k; direct min 75k; direct start 150k');
GO
*/
