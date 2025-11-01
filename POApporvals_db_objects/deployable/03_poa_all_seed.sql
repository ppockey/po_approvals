-- seed PO_ApproverDirectory for testing
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

-- seed the policy table with current production assumptions
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

-- (RUN WHEN NEEDED) keep commented until it's necessary to activate a new policy
/* Example: activating a new policy when Finance moves a gate
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
