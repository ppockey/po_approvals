-- GLOBAL defaults (apply when no house/buyer override exists)
INSERT INTO dbo.PO_ApproverDirectory (RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
VALUES
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
('Local Department Manager',       'GLOBAL', '', 'u.ldm.global',           'Local Department Manager (Global)',     'ldm.global@example.com');

-- Site-specific overrides for HouseCode = 'IP' (kick in before GLOBAL)
INSERT INTO dbo.PO_ApproverDirectory (RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
VALUES
('Local Purchasing Manager',       'IP', '',   'u.lpm.ip',     'Local Purchasing Manager (IP)',       'lpm.ip@example.com'),
('Financial Controller',           'IP', '',   'u.fc.ip',      'Financial Controller (IP)',           'fc.ip@example.com'),
('Plant Manager (General Manager)','IP', '',   'u.gm.ip',      'Plant Manager / GM (IP)',             'gm.ip@example.com'),
('Regional VP/GM',                 'IP', '',   'u.rvp.ip',     'Regional VP/GM (IP)',                 'rvp.ip@example.com'),
('Regional Supply Chain Manager',  'IP', '',   'u.rscm.ip',    'Regional Supply Chain Manager (IP)',  'rscm.ip@example.com');

-- Buyer-specific override for HouseCode = 'IP', BuyerCode = 'EPM109' (most specific)
INSERT INTO dbo.PO_ApproverDirectory (RoleCode, HouseCode, BuyerCode, UserId, DisplayName, Email)
VALUES
('Local Purchasing Manager', 'IP', 'EPM109', 'u.lpm.ip.epm109', 'Local Purchasing Manager (IP/EPM109)', 'lpm.ip.epm109@example.com'),
('Buyer',                    'IP', 'EPM109', 'u.buyer.ip.epm109','Buyer (IP/EPM109)',                   'buyer.ip.epm109@example.com');
