-- name: drop_table
DROP TABLE IF EXISTS ctgov.rg_meddra_map;

-- name: create_table
CREATE TABLE ctgov.rg_meddra_map
(
    original TEXT,
    standard TEXT,
    soc      TEXT,
    pt_code  INTEGER
);


-- name: update_general_disorders
UPDATE ctgov.reported_events
SET organ_system = 'General disorders and administration site conditions'
WHERE organ_system = 'General disorders';

-- name: find_unknown_terms
SELECT lower(e.adverse_event_term)
FROM ctgov.reported_events e
WHERE lower(e.organ_system) = $1
  AND e.pt_code IS NULL
  AND e.adverse_event_term IS NOT NULL
GROUP BY lower(e.adverse_event_term);

-- name: find_all_pts
SELECT DISTINCT lower(pt_name) AS pt_name, pt_code
FROM meddra.mdhier
WHERE lower(soc_name) = $1;

-- name: find_all_socs
SELECT DISTINCT lower(soc_name)
FROM meddra.mdhier;

-- name: insert_wild_pt_code
UPDATE ctgov.reported_events re
SET pt_code = m.pt_code
FROM ctgov.rg_meddra_map m
WHERE m.original = lower(re.adverse_event_term)
  AND m.soc = lower(re.organ_system)
  AND re.pt_code IS NULL;