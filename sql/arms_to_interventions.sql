-- name: find_studies
SELECT r.nct_id, d.intervention_model AS model
FROM ctgov.result_groups r
         JOIN ctgov.reported_events re ON r.nct_id = re.nct_id
         JOIN ctgov.designs d ON r.nct_id = d.nct_id
         LEFT JOIN ctgov.interventions i ON r.nct_id = i.nct_id
WHERE r.ctgov_group_code LIKE 'E%'
  AND re.subjects_affected > 0
  AND r.nct_id IS NOT NULL
  AND (i.intervention_type NOT IN ('Procedure', 'Device', 'Behavioral', 'Diagnostic Test') OR
       i.intervention_type IS NULL)
GROUP BY r.nct_id, d.intervention_model;


-- name: find_result_groups
SELECT id AS id, lower(title) AS title, lower(description) AS description, NULL AS interventions
FROM ctgov.result_groups
WHERE nct_id = $1
  AND ctgov_group_code LIKE 'E%';

-- name: find_design_groups
SELECT dg.id                                                                             AS id,
       lower(dg.title)                                                                   AS title,
       coalesce(dg.description, '')                                                      AS description,
       string_agg(DISTINCT concat((lower(i.name)), ' || ', cast(i.id AS TEXT)), ' ||| ') AS interventions
FROM ctgov.design_groups dg
         LEFT JOIN ctgov.design_group_interventions dgi ON dgi.design_group_id = dg.id
         LEFT JOIN ctgov.interventions i ON dgi.intervention_id = i.id
WHERE dg.nct_id = $1
  AND intervention_type NOT IN ('Device', 'Behavioral', 'Diagnostic Test')
GROUP BY dg.id, lower(dg.title), dg.description;

-- name: find_interventions
SELECT DISTINCT id, lower(name) AS name
FROM ctgov.interventions
WHERE nct_id = $1
  AND intervention_type NOT IN ('Device', 'Behavioral', 'Diagnostic Test');

-- name: drop_table
DROP TABLE IF EXISTS ctgov.matches;

-- name: drop_table_rg_int
DROP TABLE IF EXISTS ctgov.result_group_intervention;

-- name: create_table
CREATE TABLE ctgov.matches
(
    nct_id        TEXT,
    rg_id         INT,
    rg_title      TEXT,
    rg_title_drug TEXT,
    rg_desc       TEXT,
    dg_id         INT,
    dg_title      TEXT,
    dg_title_drug TEXT,
    dg_desc       TEXT,
    match_method  TEXT
);

-- name: create_table_result_group_intervention
CREATE TABLE ctgov.result_group_intervention
(
    rg_id           INT,
    intervention_id INT
);

-- name: match
INSERT INTO ctgov.matches (nct_id, dg_id, dg_title, dg_desc, rg_id, rg_title, rg_desc, match_method)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: populate_remaining
INSERT INTO ctgov.matches(nct_id, rg_id, rg_title, rg_desc, match_method)
SELECT DISTINCT rg.nct_id,
                rg.id                 AS id,
                lower(rg.title)       AS title,
                lower(rg.description) AS description,
                'remaining groups'
FROM ctgov.result_groups rg
         LEFT JOIN ctgov.matches m ON m.rg_id = rg.id
         JOIN ctgov.reported_events re ON rg.nct_id = re.nct_id
WHERE m.rg_id IS NULL
  AND rg.ctgov_group_code LIKE 'E%'
  AND re.subjects_affected > 0;