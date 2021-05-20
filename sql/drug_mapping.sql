-- name: drop_mapping_table
DROP TABLE IF EXISTS ctgov.drug_mapping;

-- name: drop_join_table
DROP TABLE IF EXISTS ctgov.drug_mapping_rxcui;


-- name: create_mapping_table
CREATE TABLE ctgov.drug_mapping AS
SELECT row_number() OVER () AS id,
       original,
       clean,
       occurrences,
       source
FROM (WITH cte AS (SELECT r.nct_id
                   FROM ctgov.result_groups r
                            JOIN ctgov.reported_events re ON r.nct_id = re.nct_id
                   WHERE r.ctgov_group_code LIKE 'E%'
                     AND re.subjects_affected > 0
                     AND r.nct_id IS NOT NULL
                   GROUP BY r.nct_id)
      SELECT lower(name)     AS original,
             lower(name)     AS clean,
             count(*)        AS occurrences,
             'interventions' AS source
      FROM ctgov.interventions i
               JOIN cte ON cte.nct_id = i.nct_id
      WHERE intervention_type NOT IN ('Device', 'Behavioral', 'Diagnostic Test')
      GROUP BY lower(name)
      UNION
      SELECT lower(name)                 AS original,
             lower(name)                 AS clean,
             count(*)                    AS occurrences,
             'interventions_other_names' AS source
      FROM ctgov.intervention_other_names i
               JOIN cte ON cte.nct_id = i.nct_id
      GROUP BY lower(name)
      UNION
      SELECT DISTINCT lower(title)    AS original,
                      lower(title)    AS clean,
                      count(*)        AS occurrences,
                      'design_groups' AS source
      FROM ctgov.design_groups dg
               JOIN cte
                    ON cte.nct_id = dg.nct_id
      GROUP BY lower(title)
      UNION
      SELECT DISTINCT lower(title)    AS original,
                      lower(title)    AS clean,
                      count(*)        AS occurrences,
                      'result_groups' AS source
      FROM ctgov.result_groups rg
               JOIN cte
                    ON cte.nct_id = rg.nct_id
      GROUP BY lower(title)
     ) AS big;

-- name: mapping_clean_index
CREATE INDEX mapping_clean_index
    ON ctgov.drug_mapping (clean);


-- name: create_join_table
CREATE TABLE ctgov.drug_mapping_rxcui
(
    drug_mapping_id INT,
    original        TEXT,
    rxcui           INT,
    rx_str          TEXT
);

-- name: direct_match_rxnconso
INSERT INTO ctgov.drug_mapping_rxcui
SELECT DISTINCT dm.id AS drug_mapping_id, dm.original AS original, rx2.rxcui AS rxcui, NULL AS rx_str
FROM ctgov.drug_mapping dm
         JOIN ctgov.rxnconso rx1 ON dm.clean = rx1.str
         JOIN ctgov.rxnconso rx2 ON rx1.rxcui = rx2.rxcui
WHERE rx2.tty NOT IN ('PSN', 'SY', 'TMSY', 'DF')
  AND rx2.sab = 'RXNORM'
  AND dm.clean != 'control';

-- name: direct_match_art57
INSERT INTO ctgov.drug_mapping_rxcui
SELECT DISTINCT dm.id       AS drug_mapping_id,
                dm.original AS original,
                unnest(string_to_array(a.rxcui, ',')::INT[]),
                NULL        AS rx_str
FROM ctgov.drug_mapping dm
         JOIN ctgov.article57_rxnorm a ON dm.clean = a.name
WHERE dm.clean != 'control';

-- name: remove_matches
WITH cte AS (SELECT DISTINCT drug_mapping_id FROM ctgov.drug_mapping_rxcui)
UPDATE ctgov.drug_mapping dm
SET clean = NULL
FROM cte
WHERE cte.drug_mapping_id = dm.id;

-- name: remove_junk
UPDATE ctgov.drug_mapping dm
SET clean = NULL
WHERE length(clean) = 1
   OR clean IN
      ('all participants', '', 'total', 'control', 'intervention', 'treatment', 'single', 'study', 'usual care',
       'standard of care', 'vehicle', 'low dose', 'high dose', 'experimental', 'no treatment',
       'quality of life assessment', 'surgery');

-- name: remove_cohort
UPDATE ctgov.drug_mapping
SET clean = regexp_replace(clean, 'cohort \d', ' ', 'g')
WHERE clean LIKE '%cohort %';

-- name: sham_to_placebo
UPDATE ctgov.drug_mapping
SET clean = 'placebo'
WHERE clean LIKE 'sham %'
   OR clean IN ('plcb', 'matching placebo', 'placebo comparator', 'sham', 'sugar pill', 'placebos');

-- name: remove_q2w
UPDATE ctgov.drug_mapping
SET clean = regexp_replace(clean, 'q[0-9]w', '', 'g');

-- name: remove_qd
UPDATE ctgov.drug_mapping
SET clean = replace(clean, ' qd', '')
WHERE clean LIKE '% qd';

-- name: remove_non_alpha_numeric
UPDATE ctgov.drug_mapping
SET clean = regexp_replace(original, '[^a-z0-9]', ' ', 'g');

-- name: remove_group
UPDATE ctgov.drug_mapping
SET clean = replace(clean, 'group', '');

-- name: remove_spaces
UPDATE ctgov.drug_mapping
SET clean = replace(clean, '  ', ' ');

-- name: remove_group
UPDATE ctgov.drug_mapping
SET clean = replace(clean, 'group', '');

-- name: remove_arm
UPDATE ctgov.drug_mapping
SET clean = replace(clean, 'arm', '')
WHERE clean LIKE 'arm %'
   OR clean LIKE '% arm'
   OR clean LIKE '% arm %';

-- name: trim
UPDATE ctgov.drug_mapping
SET clean = trim(BOTH FROM clean);

-- name: pre_and_append_spaces
UPDATE ctgov.drug_mapping
SET clean = concat(' ', clean, ' ')
WHERE clean IS NOT NULL;

-- name: find_ingredients
WITH cte1 AS (SELECT DISTINCT lower(str) AS string, rxcui
              FROM ctgov.rxnconso
              WHERE sab = 'RXNORM'
                AND tty = $1)
SELECT id AS drug_mapping_id, cte1.rxcui AS rxcui, cte1.string
FROM ctgov.drug_mapping
         JOIN cte1 ON clean LIKE concat('% ', string, ' %');

-- name: synonyms
WITH cte AS (SELECT DISTINCT dm.id, dm.original, rx2.rxcui, rx2.str
             FROM ctgov.drug_mapping dm
                      JOIN ctgov.cem_staging_vocabulary_concept_synonym c ON dm.clean = lower(c.concept_synonym_name)
                      JOIN ctgov.cem_staging_vocabulary_source_to_concept_map m ON c.concept_id = m.source_concept_id
                      JOIN ctgov.rxnconso rx ON m.source_code = rx.code
                      JOIN ctgov.rxnconso rx2 ON rx.rxcui = rx2.rxcui
             WHERE dm.clean IS NOT NULL
               AND rx2.sab = 'RXNORM'
               AND rx2.tty IN ('IN', 'MIN'))
INSERT
INTO ctgov.drug_mapping_rxcui
SELECT *
FROM cte;

-- name: drop_join_table_1
DROP TABLE IF EXISTS ctgov.interventions_rxnorm;
-- name: drop_join_table_2
DROP TABLE IF EXISTS ctgov.intervention_other_names_rxnorm;
-- name: drop_join_table_3
DROP TABLE IF EXISTS ctgov.result_groups_rxnorm;
-- name: drop_join_table_4
DROP TABLE IF EXISTS ctgov.design_groups_rxnorm;


-- name: join_table_1
CREATE TABLE ctgov.interventions_rxnorm AS (
    SELECT DISTINCT inv.nct_id, inv.id, inv.name, rx.rx_str, rx.rxcui
    FROM ctgov.drug_mapping_rxcui rx
             JOIN ctgov.drug_mapping m ON rx.drug_mapping_id = m.id
             JOIN ctgov.interventions inv
                  ON lower(inv.name) = m.original
    WHERE inv.intervention_type NOT IN ('Device', 'Behavioral', 'Diagnostic Test'));

-- name: join_table_2
CREATE TABLE ctgov.intervention_other_names_rxnorm AS (
    SELECT DISTINCT inv.nct_id, inv.intervention_id, inv.name, rx.rx_str, rx.rxcui
    FROM ctgov.drug_mapping_rxcui rx
             JOIN ctgov.drug_mapping m ON rx.drug_mapping_id = m.id
             JOIN ctgov.intervention_other_names inv
                  ON lower(inv.name) = m.original);

-- name: join_table_3
CREATE TABLE ctgov.result_groups_rxnorm AS (
    SELECT DISTINCT inv.nct_id, inv.id, inv.title, rx.rx_str, rx.rxcui
    FROM ctgov.drug_mapping_rxcui rx
             JOIN ctgov.drug_mapping m ON rx.drug_mapping_id = m.id
             JOIN ctgov.result_groups inv
                  ON lower(inv.title) = m.original);

-- name: join_table_4
CREATE TABLE ctgov.design_groups_rxnorm AS (
    SELECT DISTINCT inv.nct_id, inv.id, m.original, rx.rx_str, rx.rxcui
    FROM ctgov.drug_mapping_rxcui rx
             JOIN ctgov.drug_mapping m ON rx.drug_mapping_id = m.id
             JOIN ctgov.design_groups inv
                  ON lower(inv.title) = m.original);


-- name: drop_rg_desc_map
DROP TABLE IF EXISTS ctgov.rg_desc_mapping;

-- name: rg_desc_map
CREATE TABLE ctgov.rg_desc_mapping AS
    (
        SELECT row_number() OVER ()                   AS id,
               lower(m.rg_desc)                       AS description,
               string_agg(cast(m.rg_id AS TEXT), ',') AS ids
        FROM ctgov.matches m
                 LEFT JOIN ctgov.design_groups_rxnorm dg ON dg.id = m.dg_id
                 LEFT JOIN ctgov.result_groups_rxnorm rg ON rg.id = m.rg_id
                 LEFT JOIN ctgov.result_group_intervention ri ON m.rg_id = ri.rg_id
                 LEFT JOIN ctgov.interventions_rxnorm i ON ri.intervention_id = i.id
                 LEFT JOIN ctgov.intervention_other_names_rxnorm io ON io.intervention_id = ri.intervention_id
        WHERE dg.rxcui IS NULL
          AND rg.rxcui IS NULL
          AND i.rxcui IS NULL
          AND io.rxcui IS NULL
        GROUP BY lower(m.rg_desc));


-- name: remove_non_alpha_numeric_rg
UPDATE ctgov.rg_desc_mapping
SET description = regexp_replace(description, '[^a-z0-9]', ' ', 'g');

-- name: pre_and_append_spaces_rg
UPDATE ctgov.rg_desc_mapping
SET description = concat(' ', description, ' ')
WHERE description IS NOT NULL;

-- name: drop_join_table_rg
DROP TABLE IF EXISTS ctgov.description_mapping_rxcui;


-- name: create_join_table_rg
CREATE TABLE ctgov.description_mapping_rxcui
(
    rg_desc_mapping_id INT,
    rxcui              INT
);


-- name: description_mapping_into_rg_rx_table
INSERT INTO ctgov.result_groups_rxnorm (id, rxcui)
SELECT DISTINCT cast(unnest(string_to_array(ids, ',')) AS INT) AS id, rxcui
FROM ctgov.rg_desc_mapping
         JOIN ctgov.description_mapping_rxcui ON rg_desc_mapping.id = description_mapping_rxcui.rg_desc_mapping_id;

-- name: dg_rg_rx_table
INSERT INTO ctgov.result_groups_rxnorm (id, rxcui)
SELECT DISTINCT m.rg_id AS id, dg.rxcui AS rxcui
FROM ctgov.matches m
         JOIN ctgov.design_groups_rxnorm dg ON dg.id = m.dg_id;

-- name: i_rg_rx_table
INSERT INTO ctgov.result_groups_rxnorm (id, rxcui)
SELECT DISTINCT m.rg_id AS id, i.rxcui AS rxcui
FROM ctgov.matches m
         JOIN ctgov.result_group_intervention ri ON m.rg_id = ri.rg_id
         JOIN ctgov.interventions_rxnorm i ON ri.intervention_id = i.id;

-- name: io_rg_rx_table
INSERT INTO ctgov.result_groups_rxnorm (id, rxcui)
SELECT DISTINCT m.rg_id AS id, io.rxcui AS rxcui
FROM ctgov.matches m
         JOIN ctgov.result_group_intervention ri ON m.rg_id = ri.rg_id
         JOIN ctgov.intervention_other_names_rxnorm io ON io.intervention_id = ri.intervention_id;

-- name: i_over_dg_rg_rx_table
SELECT DISTINCT m.rg_id AS id, i.rxcui AS rxcui
FROM ctgov.matches m
         JOIN ctgov.design_group_interventions dgi ON m.dg_id = dgi.design_group_id
         JOIN ctgov.interventions_rxnorm i ON dgi.intervention_id = i.id;

-- name: io_over_dg_rg_rx_table
SELECT DISTINCT m.rg_id AS id, io.rxcui AS rxcui
FROM ctgov.matches m
         JOIN ctgov.design_group_interventions dgi ON m.dg_id = dgi.design_group_id
         JOIN ctgov.intervention_other_names_rxnorm io ON io.intervention_id = dgi.intervention_id;

-- name: drop_final_rg_in_table
DROP TABLE IF EXISTS ctgov.result_group_ingredient;

-- name: create_final_rg_in_table
CREATE TABLE ctgov.result_group_ingredient AS
WITH cte AS (SELECT DISTINCT r.nct_id,
                             r.id,
                             rx.rxcui AS rxcui,
                             rc.str,
                             rc.tty,
                             rx.rxcui AS rxcui_in
             FROM ctgov.result_groups r
                      JOIN ctgov.reported_events re ON r.nct_id = re.nct_id
                      JOIN ctgov.designs d ON r.nct_id = d.nct_id
                      JOIN ctgov.result_groups_rxnorm rx ON rx.id = r.id
                      JOIN ctgov.rxnconso rc ON rc.rxcui = rx.rxcui
             WHERE r.ctgov_group_code LIKE 'E%'
               AND rc.tty NOT IN ('IN', 'PSN', 'SY', 'TMSY', 'DF')
               AND rc.sab = 'RXNORM'
               AND re.subjects_affected > 0
               AND r.nct_id IS NOT NULL
               AND rx.rxcui NOT IN (1001007, 890964, 411, 11295))
SELECT DISTINCT cte.nct_id,
                cte.id,
                cte.rxcui AS rxcui,
                cte.str,
                cte.tty,
                r.rxcui   AS in_rxcui,
                r.str     AS in_str
FROM ctgov.rxnrel rel
         JOIN ctgov.rxnconso r ON rel.rxcui2 = cast(r.rxcui AS TEXT)
         JOIN cte ON cast(cte.rxcui AS TEXT) = rel.rxcui1
WHERE r.tty = 'IN'
  AND r.sab = 'RXNORM'
UNION
SELECT DISTINCT r.nct_id,
                r.id,
                rx.rxcui AS rxcui,
                rc.str,
                rc.tty,
                rx.rxcui AS in_rxcui,
                rc.str   AS in_str
FROM ctgov.result_groups r
         JOIN ctgov.reported_events re ON r.nct_id = re.nct_id
         JOIN ctgov.designs d ON r.nct_id = d.nct_id
         JOIN ctgov.result_groups_rxnorm rx ON rx.id = r.id
         JOIN ctgov.rxnconso rc ON rc.rxcui = rx.rxcui
WHERE r.ctgov_group_code LIKE 'E%'
  AND rc.tty = 'IN'
  AND rc.sab = 'RXNORM'
  AND re.subjects_affected > 0
  AND r.nct_id IS NOT NULL
  AND rx.rxcui NOT IN (1001007, 890964, 411, 11295);


-- name: find_rxconso_terms_for_tty
SELECT DISTINCT trim(BOTH FROM regexp_replace(regexp_replace(lower(str), '[^a-z0-9]', ' ', 'g'), '\s+', ' ', 'g')) AS str,
                rxcui
FROM ctgov.rxnconso
WHERE sab = 'RXNORM'
  AND tty = $1
  AND rxcui NOT IN (1001007, 890964, 411, 11295);

-- name: find_terms_to_map
SELECT DISTINCT id, clean
FROM ctgov.drug_mapping
WHERE clean IS NOT NULL;