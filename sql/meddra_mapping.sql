-- name: drop_table
DROP TABLE IF EXISTS ctgov.rg_meddra_map;

-- name: create_table
CREATE TABLE ctgov.rg_meddra_map
(
    original TEXT,
    standard TEXT,
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
SELECT DISTINCT trim(BOTH FROM
                     regexp_replace(regexp_replace(lower(pt_name), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g')) AS pt_name,
                pt_code
FROM meddra.mdhier
UNION
SELECT DISTINCT trim(BOTH FROM
                     regexp_replace(regexp_replace(lower(llt_name), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g')) AS pt_name,
                pt_code
FROM meddra.llt;

-- name: find_all_unknown_terms
SELECT trim(BOTH FROM regexp_replace(regexp_replace(lower(e.adverse_event_term), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g'))
FROM ctgov.reported_events e
WHERE e.organ_system != 'Total'
  AND e.pt_code IS NULL
  AND e.adverse_event_term IS NOT NULL
  AND e.subjects_affected > 0
GROUP BY trim(BOTH FROM
              regexp_replace(regexp_replace(lower(e.adverse_event_term), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g'))
HAVING count(*) > 1;

-- name: find_pts
SELECT DISTINCT lower(pt_name) AS pt_name, pt_code
FROM meddra.mdhier
WHERE lower(soc_name) = $1;

-- name: find_all_socs
SELECT DISTINCT lower(soc_name)
FROM meddra.mdhier;


-- name: insert_wild_pt_code_1
UPDATE ctgov.reported_events re
SET pt_code = m.pt_code
FROM ctgov.rg_meddra_map m
WHERE m.original = lower(re.adverse_event_term)
  AND re.pt_code IS NULL;

-- name: insert_wild_pt_code_2
UPDATE ctgov.reported_events re
SET pt_code = m.pt_code
FROM ctgov.rg_meddra_map m
WHERE m.original =
      trim(BOTH FROM regexp_replace(regexp_replace(lower(re.adverse_event_term), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g'))
  AND re.pt_code IS NULL;


-- name: catch_terms_including_the_word_or
WITH cte1 AS (SELECT DISTINCT trim(BOTH FROM
                                   regexp_replace(regexp_replace(lower(s.either), '[^a-z]', ' ', 'g'), '\s+', ' ',
                                                  'g')) AS loose,
                              adverse_event_term
              FROM ctgov.reported_events t,
                   unnest(string_to_array(t.adverse_event_term, ' or ')) s(either)
              WHERE pt_code IS NULL
                AND adverse_event_term LIKE '% or %'
              ORDER BY adverse_event_term),
     cte2 AS (SELECT DISTINCT trim(BOTH FROM
                                   regexp_replace(regexp_replace(lower(pt_name), '[^a-z]', ' ', 'g'), '\s+', ' ',
                                                  'g')) AS pt_name,
                              pt_code
              FROM meddra.mdhier
              UNION
              SELECT DISTINCT trim(BOTH FROM
                                   regexp_replace(regexp_replace(lower(llt_name), '[^a-z]', ' ', 'g'), '\s+', ' ',
                                                  'g')) AS pt_name,
                              pt_code
              FROM meddra.llt)
INSERT
INTO ctgov.rg_meddra_map (original, standard, pt_code)
SELECT cte1.adverse_event_term AS original,
       cte2.pt_name            AS standard,
       pt_code                 AS pt_code
FROM cte1
         JOIN cte2 ON cte1.loose = cte2.pt_name
GROUP BY cte1.adverse_event_term, cte2.pt_name, pt_code;
