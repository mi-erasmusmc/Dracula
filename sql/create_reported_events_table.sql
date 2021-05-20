DROP TABLE IF EXISTS ctgov.reported_events_2;

CREATE TABLE ctgov.reported_events_2 AS
SELECT DISTINCT re.id,
                re.nct_id,
                re.result_group_id,
                re.ctgov_group_code,
                re.time_frame,
                re.event_type,
                re.default_vocab,
                re.default_assessment,
                re.subjects_affected,
                re.subjects_at_risk,
                re.description,
                re.event_count,
                re.organ_system,
                re.adverse_event_term,
                re.frequency_threshold,
                re.vocab,
                re.assessment,
                m.pt_code
FROM ctgov.reported_events re
         LEFT JOIN meddra.mdhier m ON lower(m.pt_name) = lower(re.adverse_event_term);

WITH cte AS (SELECT DISTINCT trim(BOTH FROM
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
UPDATE ctgov.reported_events_2 re
SET pt_code = cte.pt_code
FROM cte
WHERE trim(BOTH FROM
           regexp_replace(regexp_replace(lower(re.adverse_event_term), '[^a-z]', ' ', 'g'), '\s+', ' ', 'g')) =
      cte.pt_name
  AND re.pt_code IS NULL;

DROP INDEX IF EXISTS ctgov.index_reported_events_on_event_type_2;

CREATE INDEX index_reported_events_on_event_type_2
    ON ctgov.reported_events_2 (event_type);

DROP INDEX IF EXISTS ctgov.index_reported_events_on_nct_id_2;

CREATE INDEX index_reported_events_on_nct_id_2
    ON ctgov.reported_events_2 (nct_id);

DROP INDEX IF EXISTS ctgov.index_reported_events_on_subjects_affected_2;

CREATE INDEX index_reported_events_on_subjects_affected_2
    ON ctgov.reported_events_2 (subjects_affected);

DROP INDEX IF EXISTS ctgov.reported_events_nct_idx_2;

CREATE INDEX reported_events_nct_idx_2
    ON ctgov.reported_events_2 (nct_id);

DROP TABLE IF EXISTS ctgov.reported_events;

ALTER TABLE ctgov.reported_events_2
    RENAME TO reported_events;
