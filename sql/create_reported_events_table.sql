CREATE TABLE ctgov.reported_events_2 AS
SELECT DISTINCT re.*, m.pt_code
FROM ctgov.reported_events re
         LEFT JOIN meddra.mdhier m ON lower(m.pt_name) = lower(re.adverse_event_term);

CREATE INDEX index_reported_events_on_event_type_2
    ON ctgov.reported_events_2 (event_type);

CREATE INDEX index_reported_events_on_nct_id_2
    ON ctgov.reported_events_2 (nct_id);

CREATE INDEX index_reported_events_on_subjects_affected_2
    ON ctgov.reported_events_2 (subjects_affected);

CREATE INDEX reported_events_nct_idx_2
    ON ctgov.reported_events_2 (nct_id);

DROP TABLE IF EXISTS ctgov.reported_events;

ALTER TABLE ctgov.reported_events_2
    RENAME TO reported_events;
